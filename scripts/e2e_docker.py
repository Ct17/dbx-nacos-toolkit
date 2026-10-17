#!/usr/bin/env python3
"""Nacos Toolkit 插件 Docker 端到端测试。

针对 Docker 中运行的真实 Nacos，驱动 sidecar 二进制走 DBX 插件 stdio
协议（JSON-RPC 2.0，每行一条消息），逐项验证：

    plugin/initialize -> connection/test -> connection/connect -> nacos/ping
    -> mcp/tools -> mcp/call 七个工具 -> 错误路径 -> connection/disconnect

两种测试模式：
  * v1 模式（默认）：目标为 Nacos 1.x/2.x（或 3.x 搭配 legacy adapter），
    数据准备走 v1 Open API；
  * v3 模式（传入 --console-port）：目标为纯 Nacos 3.x（无 adapter），
    数据准备走 v3 console/client API（Bearer 鉴权），插件经自动探测走
    v3 协议栈。

脚本会自动准备测试数据（命名空间 / 配置与历史 / 服务实例），数据写入
均幂等，可重复运行；额外传入 --auth-port 时，还会对启用了鉴权的第二个
Nacos 实例验证「登录 + token 复用 + 错误凭据」链路。

用法：
    # v1（Nacos 2.4.3 等）
    python3 scripts/e2e_docker.py --port 18848 [--auth-port 18948]
    # v3（纯 Nacos 3.2.4）
    python3 scripts/e2e_docker.py --port 18868 --console-port 18088 \
        [--auth-port 18968 --auth-console-port 18098] [--no-prepare]

退出码：0 全部通过，1 存在失败。
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BACKEND_DIR = os.path.join(ROOT, "backend")

# 统一绕过环境代理直连本机（开发机可能配置了 HTTP 代理变量）。
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))

# --- 测试夹具内容 -------------------------------------------------------------

GATEWAY_V1 = "server:\n  port: 9000\n"
GATEWAY_DEV = "server:\n  port: 9000\nlogging:\n  level: debug\n"
GATEWAY_PROD = "server:\n  port: 8080\nlogging:\n  level: info\n"
APP_DEV = "app.name=e2e-demo\nfeature.x.enabled=true\n"
APP_TEST = "app.name=e2e-demo\nfeature.x.enabled=false\n"
EXTRA_JSON = '{"retry": 3}\n'


# --- 结果收集 -----------------------------------------------------------------

class Checks:
    def __init__(self):
        self.results = []  # 元素为 ("section", 标题, None, None) 或 (None, 名称, ok, 详情)

    def section(self, title):
        self.results.append(("section", title, None, None))

    def check(self, name, ok, detail=""):
        self.results.append((None, name, bool(ok), detail))
        return bool(ok)

    def report(self):
        failures = []
        for section, name, ok, detail in self.results:
            if section is not None:
                print(f"\n== {name} ==")
                continue
            tag = "ok  " if ok else "FAIL"
            line = f"  [{tag}] {name}"
            if not ok and detail:
                line += f"\n         -> {detail[:500]}"
            print(line)
            if not ok:
                failures.append(name)
        return failures


# --- Nacos 直连（仅数据准备，与插件无关）----------------------------------------

def _raw_request(url, method="GET", params=None, token=None, bearer=False):
    """带可选凭据的原生 HTTP 请求（绕过环境代理）。"""
    data = None
    if params is not None:
        cleaned = {k: str(v) for k, v in params.items() if v is not None}
        encoded = urllib.parse.urlencode(cleaned, safe="*")
        if method == "GET":
            url += "?" + encoded
        else:
            data = encoded.encode()
    req = urllib.request.Request(url, data=data, method=method)
    if data is not None:
        req.add_header("Content-Type", "application/x-www-form-urlencoded")
    if token:
        if bearer:
            req.add_header("Authorization", "Bearer " + token)
        else:
            sep = "&" if "?" in url else "?"
            url = f"{url}{sep}accessToken={urllib.parse.quote(token)}"
            req = urllib.request.Request(url, data=data, method=method)
            if data is not None:
                req.add_header("Content-Type", "application/x-www-form-urlencoded")
    try:
        with OPENER.open(req, timeout=15) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as err:
        return err.code, err.read().decode()


class Admin:
    """v1 Open API 直连客户端（accessToken 查询参数鉴权）。"""

    def __init__(self, port, token=""):
        self.base = f"http://127.0.0.1:{port}/nacos"
        self.token = token

    def request(self, method, path, params=None):
        return _raw_request(self.base + path, method, params, token=self.token, bearer=False)

    def get(self, path, params=None):
        return self.request("GET", path, params)

    def post(self, path, params=None):
        return self.request("POST", path, params)

    def put(self, path, params=None):
        return self.request("PUT", path, params)


class ConsoleV3:
    """Nacos 3.x console/client API 直连客户端（Bearer 头鉴权）。"""

    def __init__(self, console_port, server_port, token=""):
        self.console_base = f"http://127.0.0.1:{console_port}"
        self.server_base = f"http://127.0.0.1:{server_port}/nacos"
        self.token = token

    # console 接口（无 context path）
    def get(self, path, params=None):
        return _raw_request(self.console_base + path, "GET", params, token=self.token, bearer=True)

    def post(self, path, params=None):
        return _raw_request(self.console_base + path, "POST", params, token=self.token, bearer=True)

    # client 接口（/nacos 前缀）
    def client_get(self, path, params=None):
        return _raw_request(self.server_base + path, "GET", params, token=self.token, bearer=True)

    def client_post(self, path, params=None):
        return _raw_request(self.server_base + path, "POST", params, token=self.token, bearer=True)


def ensure_admin(port, password="nacos"):
    """初始化管理员用户（v1 链路）。

    幂等：已初始化时服务端返回 409（have admin user），同样视为可用。
    Nacos 3.2+ 的 legacy adapter 对 v1 初始化端点存在编译缺陷（HTTP 500），
    此时回退到 v3 初始化接口（3.0+ 均提供）。
    """
    status, body = Admin(port).post("/v1/auth/users/admin", {"password": password})
    if status == 200:
        return
    status2, body2 = Admin(port).post("/v3/auth/user/admin", {"password": password})
    if status2 == 200:
        return
    raise RuntimeError(
        f"初始化管理员失败: v1 HTTP {status} {body[:120]} / v3 HTTP {status2} {body2[:120]}")


def ensure_admin_v3(console_port, password="nacos"):
    """初始化管理员用户（v3 链路；幂等，已存在时返回 code 409 + have admin）。"""
    status, body = _raw_request(
        f"http://127.0.0.1:{console_port}/v3/auth/user/admin", "POST", {"password": password})
    compact = body.replace(" ", "")
    if status == 200 and ('"code":0' in compact or "have admin" in body):
        return
    raise RuntimeError(f"初始化管理员失败: HTTP {status} {body[:200]}")


def login(port, username, password):
    """v1 登录并返回 accessToken（鉴权模式使用）。"""
    status, body = Admin(port).post("/v1/auth/login", {"username": username, "password": password})
    if status != 200:
        raise RuntimeError(f"登录 127.0.0.1:{port} 失败: HTTP {status} {body[:200]}")
    return json.loads(body)["accessToken"]


def login_v3(console_port, username, password):
    """v3 登录并返回 accessToken（Bearer 鉴权使用）。"""
    status, body = _raw_request(
        f"http://127.0.0.1:{console_port}/v3/auth/user/login", "POST",
        {"username": username, "password": password})
    if status != 200:
        raise RuntimeError(f"登录 127.0.0.1:{console_port} 失败: HTTP {status} {body[:200]}")
    return json.loads(body)["accessToken"]


def wait_ready(port, timeout=120):
    """轮询 v1 readiness 接口，直到 Nacos 就绪。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            status, body = Admin(port).get("/v1/console/health/readiness")
            # 2.4.x 返回纯文本 "OK"，2.x 早期版本返回 {"status":"UP"}，两种都认。
            compact = body.replace(" ", "").upper()
            if status == 200 and (body.strip().upper() == "OK" or '"STATUS":"UP"' in compact):
                return
        except Exception:
            pass
        time.sleep(2)
    raise RuntimeError(f"Nacos 127.0.0.1:{port} 在 {timeout} 秒内未就绪")


def wait_ready_v3(console_port, timeout=180):
    """轮询 v3 console readiness 接口，直到 Nacos 3.x 就绪。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            status, body = _raw_request(
                f"http://127.0.0.1:{console_port}/v3/console/health/readiness")
            if status == 200 and '"code":0' in body.replace(" ", ""):
                return
        except Exception:
            pass
        time.sleep(2)
    raise RuntimeError(f"Nacos 3.x console 127.0.0.1:{console_port} 在 {timeout} 秒内未就绪")


# --- 数据准备（v1）--------------------------------------------------------------

def ensure_namespace(admin, ns_id, show_name):
    status, body = admin.get("/v1/console/namespaces")
    if status == 200:
        existing = {item.get("namespace", "") for item in json.loads(body).get("data", [])}
        if ns_id in existing:
            return
    status, body = admin.post("/v1/console/namespaces", {
        "customNamespaceId": ns_id, "namespaceName": show_name, "namespaceDesc": "e2e fixture",
    })
    if status != 200 or "exist" in body.lower():
        raise RuntimeError(f"创建命名空间 {ns_id} 失败: HTTP {status} {body[:200]}")


def publish(admin, namespace, data_id, content, ctype):
    params = {
        "dataId": data_id, "group": "DEFAULT_GROUP", "content": content,
        "type": ctype, "tenant": namespace,
    }
    status, body = admin.post("/v1/cs/configs", params)
    if status == 200 and body.strip() in ("true", "ok"):
        return
    # Nacos 3.2+ 的 legacy adapter 对 POST /v1/cs/configs 存在编译缺陷
    # （方法参数名信息缺失导致绑定失败，HTTP 500），回退到 v2 写入口。
    # 注意 v2 绑定的是 ConfigForm 对象：命名空间参数名是 namespaceId（非 v1 的 tenant）。
    status2, body2 = admin.post("/v2/cs/config", {
        "dataId": data_id, "group": "DEFAULT_GROUP", "content": content,
        "type": ctype, "namespaceId": namespace,
    })
    if status2 == 200 and '"data":true' in body2.replace(" ", ""):
        return
    raise RuntimeError(
        f"发布配置 {namespace}/{data_id} 失败: "
        f"v1 HTTP {status} {body[:120]} / v2 HTTP {status2} {body2[:120]}")


def register_instance(admin, namespace, service, ip, port, weight, metadata):
    status, body = admin.post("/v1/ns/instance", {
        "serviceName": service, "ip": ip, "port": port, "namespaceId": namespace,
        "groupName": "DEFAULT_GROUP", "clusterName": "DEFAULT", "weight": weight,
        "ephemeral": "true", "enabled": "true",
        "metadata": json.dumps(metadata or {}),
    })
    if status != 200 or body.strip() != "ok":
        raise RuntimeError(f"注册实例 {namespace}/{service}@{ip}:{port} 失败: HTTP {status} {body[:200]}")


class Heartbeats:
    """v1：为临时实例持续发心跳（Nacos 临时实例 15 秒无心跳会被剔除）。"""

    def __init__(self, admin):
        self.admin = admin
        self.instances = []
        self._stop = threading.Event()
        self._thread = None

    def add(self, namespace, service, ip, port, weight, metadata):
        instance = {"namespace": namespace, "service": service, "ip": ip,
                    "port": port, "weight": weight, "metadata": metadata}
        self.instances.append(instance)
        self._beat(instance)

    def start(self):
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()

    def _run(self):
        while not self._stop.wait(4):
            for instance in list(self.instances):
                self._beat(instance)

    def _beat(self, instance):
        beat = json.dumps({
            "serviceName": instance["service"], "ip": instance["ip"],
            "port": instance["port"], "cluster": "DEFAULT", "weight": instance["weight"],
        })
        status, body = self.admin.put("/v1/ns/instance/beat", {
            "serviceName": instance["service"], "namespaceId": instance["namespace"],
            "groupName": "DEFAULT_GROUP", "beat": beat,
        })
        # Nacos beat 成功返回 code 10200；实例已被剔除时返回 20404（instance not found）。
        if status != 200 or "20404" in body.replace(" ", ""):
            register_instance(self.admin, instance["namespace"], instance["service"],
                              instance["ip"], instance["port"], instance["weight"], instance["metadata"])


def prepare_fixtures(admin):
    """写入全部 v1 测试数据；重复运行安全（配置为 upsert，命名空间先查后建）。"""
    for ns_id, name in (("dev", "dev 环境"), ("test", "test 环境"), ("prod", "prod 环境")):
        ensure_namespace(admin, ns_id, name)

    # dev/gateway.yaml：先写 v1 再更新为最终版 —— 让变更历史至少有两条。
    publish(admin, "dev", "gateway.yaml", GATEWAY_V1, "yaml")
    publish(admin, "dev", "gateway.yaml", GATEWAY_DEV, "yaml")
    publish(admin, "dev", "app.properties", APP_DEV, "properties")
    publish(admin, "dev", "extra-config.json", EXTRA_JSON, "json")

    publish(admin, "test", "gateway.yaml", GATEWAY_DEV, "yaml")      # 与 dev 相同 -> identical
    publish(admin, "test", "app.properties", APP_TEST, "properties")  # 与 dev 不同 -> different
    publish(admin, "prod", "gateway.yaml", GATEWAY_PROD, "yaml")      # 与 dev 不同 -> different
    # prod 故意不写 app.properties —— 用于覆盖 missing-in-target 场景。

    # Nacos 3.x 的配置更新存在 1-2 秒读缓存延迟；轮询确认最终版本可读后再继续。
    deadline = time.time() + 15
    status, body = 0, ""
    while time.time() < deadline:
        status, body = admin.get("/v1/cs/configs", {
            "dataId": "gateway.yaml", "group": "DEFAULT_GROUP", "tenant": "dev",
        })
        if status == 200 and body == GATEWAY_DEV:
            return
        time.sleep(0.5)
    raise RuntimeError(
        f"dev/gateway.yaml 最终版本 15 秒内不可见: HTTP {status} {body[:120]}")


def register_instances(admin, heartbeats):
    fixtures = [
        ("dev", "order-service", "10.210.1.11", 8080, "1.0", {"version": "1.0"}),
        ("dev", "order-service", "10.210.1.12", 8080, "1.5", {}),
        ("dev", "user-service", "10.210.2.21", 9090, "1.0", {}),
    ]
    for namespace, service, ip, port, weight, metadata in fixtures:
        register_instance(admin, namespace, service, ip, port, weight, metadata)
        heartbeats.add(namespace, service, ip, port, weight, metadata)
    deadline = time.time() + 15
    visible = 0
    while time.time() < deadline:
        status, body = admin.get("/v1/ns/instance/list", {
            "serviceName": "order-service", "namespaceId": "dev", "groupName": "DEFAULT_GROUP",
        })
        hosts = json.loads(body).get("hosts", []) if status == 200 else []
        visible = len(hosts)
        if visible == 2:
            return
        time.sleep(0.5)
    raise RuntimeError(f"注册后 15 秒内未观察到 order-service 的 2 个实例（当前 {visible} 个）")


# --- 数据准备（v3，无 adapter）---------------------------------------------------

def ensure_namespace_v3(console, ns_id, show_name):
    status, body = console.get("/v3/console/core/namespace/list")
    if status == 200:
        existing = {item.get("namespace", "") for item in json.loads(body).get("data", [])}
        if ns_id in existing:
            return
    status, body = console.post("/v3/console/core/namespace", {
        "customNamespaceId": ns_id, "namespaceName": show_name, "namespaceDesc": "e2e fixture",
    })
    if status != 200 or '"code":0' not in body.replace(" ", ""):
        if "already exist" in body:
            return
        raise RuntimeError(f"创建命名空间 {ns_id} 失败: HTTP {status} {body[:200]}")


def publish_v3(console, namespace, data_id, content, ctype):
    params = {"dataId": data_id, "groupName": "DEFAULT_GROUP", "content": content, "type": ctype}
    if namespace:
        params["namespaceId"] = namespace
    status, body = console.post("/v3/console/cs/config", params)
    if status != 200 or '"code":0' not in body.replace(" ", ""):
        raise RuntimeError(f"发布配置 {namespace}/{data_id} 失败: HTTP {status} {body[:200]}")


def register_instance_v3(console, namespace, service, ip, port, weight, metadata):
    params = {
        "serviceName": service, "ip": ip, "port": port,
        "groupName": "DEFAULT_GROUP", "clusterName": "DEFAULT",
        "weight": weight, "ephemeral": "true",
    }
    if namespace:
        params["namespaceId"] = namespace
    if metadata:
        params["metadata"] = json.dumps(metadata)
    status, body = console.client_post("/v3/client/ns/instance", params)
    if status != 200 or '"code":0' not in body.replace(" ", ""):
        raise RuntimeError(f"注册实例 {namespace}/{service}@{ip}:{port} 失败: HTTP {status} {body[:200]}")


class HeartbeatsV3:
    """v3：心跳 = POST /v3/client/ns/instance + heartBeat=true。

    成功返回 code 0；实例被剔除时返回 INSTANCE_NOT_FOUND（21003），
    此时自动重新注册。
    """

    def __init__(self, console):
        self.console = console
        self.instances = []
        self._stop = threading.Event()
        self._thread = None

    def add(self, namespace, service, ip, port, weight, metadata):
        instance = {"namespace": namespace, "service": service, "ip": ip,
                    "port": port, "weight": weight, "metadata": metadata}
        self.instances.append(instance)
        self._beat(instance)

    def start(self):
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()

    def _run(self):
        while not self._stop.wait(4):
            for instance in list(self.instances):
                self._beat(instance)

    def _beat(self, instance):
        params = {
            "serviceName": instance["service"], "ip": instance["ip"],
            "port": instance["port"], "groupName": "DEFAULT_GROUP",
            "clusterName": "DEFAULT", "weight": instance["weight"],
            "ephemeral": "true", "heartBeat": "true",
        }
        if instance["namespace"]:
            params["namespaceId"] = instance["namespace"]
        status, body = self.console.client_post("/v3/client/ns/instance", params)
        if status != 200 or '"code":0' not in body.replace(" ", ""):
            register_instance_v3(self.console, instance["namespace"], instance["service"],
                                 instance["ip"], instance["port"], instance["weight"], instance["metadata"])


def prepare_fixtures_v3(console):
    """写入全部 v3 测试数据；重复运行安全。"""
    for ns_id, name in (("dev", "dev 环境"), ("test", "test 环境"), ("prod", "prod 环境")):
        ensure_namespace_v3(console, ns_id, name)

    publish_v3(console, "dev", "gateway.yaml", GATEWAY_V1, "yaml")
    publish_v3(console, "dev", "gateway.yaml", GATEWAY_DEV, "yaml")
    publish_v3(console, "dev", "app.properties", APP_DEV, "properties")
    publish_v3(console, "dev", "extra-config.json", EXTRA_JSON, "json")

    publish_v3(console, "test", "gateway.yaml", GATEWAY_DEV, "yaml")
    publish_v3(console, "test", "app.properties", APP_TEST, "properties")
    publish_v3(console, "prod", "gateway.yaml", GATEWAY_PROD, "yaml")

    # 写后读缓存 1-3 秒：等最终版本在 client 读接口可见。
    deadline = time.time() + 20
    while time.time() < deadline:
        status, body = console.client_get("/v3/client/cs/config", {
            "dataId": "gateway.yaml", "groupName": "DEFAULT_GROUP", "namespaceId": "dev"})
        try:
            content = json.loads(body).get("data", {}).get("content")
        except Exception:
            content = None
        if status == 200 and content == GATEWAY_DEV:
            break
        time.sleep(0.5)
    else:
        raise RuntimeError(f"dev/gateway.yaml 最终版本 20 秒内不可见: HTTP {status} {body[:200]}")

    # 命名空间计数（configCount）也需要一点时间追上，等 dev == 3 再继续，
    # 避免 list_namespaces 断言撞上统计延迟。
    deadline = time.time() + 15
    while time.time() < deadline:
        status, body = console.get("/v3/console/core/namespace/list")
        try:
            counts = {item["namespace"]: item.get("configCount") for item in json.loads(body)["data"]}
        except Exception:
            counts = {}
        if counts.get("dev") == 3:
            return
        time.sleep(0.5)
    raise RuntimeError(f"dev 命名空间 configCount 15 秒内未到 3（当前 {counts.get('dev')}）")


def register_instances_v3(console, heartbeats):
    fixtures = [
        ("dev", "order-service", "10.210.1.11", 8080, "1.0", {"version": "1.0"}),
        ("dev", "order-service", "10.210.1.12", 8080, "1.5", {}),
        ("dev", "user-service", "10.210.2.21", 9090, "1.0", {}),
    ]
    for namespace, service, ip, port, weight, metadata in fixtures:
        register_instance_v3(console, namespace, service, ip, port, weight, metadata)
        heartbeats.add(namespace, service, ip, port, weight, metadata)
    deadline = time.time() + 15
    visible = 0
    while time.time() < deadline:
        status, body = console.client_get("/v3/client/ns/instance/list", {
            "serviceName": "order-service", "namespaceId": "dev", "groupName": "DEFAULT_GROUP"})
        try:
            visible = len(json.loads(body).get("data") or [])
        except Exception:
            visible = 0
        if visible == 2:
            return
        time.sleep(0.5)
    raise RuntimeError(f"注册后 15 秒内未观察到 order-service 的 2 个实例（当前 {visible} 个）")


# --- sidecar 驱动 --------------------------------------------------------------

class Sidecar:
    def __init__(self, binary):
        self.process = subprocess.Popen(
            [binary], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        self.next_id = 0

    def request(self, method, params):
        self.next_id += 1
        message = {"jsonrpc": "2.0", "id": self.next_id, "method": method, "params": params}
        self.process.stdin.write(json.dumps(message) + "\n")
        self.process.stdin.flush()
        for _ in range(200):
            line = self.process.stdout.readline()
            if not line:
                raise RuntimeError(f"{method} 无响应（sidecar 已退出）")
            line = line.strip()
            if not line:
                continue
            payload = json.loads(line)
            if payload.get("id") == self.next_id:
                return payload
        raise RuntimeError(f"{method} 未收到匹配响应（id={self.next_id}）")

    def call_tool(self, tool, arguments, connection):
        return self.request("mcp/call", {
            "tool": tool, "arguments": arguments, "lifecycle": {"connection": connection},
        })

    def close(self):
        try:
            self.process.stdin.close()
        except Exception:
            pass
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()


def tool_text(response):
    """取出 mcp/call 响应的文本内容。"""
    return response.get("result", {}).get("content", [{}])[0].get("text", "")


def tool_json(response):
    """mcp/call 的文本内容按 JSON 解析（失败返回空字典）。"""
    try:
        return json.loads(tool_text(response))
    except Exception:
        return {}


def tool_is_error(response):
    return response.get("result", {}).get("isError") is True


def base_connection(port, console_port=None, namespace="dev"):
    connection = {
        "id": "e2e-base", "name": "E2E Nacos (docker)",
        "host": "127.0.0.1", "port": port,
        "external_config": {"context_path": "/nacos", "namespace": namespace},
    }
    if console_port:
        # v3：console API 始终要求账号；同时显式给出 console 端口。
        connection["username"] = "nacos"
        connection["password"] = "nacos"
        connection["external_config"]["console_port"] = console_port
    return connection


# --- 测试套件：基础功能（无鉴权）------------------------------------------------

def run_base_suite(checks, sidecar, port, v3=False, console_port=None):
    connection = base_connection(port, console_port)
    checks.section("阶段 1：连接生命周期与工具目录")

    init = sidecar.request("plugin/initialize", {"host": {"protocolVersions": [1]}})
    result = init.get("result", {})
    checks.check("plugin/initialize 协商协议 v1", result.get("protocolVersion") == 1, json.dumps(init, ensure_ascii=False))
    checks.check("插件 id 与 manifest 一致",
                 result.get("plugin", {}).get("id") == "io.github.ct17.nacos_toolkit",
                 json.dumps(result, ensure_ascii=False))

    test = sidecar.request("connection/test", {"connection": connection})
    message = test.get("result", {}).get("message", "")
    checks.check("connection/test 连通性 + 数据面探测通过",
                 test.get("result", {}).get("success") is True and "ready" in message,
                 json.dumps(test, ensure_ascii=False))
    checks.check(f"connection/test 报告协议栈 {'v3' if v3 else 'v1'}",
                 f"API {'v3' if v3 else 'v1'}" in message,
                 json.dumps(test, ensure_ascii=False))

    dead = dict(connection, id="e2e-dead", port=59999)
    dead_test = sidecar.request("connection/test", {"connection": dead})
    checks.check("connection/test 对不可达端口给出错误",
                 bool(dead_test.get("error", {}).get("message")),
                 json.dumps(dead_test, ensure_ascii=False))

    connect = sidecar.request("connection/connect", {"connection": connection})
    checks.check("connection/connect 建立连接",
                 connect.get("result", {}).get("success") is True,
                 json.dumps(connect, ensure_ascii=False))

    tools = sidecar.request("mcp/tools", {"connectionId": "e2e-base"})
    catalogue = tools.get("result", {}).get("tools", [])
    names = sorted(tool["name"] for tool in catalogue)
    expected = ["diff_config", "get_config", "get_config_history", "list_instances",
                "list_namespaces", "list_services", "search_configs"]
    checks.check("mcp/tools 返回全部 7 个工具", names == expected, json.dumps(names, ensure_ascii=False))
    checks.check("全部工具标注 readOnlyHint（只读）",
                 bool(catalogue) and all(t.get("annotations", {}).get("readOnlyHint") is True for t in catalogue))

    ping = sidecar.request("nacos/ping", {"connectionId": "e2e-base"})
    ping_result = ping.get("result", {})
    checks.check("nacos/ping 工作台健康探测",
                 ping_result.get("ok") is True
                 and str(ping_result.get("baseUrl", "")).endswith("/nacos")
                 and ping_result.get("api") == ("v3" if v3 else "v1"),
                 json.dumps(ping, ensure_ascii=False))

    # 显式 api_version 覆盖：跳过自动探测直连指定协议栈（连接表单的恢复手段）。
    # 浅拷贝后重建 external_config，避免污染基础连接。
    override = dict(connection, id="e2e-api-override")
    override["external_config"] = dict(connection["external_config"],
                                       api_version="v3" if v3 else "v1")
    override_test = sidecar.request("connection/test", {"connection": override})
    override_message = override_test.get("result", {}).get("message", "")
    checks.check("connection/test：显式 api_version 覆盖自动探测",
                 override_test.get("result", {}).get("success") is True
                 and f"API {'v3' if v3 else 'v1'}" in override_message,
                 json.dumps(override_test, ensure_ascii=False))

    # --- 七个工具逐个验证 ---
    checks.section("阶段 2：七个只读工具逐项验证")

    call = sidecar.call_tool("get_config", {"dataId": "gateway.yaml"}, connection)
    payload = tool_json(call)
    checks.check("get_config：读取 dev/gateway.yaml 完整内容",
                 payload.get("content") == GATEWAY_DEV and payload.get("namespace") == "dev",
                 tool_text(call))

    call = sidecar.call_tool("get_config", {"dataId": "gateway.yaml", "namespace": "prod"}, connection)
    payload = tool_json(call)
    checks.check("get_config：namespace 参数覆盖连接默认值（读 prod）",
                 payload.get("content") == GATEWAY_PROD,
                 tool_text(call))

    call = sidecar.call_tool("search_configs", {}, connection)
    payload = tool_json(call)
    checks.check("search_configs：全量搜索 dev 共 3 条",
                 payload.get("totalCount") == 3 and len(payload.get("items", [])) == 3,
                 tool_text(call))

    call = sidecar.call_tool("search_configs", {"dataId": "gateway"}, connection)
    payload = tool_json(call)
    items = payload.get("items", [])
    checks.check("search_configs：关键词过滤命中 1 条且无内容泄漏",
                 payload.get("totalCount") == 1 and bool(items)
                 and items[0].get("dataId") == "gateway.yaml" and "content" not in items[0],
                 tool_text(call))

    call = sidecar.call_tool("search_configs", {"pageSize": 2}, connection)
    payload = tool_json(call)
    checks.check("search_configs：pageSize=2 分页正确（共 2 页）",
                 payload.get("totalCount") == 3 and len(payload.get("items", [])) == 2
                 and payload.get("pagesAvailable") == 2,
                 tool_text(call))

    call = sidecar.call_tool("diff_config",
                             {"dataId": "gateway.yaml", "namespaces": ["dev", "test", "prod"]}, connection)
    payload = tool_json(call)
    comparisons = {entry.get("namespace"): entry for entry in payload.get("comparisons", [])}
    checks.check("diff_config：dev 与 test 完全相同 -> identical",
                 comparisons.get("test", {}).get("status") == "identical",
                 tool_text(call))
    prod_entry = comparisons.get("prod", {})
    checks.check("diff_config：dev 与 prod 差异 -> 行级 diff（+8080 / -9000）",
                 prod_entry.get("status") == "different"
                 and "+   port: 8080" in prod_entry.get("diff", "")
                 and "-   port: 9000" in prod_entry.get("diff", "")
                 and prod_entry.get("added") == 2 and prod_entry.get("removed") == 2,
                 tool_text(call))

    call = sidecar.call_tool("diff_config",
                             {"dataId": "app.properties", "namespaces": ["dev", "prod"]}, connection)
    payload = tool_json(call)
    entry = (payload.get("comparisons") or [{}])[0]
    checks.check("diff_config：prod 缺少该配置 -> missing-in-target",
                 entry.get("status") == "missing-in-target",
                 tool_text(call))

    call = sidecar.call_tool("get_config_history", {"dataId": "gateway.yaml"}, connection)
    payload = tool_json(call)
    revisions = payload.get("revisions", [])
    checks.check("get_config_history：列出变更历史（≥2 条）",
                 payload.get("totalCount", 0) >= 2 and len(revisions) >= 2,
                 tool_text(call))
    checks.check("get_config_history：条目含 historyId / opType / 时间字段",
                 bool(revisions) and all(rev.get("historyId") and rev.get("opType") and rev.get("createdTime")
                                         for rev in revisions),
                 tool_text(call))
    checks.check("get_config_history：opType 已清除定长空格（I/U/D 枚举值）",
                 bool(revisions) and all(rev.get("opType") in ("I", "U", "D") for rev in revisions),
                 tool_text(call))
    checks.check("get_config_history：时间取自真实变更时间（非 derby 2010 占位值）",
                 bool(revisions) and all(not rev.get("createdTime", "").startswith("2010")
                                         for rev in revisions),
                 tool_text(call))
    if revisions:
        first = revisions[0]
        call = sidecar.call_tool("get_config_history",
                                 {"dataId": "gateway.yaml", "historyId": first["historyId"]}, connection)
        payload = tool_json(call)
        checks.check("get_config_history：按 historyId 取单版本详情含内容",
                     payload.get("historyId") == first["historyId"] and bool(payload.get("content")),
                     tool_text(call))

    call = sidecar.call_tool("list_namespaces", {}, connection)
    payload = tool_json(call)
    by_id = {item.get("namespace"): item for item in payload.get("items", [])}
    # 2.x 的 public 命名空间 id 为空串，3.x 为 "public"，两者都接受。
    checks.check("list_namespaces：包含 public/dev/test/prod",
                 {"dev", "test", "prod"} <= set(by_id) and ("public" in by_id or "" in by_id),
                 tool_text(call))
    checks.check("list_namespaces：dev 的 configCount 统计正确（=3）",
                 by_id.get("dev", {}).get("configCount") == 3,
                 tool_text(call))

    call = sidecar.call_tool("list_services", {}, connection)
    payload = tool_json(call)
    checks.check("list_services：列出 dev 下 order-service / user-service",
                 sorted(payload.get("services", [])) == ["order-service", "user-service"],
                 tool_text(call))

    call = sidecar.call_tool("list_instances", {"serviceName": "order-service"}, connection)
    payload = tool_json(call)
    instances = payload.get("instances", [])
    ip_set = sorted(inst.get("ip", "") for inst in instances)
    checks.check("list_instances：order-service 两个实例（含健康数）",
                 payload.get("instanceCount") == 2 and ip_set == ["10.210.1.11", "10.210.1.12"]
                 and payload.get("healthyCount") == 2,
                 tool_text(call))
    # valid 字段只在服务端真的提供时才输出（2.4.3 与 3.2.4 的实测响应都没有该
    # 字段，工具已修正为不伪造 false），因此这里只断言公共字段。
    required_fields = ("ip", "port", "healthy", "enabled", "weight", "cluster")
    checks.check("list_instances：实例字段完整（健康/权重/集群/metadata）",
                 all(all(key in inst for key in required_fields) for inst in instances)
                 and any(inst.get("metadata") for inst in instances),
                 tool_text(call))

    # cluster 过滤：测试实例均注册在 DEFAULT 集群（v1 传 clusters，v3 传 clusterName）。
    call = sidecar.call_tool("list_instances",
                             {"serviceName": "order-service", "cluster": "DEFAULT"}, connection)
    payload = tool_json(call)
    checks.check("list_instances：cluster 过滤命中 DEFAULT 集群（2 实例）",
                 payload.get("instanceCount") == 2 and len(payload.get("instances", [])) == 2,
                 tool_text(call))
    call = sidecar.call_tool("list_instances",
                             {"serviceName": "order-service", "cluster": "no-such-cluster"}, connection)
    payload = tool_json(call)
    checks.check("list_instances：cluster 过滤无命中 -> 0 实例",
                 payload.get("instanceCount") == 0 and payload.get("instances") == [],
                 tool_text(call))

    # --- 错误路径 ---
    checks.section("阶段 3：错误路径与边界")

    call = sidecar.call_tool("get_config", {"dataId": "no-such.yaml"}, connection)
    checks.check("get_config：不存在的配置 -> isError + not found",
                 tool_is_error(call) and "not found" in tool_text(call),
                 tool_text(call))

    call = sidecar.call_tool("get_config", {}, connection)
    checks.check("get_config：缺少 dataId -> isError",
                 tool_is_error(call) and "dataId is required" in tool_text(call),
                 tool_text(call))

    call = sidecar.call_tool("diff_config", {"dataId": "gateway.yaml", "namespaces": ["dev"]}, connection)
    checks.check("diff_config：少于两个命名空间 -> isError",
                 tool_is_error(call) and "at least two" in tool_text(call),
                 tool_text(call))

    call = sidecar.call_tool("list_instances", {}, connection)
    checks.check("list_instances：缺少 serviceName -> isError",
                 tool_is_error(call) and "serviceName is required" in tool_text(call),
                 tool_text(call))

    call = sidecar.call_tool("no_such_tool", {}, connection)
    checks.check("未知工具名 -> isError + unknown tool",
                 tool_is_error(call) and "unknown tool" in tool_text(call),
                 tool_text(call))

    ghost = sidecar.request("mcp/call", {
        "tool": "get_config", "arguments": {"dataId": "gateway.yaml"},
        "lifecycle": {"connection": {"id": "ghost"}},
    })
    checks.check("无连接信息调用工具 -> 可读错误提示",
                 tool_is_error(ghost) and "no usable Nacos connection" in tool_text(ghost),
                 tool_text(ghost))

    unknown = sidecar.request("does/not/exist", {})
    checks.check("未知方法 -> JSON-RPC error -32601",
                 unknown.get("error", {}).get("code") == -32601,
                 json.dumps(unknown, ensure_ascii=False))

    disconnect = sidecar.request("connection/disconnect", {"connection": {"id": "e2e-base"}})
    checks.check("connection/disconnect 断开连接",
                 disconnect.get("result", {}).get("success") is True,
                 json.dumps(disconnect, ensure_ascii=False))


# --- 测试套件：鉴权模式 ---------------------------------------------------------

def run_auth_suite(checks, sidecar, port, v3=False, console_port=None):
    checks.section(f"阶段 4：鉴权模式（NACOS_AUTH_ENABLE=true，{'v3' if v3 else 'v1'}）")

    if v3:
        ensure_admin_v3(console_port)
        token = login_v3(console_port, "nacos", "nacos")
        console = ConsoleV3(console_port, port, token)
        try:
            publish_v3(console, "", "auth-test.properties", "auth.enabled=true\n", "properties")
            checks.check("（前置）写入鉴权测试配置 auth-test.properties", True)
        except RuntimeError as err:
            checks.check("（前置）写入鉴权测试配置 auth-test.properties", False, str(err)[:200])
            return
        # 写后读缓存：等配置在 client 读接口可见。
        deadline = time.time() + 10
        while time.time() < deadline:
            status, body = console.client_get("/v3/client/cs/config", {
                "dataId": "auth-test.properties", "groupName": "DEFAULT_GROUP"})
            try:
                content = json.loads(body).get("data", {}).get("content")
            except Exception:
                content = None
            if status == 200 and content and "auth.enabled=true" in content:
                break
            time.sleep(0.5)
    else:
        ensure_admin(port)
        token = login(port, "nacos", "nacos")
        admin = Admin(port, token=token)
        try:
            # 走 publish() 以获得 3.2+ 的 v2 写入口回退；auth-test.properties 位于 public 命名空间。
            publish(admin, "", "auth-test.properties", "auth.enabled=true\n", "properties")
            checks.check("（前置）写入鉴权测试配置 auth-test.properties", True)
        except RuntimeError as err:
            checks.check("（前置）写入鉴权测试配置 auth-test.properties", False, str(err)[:200])
            return

        # Nacos 3.x 的配置写入存在 1-2 秒读缓存延迟，等待可见后再继续。
        deadline = time.time() + 10
        while time.time() < deadline:
            status, body = admin.get("/v1/cs/configs", {
                "dataId": "auth-test.properties", "group": "DEFAULT_GROUP"})
            if status == 200 and "auth.enabled=true" in body:
                break
            time.sleep(0.5)

    external = {"context_path": "/nacos"}
    if console_port:
        external["console_port"] = console_port
    connection = {
        "id": "e2e-auth", "name": "E2E Nacos (auth)",
        "host": "127.0.0.1", "port": port, "username": "nacos", "password": "nacos",
        "external_config": external,
    }

    test = sidecar.request("connection/test", {"connection": connection})
    checks.check("正确凭据 connection/test 成功（健康 + 数据面）",
                 test.get("result", {}).get("success") is True,
                 json.dumps(test, ensure_ascii=False))

    connect = sidecar.request("connection/connect", {"connection": connection})
    checks.check("正确凭据 connection/connect 成功",
                 connect.get("result", {}).get("success") is True,
                 json.dumps(connect, ensure_ascii=False))

    call = sidecar.call_tool("get_config", {"dataId": "auth-test.properties"}, connection)
    payload = tool_json(call)
    checks.check("正确凭据 get_config（登录 + token 链路）",
                 payload.get("content") == "auth.enabled=true\n",
                 tool_text(call))

    call = sidecar.call_tool("list_namespaces", {}, connection)
    checks.check("正确凭据 list_namespaces 成功", not tool_is_error(call), tool_text(call))

    wrong = dict(connection, id="e2e-auth-wrong", password="wrong-password")
    test = sidecar.request("connection/test", {"connection": wrong})
    err = test.get("error", {}).get("message", "")
    checks.check("错误密码 connection/test -> 登录被拒提示",
                 "wrong username or password" in err,
                 json.dumps(test, ensure_ascii=False))

    anonymous = dict(connection, id="e2e-auth-anon", username="", password="")
    test = sidecar.request("connection/test", {"connection": anonymous})
    err = test.get("error", {}).get("message", "")
    checks.check("无凭据 connection/test -> 数据面探测拦截",
                 "reading configs failed" in err,
                 json.dumps(test, ensure_ascii=False))

    call = sidecar.call_tool("get_config", {"dataId": "auth-test.properties"}, anonymous)
    checks.check("无凭据 get_config -> isError（HTTP 403）",
                 tool_is_error(call) and "403" in tool_text(call),
                 tool_text(call))


# --- 入口 ---------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Nacos Toolkit 插件 Docker E2E 测试")
    parser.add_argument("--port", type=int, default=18848, help="无鉴权 Nacos 服务端口（默认 18848）")
    parser.add_argument("--console-port", type=int, default=None,
                        help="无鉴权 Nacos 3.x console 端口（提供即按 v3 模式测试）")
    parser.add_argument("--auth-port", type=int, default=None, help="开启鉴权的 Nacos 服务端口（可选）")
    parser.add_argument("--auth-console-port", type=int, default=None,
                        help="开启鉴权的 Nacos 3.x console 端口（v3 鉴权链路必需）")
    parser.add_argument("--no-prepare", action="store_true", help="跳过测试数据准备")
    args = parser.parse_args()

    checks = Checks()
    v3 = bool(args.console_port)
    auth_v3 = bool(args.auth_console_port)

    admin = Admin(args.port)
    heartbeats = None
    if v3:
        print(f"等待 Nacos 3.x console 127.0.0.1:{args.console_port} 就绪…")
        wait_ready_v3(args.console_port)
        if not args.no_prepare:
            print("准备测试数据（v3 console/client API）…")
            ensure_admin_v3(args.console_port)
            token = login_v3(args.console_port, "nacos", "nacos")
            console = ConsoleV3(args.console_port, args.port, token)
            prepare_fixtures_v3(console)
            heartbeats = HeartbeatsV3(console)
            register_instances_v3(console, heartbeats)
            heartbeats.start()
    else:
        print(f"等待 Nacos 127.0.0.1:{args.port} 就绪…")
        wait_ready(args.port)
        if not args.no_prepare:
            print("准备测试数据（命名空间 / 配置与历史 / 服务实例）…")
            prepare_fixtures(admin)
            heartbeats = Heartbeats(admin)
            register_instances(admin, heartbeats)
            heartbeats.start()

    try:
        with tempfile.TemporaryDirectory() as tmp:
            binary = os.path.join(tmp, "dbx-plugin-nacos-toolkit")
            print("构建 sidecar…")
            subprocess.run(["go", "build", "-o", binary, "."], cwd=BACKEND_DIR, check=True)

            sidecar = Sidecar(binary)
            try:
                run_base_suite(checks, sidecar, args.port, v3=v3, console_port=args.console_port)
            finally:
                sidecar.close()

            if args.auth_port:
                if auth_v3:
                    print(f"等待鉴权 Nacos 3.x console 127.0.0.1:{args.auth_console_port} 就绪…")
                    wait_ready_v3(args.auth_console_port)
                else:
                    print(f"等待鉴权 Nacos 127.0.0.1:{args.auth_port} 就绪…")
                    wait_ready(args.auth_port)
                sidecar = Sidecar(binary)
                try:
                    run_auth_suite(checks, sidecar, args.auth_port,
                                   v3=auth_v3, console_port=args.auth_console_port)
                finally:
                    sidecar.close()
    finally:
        if heartbeats:
            heartbeats.stop()

    print()
    failures = checks.report()
    total = sum(1 for _, name, ok, _ in checks.results if ok is not None)
    print()
    if failures:
        print(f"E2E FAILED：{len(failures)}/{total} 项失败：")
        for name in failures:
            print(f"  - {name}")
        return 1
    print(f"E2E PASSED：{total} 项检查全部通过。")
    return 0


if __name__ == "__main__":
    sys.exit(main())

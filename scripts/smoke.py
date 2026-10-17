#!/usr/bin/env python3
"""Protocol-level smoke test for the Nacos Toolkit DBX plugin sidecar.

Builds the Go sidecar, starts a mock Nacos server, then drives the sidecar
over the DBX plugin stdio protocol (JSON-RPC 2.0, one message per line)
exactly like the DBX host does:

    plugin/initialize -> connection/test -> connection/connect
    -> mcp/tools -> mcp/call (get_config / search_configs / find_in_configs /
       diff_envs / diff_history / diagnose / list_services)

Usage:
    python3 scripts/smoke.py

Exit code 0 on success, 1 on failure.
"""

import http.server
import json
import os
import subprocess
import sys
import tempfile
import threading
import urllib.parse

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BACKEND_DIR = os.path.join(ROOT, "backend")

TIMEOUT_SECONDS = 30


# --- mock Nacos -------------------------------------------------------------

class MockNacos(http.server.BaseHTTPRequestHandler):
    token = "smoke-token"

    def log_message(self, *args):  # keep the output clean
        pass

    def _json(self, payload, status=200):
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _text(self, value, status=200):
        body = value.encode()
        self.send_response(status)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        self.rfile.read(length)
        if self.path.startswith("/nacos/v1/auth/login"):
            self._json({"accessToken": self.token, "tokenTtl": 18000})
        else:
            self._text("not found", 404)

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        query = urllib.parse.parse_qs(parsed.query)
        path = parsed.path

        if path == "/nacos/v1/console/health/readiness":
            self._json({"status": "UP"})
            return

        if path == "/nacos/v1/console/server/state":
            self._json({"version": "2.4.3", "serverEnv": "standalone", "auth_enabled": "true"})
            return

        if query.get("accessToken", [""])[0] != self.token:
            self._text("user not found!", 403)
            return

        if path == "/nacos/v1/cs/configs":
            if query.get("search", [""])[0] == "blur":
                tenant = query.get("tenant", [""])[0]
                if tenant == "prod":
                    self._json({
                        "totalCount": 2,
                        "pageNumber": 1,
                        "pagesAvailable": 1,
                        "pageItems": [
                            {"dataId": "gateway.yaml", "group": "DEFAULT_GROUP",
                             "tenant": "prod", "type": "yaml", "md5": "def"},
                            {"dataId": "extra.yaml", "group": "DEFAULT_GROUP",
                             "tenant": "prod", "type": "yaml", "md5": "new"},
                        ],
                    })
                elif tenant == "dev":
                    self._json({
                        "totalCount": 2,
                        "pageNumber": 1,
                        "pagesAvailable": 1,
                        "pageItems": [
                            {"dataId": "gateway.yaml", "group": "DEFAULT_GROUP",
                             "tenant": "dev", "type": "yaml", "md5": "abc"},
                            {"dataId": "stale.yaml", "group": "DEFAULT_GROUP",
                             "tenant": "dev", "type": "yaml", "md5": "old"},
                        ],
                    })
                else:
                    self._json({"totalCount": 0, "pageNumber": 1,
                                "pagesAvailable": 0, "pageItems": []})
                return
            if query.get("dataId", [""])[0] == "gateway.yaml":
                # prod 命名空间返回不同内容，供 diff_config 冒烟断言使用。
                tenant = query.get("tenant", [""])[0]
                if tenant == "prod":
                    self._text("server:\n  port: 8080\n")
                else:
                    self._text("server:\n  port: 9000\n")
                return
            self._json({"status": 404, "message": "config data not exist"}, 404)
            return

        if path == "/nacos/v1/console/namespaces":
            self._json({"code": 200, "data": [
                {"namespace": "", "namespaceShowName": "public", "configCount": 1},
                {"namespace": "dev", "namespaceShowName": "dev", "configCount": 1},
            ]})
            return

        if path == "/nacos/v1/ns/service/list":
            self._json({"count": 1, "doms": ["order-service"]})
            return

        if path == "/nacos/v1/ns/instance/list":
            self._json({
                "name": "DEFAULT_GROUP@@order-service",
                "reachable": True,
                "hosts": [
                    {"ip": "10.0.0.1", "port": 8080, "healthy": True, "enabled": True,
                     "valid": True, "weight": 1, "clusterName": "DEFAULT", "metadata": {}},
                ],
            })
            return

        if path == "/nacos/v1/cs/history":
            if query.get("nid", [""])[0]:
                self._json({
                    "id": "1001", "dataId": "gateway.yaml", "group": "DEFAULT_GROUP",
                    "tenant": "dev", "md5": "aaa", "srcUser": "alice", "srcIp": "10.1.1.1",
                    "opType": "U", "createdTime": 1760000000000,
                    "content": "server:\n  port: 8999\n",
                })
                return
            self._json({
                "totalCount": 1,
                "pageNumber": 1,
                "pagesAvailable": 1,
                "pageItems": [
                    {"id": "1001", "dataId": "gateway.yaml", "group": "DEFAULT_GROUP",
                     "tenant": "dev", "md5": "aaa", "srcUser": "alice", "srcIp": "10.1.1.1",
                     "opType": "U", "createdTime": 1760000000000},
                ],
            })
            return

        self._text("not found", 404)


def start_mock_nacos():
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), MockNacos)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


# --- sidecar driver ---------------------------------------------------------

class Sidecar:
    def __init__(self, binary):
        self.process = subprocess.Popen(
            [binary],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

    def request(self, message_id, method, params):
        message = {"jsonrpc": "2.0", "id": message_id, "method": method, "params": params}
        self.process.stdin.write(json.dumps(message) + "\n")
        self.process.stdin.flush()
        for _ in range(50):
            line = self.process.stdout.readline()
            if not line:
                break
            line = line.strip()
            if not line:
                continue
            payload = json.loads(line)
            if payload.get("id") == message_id:
                return payload
        raise RuntimeError(f"no response for {method} (id={message_id})")

    def close(self):
        try:
            self.process.stdin.close()
        except Exception:
            pass
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()


def main():
    failures = []

    def check(name, condition, detail=""):
        status = "ok" if condition else "FAIL"
        print(f"  [{status}] {name}" + (f" — {detail}" if detail and not condition else ""))
        if not condition:
            failures.append(name)

    # 1. Build the sidecar.
    with tempfile.TemporaryDirectory() as tmp:
        binary = os.path.join(tmp, "dbx-plugin-nacos-toolkit")
        print("building sidecar…")
        subprocess.run(
            ["go", "build", "-o", binary, "."],
            cwd=BACKEND_DIR, check=True,
        )

        # 2. Start the mock Nacos.
        mock = start_mock_nacos()
        mock_port = mock.server_address[1]
        print(f"mock Nacos on 127.0.0.1:{mock_port}")

        # 3. Drive the protocol.
        sidecar = Sidecar(binary)
        try:
            print("protocol walkthrough:")

            init = sidecar.request(1, "plugin/initialize",
                                   {"host": {"protocolVersions": [1]}})
            result = init.get("result", {})
            check("plugin/initialize negotiates protocol 1",
                  result.get("protocolVersion") == 1)
            check("plugin identity matches the manifest",
                  result.get("plugin", {}).get("id") == "io.github.ct17.nacos_toolkit",
                  json.dumps(result))

            connection = {
                "id": "smoke-conn",
                "name": "Smoke Nacos",
                "host": "127.0.0.1",
                "port": mock_port,
                "username": "nacos",
                "password": "nacos",
                # api_version 固定 v1：mock 是纯 v1 服务，跳过自动探测保持确定性。
                "external_config": {"context_path": "/nacos", "namespace": "dev", "api_version": "v1"},
            }
            test = sidecar.request(2, "connection/test", {"connection": connection})
            check("connection/test succeeds against the mock",
                  test.get("result", {}).get("success") is True,
                  json.dumps(test))

            connect = sidecar.request(3, "connection/connect", {"connection": connection})
            check("connection/connect succeeds",
                  connect.get("result", {}).get("success") is True,
                  json.dumps(connect))

            tools = sidecar.request(4, "mcp/tools", {"connectionId": "smoke-conn"})
            catalogue = tools.get("result", {}).get("tools", [])
            names = sorted(tool["name"] for tool in catalogue)
            check("mcp/tools lists the 11 read-only tools",
                  names == ["diagnose", "diff_config", "diff_envs", "diff_history",
                            "find_in_configs", "get_config", "get_config_history",
                            "list_instances", "list_namespaces", "list_services",
                            "search_configs"],
                  json.dumps(names))
            check("every tool is marked readOnlyHint",
                  all(tool.get("annotations", {}).get("readOnlyHint") is True
                      for tool in catalogue))

            call = sidecar.request(5, "mcp/call", {
                "tool": "get_config",
                "arguments": {"dataId": "gateway.yaml"},
                "lifecycle": {"connection": connection},
            })
            text = call.get("result", {}).get("content", [{}])[0].get("text", "")
            check("mcp/call get_config returns the config content",
                  "port: 9000" in text, text)
            check("mcp/call result is not an error",
                  call.get("result", {}).get("isError") is False, text)

            call = sidecar.request(6, "mcp/call", {
                "tool": "list_services",
                "arguments": {},
                "lifecycle": {"connection": connection},
            })
            text = call.get("result", {}).get("content", [{}])[0].get("text", "")
            check("mcp/call list_services returns service names",
                  "order-service" in text, text)

            missing = sidecar.request(7, "mcp/call", {
                "tool": "get_config",
                "arguments": {"dataId": "nope.yaml"},
                "lifecycle": {"connection": connection},
            })
            check("missing config becomes an isError result",
                  missing.get("result", {}).get("isError") is True)

            diff_call = sidecar.request(8, "mcp/call", {
                "tool": "diff_config",
                "arguments": {"dataId": "gateway.yaml", "namespaces": ["dev", "prod"]},
                "lifecycle": {"connection": connection},
            })
            text = diff_call.get("result", {}).get("content", [{}])[0].get("text", "")
            check("mcp/call diff_config reports the namespace difference",
                  '"status": "different"' in text and "+   port: 8080" in text, text)

            instances = sidecar.request(9, "mcp/call", {
                "tool": "list_instances",
                "arguments": {"serviceName": "order-service"},
                "lifecycle": {"connection": connection},
            })
            text = instances.get("result", {}).get("content", [{}])[0].get("text", "")
            check("mcp/call list_instances returns the instance list",
                  "10.0.0.1" in text and '"instanceCount": 1' in text, text)

            history = sidecar.request(11, "mcp/call", {
                "tool": "get_config_history",
                "arguments": {"dataId": "gateway.yaml"},
                "lifecycle": {"connection": connection},
            })
            text = history.get("result", {}).get("content", [{}])[0].get("text", "")
            check("mcp/call get_config_history lists revisions",
                  '"historyId": "1001"' in text and '"operator": "alice"' in text, text)

            content_search = sidecar.request(12, "mcp/call", {
                "tool": "find_in_configs",
                "arguments": {"keyword": "port: 9000"},
                "lifecycle": {"connection": connection},
            })
            text = content_search.get("result", {}).get("content", [{}])[0].get("text", "")
            check("mcp/call find_in_configs returns a matching snippet",
                  '"dataId": "gateway.yaml"' in text and "port: 9000" in text, text)
            check("mcp/call find_in_configs reports scan coverage",
                  '"candidateCount": 2' in text, text)

            env_diff = sidecar.request(13, "mcp/call", {
                "tool": "diff_envs",
                "arguments": {"namespaces": ["dev", "prod"]},
                "lifecycle": {"connection": connection},
            })
            text = env_diff.get("result", {}).get("content", [{}])[0].get("text", "")
            check("mcp/call diff_envs reports baseline/target drift",
                  '"different": 1' in text and '"onlyInBaseline": 1' in text
                  and '"onlyInTarget": 1' in text, text)

            history_diff = sidecar.request(14, "mcp/call", {
                "tool": "diff_history",
                "arguments": {"dataId": "gateway.yaml", "fromHistoryId": "1001"},
                "lifecycle": {"connection": connection},
            })
            text = history_diff.get("result", {}).get("content", [{}])[0].get("text", "")
            check("mcp/call diff_history compares a revision with current",
                  '"status": "different"' in text and "+   port: 9000" in text
                  and "-   port: 8999" in text, text)

            diagnosis = sidecar.request(15, "mcp/call", {
                "tool": "diagnose",
                "arguments": {},
                "lifecycle": {"connection": connection},
            })
            text = diagnosis.get("result", {}).get("content", [{}])[0].get("text", "")
            check("mcp/call diagnose reports an ok connection",
                  '"overall": "ok"' in text and '"version": "2.4.3"' in text
                  and '"protocol": "v1"' in text, text)

            unknown = sidecar.request(16, "does/not-exist", {})
            check("unknown methods return JSON-RPC error -32601",
                  unknown.get("error", {}).get("code") == -32601,
                  json.dumps(unknown))
        finally:
            sidecar.close()
            mock.shutdown()

    print()
    if failures:
        print(f"SMOKE FAILED: {len(failures)} check(s) failed: {', '.join(failures)}")
        return 1
    print("SMOKE PASSED: sidecar protocol walkthrough is clean.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

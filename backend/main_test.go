package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	dbxpluginsdk "github.com/t8y2/dbx/plugins/sdk/go/dbx-plugin-sdk"
)

// --- mock Nacos server ------------------------------------------------------

// newMockNacos starts a fake Nacos server covering the endpoints the plugin
// uses: auth login, config read/search, namespaces, service list, health.
func newMockNacos(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()

	mux.HandleFunc("/nacos/v1/auth/login", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		if r.FormValue("username") != "nacos" || r.FormValue("password") != "nacos" {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"status":403,"message":"user not found!"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"accessToken":"test-token","tokenTtl":18000}`))
	})

	authOK := func(w http.ResponseWriter, r *http.Request) bool {
		if r.URL.Query().Get("accessToken") != "test-token" {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte("user not found!"))
			return false
		}
		return true
	}

	mux.HandleFunc("/nacos/v1/cs/configs", func(w http.ResponseWriter, r *http.Request) {
		if !authOK(w, r) {
			return
		}
		query := r.URL.Query()
		if query.Get("search") == "blur" {
			// Search endpoint: honor tenant and simple keyword filtering.
			if query.Get("tenant") != "dev" {
				_, _ = w.Write([]byte(`{"totalCount":0,"pageNumber":1,"pagesAvailable":0,"pageItems":[]}`))
				return
			}
			_, _ = w.Write([]byte(`{"totalCount":2,"pageNumber":1,"pagesAvailable":1,"pageItems":[
				{"dataId":"gateway.yaml","group":"DEFAULT_GROUP","tenant":"dev","type":"yaml","md5":"aaa","content":"big payload nobody wants in lists"},
				{"dataId":"orders.yaml","group":"DEFAULT_GROUP","tenant":"dev","type":"yaml","md5":"bbb","content":"more payload"}
			]}`))
			return
		}
		switch {
		case query.Get("dataId") == "gateway.yaml" && query.Get("tenant") == "prod":
			// prod 命名空间下同名配置内容不同，供 diff_config 测试使用。
			_, _ = w.Write([]byte("server:\n  port: 8080\nroutes:\n  - id: order\n  - id: user\n"))
		case query.Get("dataId") == "gateway.yaml" && query.Get("tenant") != "empty-ns":
			// dev / dev2 / 空 tenant 返回同一份内容；empty-ns 走 404 分支。
			_, _ = w.Write([]byte("server:\n  port: 9000\nroutes:\n  - id: order\n"))
		case query.Get("dataId") == "hidden.yaml":
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte("authorization failed"))
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"timestamp":"2026-10-16","status":404,"message":"config data not exist"}`))
		}
	})

	mux.HandleFunc("/nacos/v1/console/health/readiness", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"status":"UP"}`))
	})

	mux.HandleFunc("/nacos/v1/console/namespaces", func(w http.ResponseWriter, r *http.Request) {
		if !authOK(w, r) {
			return
		}
		_, _ = w.Write([]byte(`{"code":200,"data":[
			{"namespace":"","namespaceShowName":"public","configCount":3,"quota":200,"type":0},
			{"namespace":"dev","namespaceShowName":"dev","configCount":2,"quota":200,"type":2}
		]}`))
	})

	mux.HandleFunc("/nacos/v1/ns/service/list", func(w http.ResponseWriter, r *http.Request) {
		if !authOK(w, r) {
			return
		}
		_, _ = w.Write([]byte(`{"count":2,"doms":["order-service","gateway-service"]}`))
	})

	mux.HandleFunc("/nacos/v1/ns/instance/list", func(w http.ResponseWriter, r *http.Request) {
		if !authOK(w, r) {
			return
		}
		if r.URL.Query().Get("serviceName") != "order-service" {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"status":404,"message":"service not found"}`))
			return
		}
		_, _ = w.Write([]byte(`{"name":"DEFAULT_GROUP@@order-service","reachable":true,"hosts":[
			{"ip":"10.0.0.1","port":8080,"healthy":true,"enabled":true,"valid":true,"weight":1,"clusterName":"DEFAULT","metadata":{"zone":"az1"}},
			{"ip":"10.0.0.2","port":8080,"healthy":false,"enabled":true,"valid":true,"weight":0.5,"clusterName":"DEFAULT","metadata":{}}
		]}`))
	})

	mux.HandleFunc("/nacos/v1/cs/history", func(w http.ResponseWriter, r *http.Request) {
		if !authOK(w, r) {
			return
		}
		query := r.URL.Query()
		// 复刻 Nacos 真实路由（HistoryController）：带 search=accurate 一律命中
		// 列表方法——即使带了 nid 也返回分页列表；不带 search 且带 nid 才命中
		// 详情方法。回归防护：详情调用若误带 search，此处返回列表结构，插件的
		// 防御检查会报错而不是静默给出空数据。
		if query.Get("search") == "accurate" {
			_, _ = w.Write([]byte(`{"totalCount":2,"pageNumber":1,"pagesAvailable":1,"pageItems":[
				{"id":"1001","dataId":"gateway.yaml","group":"DEFAULT_GROUP","tenant":"dev","md5":"aaa","srcUser":"alice","srcIp":"10.1.1.1","opType":"U         ","createdTime":"2010-05-05T00:00:00.000+08:00","lastModifiedTime":"2026-10-17T00:15:18.031+08:00"},
				{"id":"1000","dataId":"gateway.yaml","group":"DEFAULT_GROUP","tenant":"dev","md5":"bbb","srcUser":"bob","srcIp":"10.1.1.2","opType":"I","createdTime":1759000000000}
			]}`))
			return
		}
		if nid := query.Get("nid"); nid != "" {
			if nid != "1001" {
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(`{"status":404,"message":"no config history"}`))
				return
			}
			_, _ = w.Write([]byte(`{"id":"1001","dataId":"gateway.yaml","group":"DEFAULT_GROUP","tenant":"dev",
				"md5":"aaa","content":"server:\n  port: 8999\n","srcUser":"alice","srcIp":"10.1.1.1",
				"opType":"U         ","createdTime":"2010-05-05T00:00:00.000+08:00",
				"lastModifiedTime":"2026-10-17T00:15:18.031+08:00"}`))
			return
		}
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"status":400,"message":"missing nid"}`))
	})

	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server
}

func mockHostPort(t *testing.T, rawURL string) (string, int) {
	t.Helper()
	parsed, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("parse mock url: %v", err)
	}
	port, err := strconv.Atoi(parsed.Port())
	if err != nil {
		t.Fatalf("parse mock port: %v", err)
	}
	return parsed.Hostname(), port
}

// connectionPayload builds a lifecycle connection object the way the host
// injects it, pointing at the mock server. api_version 固定为 v1：这是一个纯
// v1 mock，跳过自动探测让用例保持确定性（探测逻辑由专项用例覆盖）。
func connectionPayload(t *testing.T, server *httptest.Server) map[string]any {
	t.Helper()
	host, port := mockHostPort(t, server.URL)
	return map[string]any{
		"id":       "conn-1",
		"name":     "Nacos dev",
		"host":     host,
		"port":     float64(port),
		"username": "nacos",
		"password": "nacos",
		"external_config": map[string]any{
			"context_path": "/nacos",
			"namespace":    "dev",
			"api_version":  "v1",
		},
	}
}

// --- harness ----------------------------------------------------------------

func newTestPlugin() *plugin {
	return newPlugin()
}

func callHandle(t *testing.T, p *plugin, method string, params map[string]any) (map[string]any, *dbxpluginsdk.PluginError) {
	t.Helper()
	raw, err := json.Marshal(params)
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}
	result, pluginErr := p.Handle(dbxpluginsdk.RequestContext{}, method, raw, nil)
	if pluginErr != nil {
		return nil, pluginErr
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("marshal result: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	return decoded, nil
}

func callToolRaw(t *testing.T, p *plugin, tool string, arguments map[string]any, connection map[string]any) map[string]any {
	t.Helper()
	lifecycle := map[string]any{}
	if connection != nil {
		lifecycle["connection"] = connection
	}
	result, pluginErr := callHandle(t, p, "mcp/call", map[string]any{
		"tool":      tool,
		"arguments": arguments,
		"lifecycle": lifecycle,
	})
	if pluginErr != nil {
		t.Fatalf("mcp/call returned protocol error: %+v", pluginErr)
	}
	return result
}

func toolText(t *testing.T, result map[string]any) string {
	t.Helper()
	content, ok := result["content"].([]any)
	if !ok || len(content) == 0 {
		t.Fatalf("tool result has no content: %v", result)
	}
	part, _ := content[0].(map[string]any)
	text, _ := part["text"].(string)
	return text
}

func toolIsError(result map[string]any) bool {
	isError, _ := result["isError"].(bool)
	return isError
}

// --- tests ------------------------------------------------------------------

func TestToolCatalogue(t *testing.T) {
	p := newTestPlugin()
	result, pluginErr := callHandle(t, p, "mcp/tools", map[string]any{"connectionId": "conn-1"})
	if pluginErr != nil {
		t.Fatalf("mcp/tools failed: %+v", pluginErr)
	}
	tools, ok := result["tools"].([]any)
	if !ok || len(tools) != 7 {
		t.Fatalf("expected 7 tools, got %v", result["tools"])
	}
	names := map[string]bool{}
	for _, entry := range tools {
		tool := entry.(map[string]any)
		names[tool["name"].(string)] = true
		annotations, _ := tool["annotations"].(map[string]any)
		if annotations == nil || annotations["readOnlyHint"] != true {
			t.Errorf("tool %v must declare readOnlyHint: true", tool["name"])
		}
		schema, _ := tool["inputSchema"].(map[string]any)
		props, _ := schema["properties"].(map[string]any)
		if _, ok := props["connectionId"]; !ok {
			t.Errorf("tool %v schema must declare connectionId", tool["name"])
		}
	}
	for _, want := range []string{"get_config", "search_configs", "diff_config", "get_config_history", "list_namespaces", "list_services", "list_instances"} {
		if !names[want] {
			t.Errorf("missing tool %s", want)
		}
	}
}

func TestConnectionLifecycle(t *testing.T) {
	server := newMockNacos(t)
	p := newTestPlugin()
	connection := connectionPayload(t, server)

	result, pluginErr := callHandle(t, p, "connection/test", map[string]any{"connection": connection})
	if pluginErr != nil {
		t.Fatalf("connection/test failed: %+v", pluginErr)
	}
	if result["success"] != true {
		t.Fatalf("connection/test not successful: %v", result)
	}

	if _, pluginErr := callHandle(t, p, "connection/connect", map[string]any{"connection": connection}); pluginErr != nil {
		t.Fatalf("connection/connect failed: %+v", pluginErr)
	}

	ping, pluginErr := callHandle(t, p, "nacos/ping", map[string]any{"connectionId": "conn-1"})
	if pluginErr != nil {
		t.Fatalf("nacos/ping failed: %+v", pluginErr)
	}
	if ping["ok"] != true {
		t.Fatalf("ping not ok: %v", ping)
	}

	if _, pluginErr := callHandle(t, p, "connection/disconnect", map[string]any{"connection": connection}); pluginErr != nil {
		t.Fatalf("connection/disconnect failed: %+v", pluginErr)
	}
	if _, pluginErr := callHandle(t, p, "nacos/ping", map[string]any{"connectionId": "conn-1"}); pluginErr == nil {
		t.Fatal("ping after disconnect should fail")
	}
}

func TestGetConfigTool(t *testing.T) {
	server := newMockNacos(t)
	p := newTestPlugin()
	result := callToolRaw(t, p, "get_config", map[string]any{"dataId": "gateway.yaml"}, connectionPayload(t, server))
	if toolIsError(result) {
		t.Fatalf("unexpected tool error: %s", toolText(t, result))
	}
	text := toolText(t, result)
	if !strings.Contains(text, "server:") || !strings.Contains(text, "port: 9000") {
		t.Fatalf("config content missing: %s", text)
	}
	if !strings.Contains(text, `"namespace": "dev"`) {
		t.Fatalf("namespace not reported: %s", text)
	}
}

func TestGetConfigNotFound(t *testing.T) {
	server := newMockNacos(t)
	p := newTestPlugin()
	result := callToolRaw(t, p, "get_config", map[string]any{"dataId": "nope.yaml"}, connectionPayload(t, server))
	if !toolIsError(result) {
		t.Fatalf("missing config should be an error result: %v", result)
	}
	if !strings.Contains(toolText(t, result), "config not found") {
		t.Fatalf("unexpected error text: %s", toolText(t, result))
	}
}

func TestSearchConfigsTool(t *testing.T) {
	server := newMockNacos(t)
	p := newTestPlugin()
	result := callToolRaw(t, p, "search_configs", map[string]any{"dataId": "yaml"}, connectionPayload(t, server))
	if toolIsError(result) {
		t.Fatalf("unexpected tool error: %s", toolText(t, result))
	}
	text := toolText(t, result)
	if !strings.Contains(text, "gateway.yaml") || !strings.Contains(text, "orders.yaml") {
		t.Fatalf("expected config items in result: %s", text)
	}
	if strings.Contains(text, "big payload") {
		t.Fatalf("search results must not include config content: %s", text)
	}
}

func TestListNamespacesTool(t *testing.T) {
	server := newMockNacos(t)
	p := newTestPlugin()
	result := callToolRaw(t, p, "list_namespaces", map[string]any{}, connectionPayload(t, server))
	if toolIsError(result) {
		t.Fatalf("unexpected tool error: %s", toolText(t, result))
	}
	text := toolText(t, result)
	if !strings.Contains(text, `"showName": "dev"`) || !strings.Contains(text, `"configCount": 2`) {
		t.Fatalf("namespace entries missing: %s", text)
	}
}

func TestListServicesTool(t *testing.T) {
	server := newMockNacos(t)
	p := newTestPlugin()
	result := callToolRaw(t, p, "list_services", map[string]any{}, connectionPayload(t, server))
	if toolIsError(result) {
		t.Fatalf("unexpected tool error: %s", toolText(t, result))
	}
	text := toolText(t, result)
	if !strings.Contains(text, "order-service") || !strings.Contains(text, "gateway-service") {
		t.Fatalf("service names missing: %s", text)
	}
}

func TestDiffConfigTool(t *testing.T) {
	// dev 与 prod 存在内容差异，empty-ns 下配置缺失。
	server := newMockNacos(t)
	p := newTestPlugin()
	result := callToolRaw(t, p, "diff_config", map[string]any{
		"dataId":     "gateway.yaml",
		"namespaces": []any{"dev", "prod", "empty-ns"},
	}, connectionPayload(t, server))
	if toolIsError(result) {
		t.Fatalf("unexpected tool error: %s", toolText(t, result))
	}
	text := toolText(t, result)
	if !strings.Contains(text, "\"status\": \"different\"") {
		t.Fatalf("dev vs prod 应为 different: %s", text)
	}
	if !strings.Contains(text, "+   port: 8080") || !strings.Contains(text, "-   port: 9000") {
		t.Fatalf("diff 内容不符合预期: %s", text)
	}
	if !strings.Contains(text, "+   - id: user") {
		t.Fatalf("prod 多出的 routes 行未体现: %s", text)
	}
	if !strings.Contains(text, "\"status\": \"missing-in-target\"") {
		t.Fatalf("empty-ns 应为 missing-in-target: %s", text)
	}
}

func TestDiffConfigSameContent(t *testing.T) {
	// dev 与 dev2 在 mock 中返回相同内容（都走默认分支）。
	server := newMockNacos(t)
	p := newTestPlugin()
	result := callToolRaw(t, p, "diff_config", map[string]any{
		"dataId":     "gateway.yaml",
		"namespaces": []any{"dev", "dev2"},
	}, connectionPayload(t, server))
	if toolIsError(result) {
		t.Fatalf("unexpected tool error: %s", toolText(t, result))
	}
	if !strings.Contains(toolText(t, result), "\"status\": \"identical\"") {
		t.Fatalf("相同内容应为 identical: %s", toolText(t, result))
	}
}

func TestDiffConfigRequiresTwoNamespaces(t *testing.T) {
	server := newMockNacos(t)
	p := newTestPlugin()
	result := callToolRaw(t, p, "diff_config", map[string]any{
		"dataId":     "gateway.yaml",
		"namespaces": []any{"dev"},
	}, connectionPayload(t, server))
	if !toolIsError(result) {
		t.Fatalf("单个命名空间应报错: %v", result)
	}
	if !strings.Contains(toolText(t, result), "at least two") {
		t.Fatalf("错误提示不符合预期: %s", toolText(t, result))
	}
}

func TestDiffConfigMissingDataId(t *testing.T) {
	server := newMockNacos(t)
	p := newTestPlugin()
	result := callToolRaw(t, p, "diff_config", map[string]any{
		"namespaces": []any{"dev", "prod"},
	}, connectionPayload(t, server))
	if !toolIsError(result) || !strings.Contains(toolText(t, result), "dataId is required") {
		t.Fatalf("缺少 dataId 应报错: %v", result)
	}
}

func TestListInstancesTool(t *testing.T) {
	server := newMockNacos(t)
	p := newTestPlugin()
	result := callToolRaw(t, p, "list_instances", map[string]any{
		"serviceName": "order-service",
	}, connectionPayload(t, server))
	if toolIsError(result) {
		t.Fatalf("unexpected tool error: %s", toolText(t, result))
	}
	text := toolText(t, result)
	if !strings.Contains(text, "\"instanceCount\": 2") || !strings.Contains(text, "\"healthyCount\": 1") {
		t.Fatalf("实例统计不符合预期: %s", text)
	}
	if !strings.Contains(text, "10.0.0.1") || !strings.Contains(text, "10.0.0.2") {
		t.Fatalf("实例地址缺失: %s", text)
	}
	if !strings.Contains(text, "\"healthy\": false") {
		t.Fatalf("不健康实例未标注: %s", text)
	}
	if !strings.Contains(text, "\"zone\": \"az1\"") {
		t.Fatalf("实例 metadata 缺失: %s", text)
	}
}

func TestListInstancesNotFound(t *testing.T) {
	server := newMockNacos(t)
	p := newTestPlugin()
	result := callToolRaw(t, p, "list_instances", map[string]any{
		"serviceName": "no-such-service",
	}, connectionPayload(t, server))
	if !toolIsError(result) {
		t.Fatalf("不存在的服务应返回错误结果: %v", result)
	}
	if !strings.Contains(toolText(t, result), "404") {
		t.Fatalf("错误信息不符合预期: %s", toolText(t, result))
	}
}

func TestListInstancesRequiresServiceName(t *testing.T) {
	server := newMockNacos(t)
	p := newTestPlugin()
	result := callToolRaw(t, p, "list_instances", map[string]any{}, connectionPayload(t, server))
	if !toolIsError(result) || !strings.Contains(toolText(t, result), "serviceName is required") {
		t.Fatalf("缺少 serviceName 应报错: %v", result)
	}
}

func TestGetConfigHistoryList(t *testing.T) {
	server := newMockNacos(t)
	p := newTestPlugin()
	result := callToolRaw(t, p, "get_config_history", map[string]any{
		"dataId": "gateway.yaml",
	}, connectionPayload(t, server))
	if toolIsError(result) {
		t.Fatalf("unexpected tool error: %s", toolText(t, result))
	}
	text := toolText(t, result)
	if !strings.Contains(text, "\"totalCount\": 2") {
		t.Fatalf("历史总数不符合预期: %s", text)
	}
	if !strings.Contains(text, "\"historyId\": \"1001\"") || !strings.Contains(text, "\"operator\": \"alice\"") {
		t.Fatalf("最新历史记录字段缺失: %s", text)
	}
	if !strings.Contains(text, "\"opType\": \"U\"") || !strings.Contains(text, "\"opType\": \"I\"") {
		t.Fatalf("操作类型未体现: %s", text)
	}
	// 2.4.x 形态：createdTime 是占位值，展示时间应取 lastModifiedTime；
	// opType 经 TrimSpace 去掉定长字段的尾部空格（上面 "U"/"I" 断言已验证）。
	if !strings.Contains(text, "2026-10-17T00:15:18") {
		t.Fatalf("应展示 lastModifiedTime 而非占位 createdTime: %s", text)
	}
	// 旧版形态：毫秒数字时间戳应原样保留为 createdTimeMillis。
	if !strings.Contains(text, "\"createdTimeMillis\": 1759000000000") {
		t.Fatalf("旧版毫秒时间戳应保留: %s", text)
	}
	// 列表模式不应携带配置正文。
	if strings.Contains(text, "port: 8999") {
		t.Fatalf("列表模式不应返回 content: %s", text)
	}
}

func TestGetConfigHistoryRevision(t *testing.T) {
	server := newMockNacos(t)
	p := newTestPlugin()
	result := callToolRaw(t, p, "get_config_history", map[string]any{
		"dataId":    "gateway.yaml",
		"historyId": "1001",
	}, connectionPayload(t, server))
	if toolIsError(result) {
		t.Fatalf("unexpected tool error: %s", toolText(t, result))
	}
	text := toolText(t, result)
	if !strings.Contains(text, "port: 8999") {
		t.Fatalf("详情模式应返回该版本内容: %s", text)
	}
	if !strings.Contains(text, "\"historyId\": \"1001\"") || !strings.Contains(text, "\"opType\": \"U\"") {
		t.Fatalf("详情元数据缺失: %s", text)
	}
	if !strings.Contains(text, "\"createdTime\": \"") {
		t.Fatalf("详情时间字段缺失: %s", text)
	}
	// 2.4.x 形态：详情时间应取 lastModifiedTime。
	if !strings.Contains(text, "2026-10-17T00:15:18") {
		t.Fatalf("详情应展示 lastModifiedTime: %s", text)
	}
}

func TestFlexibleTimeUnmarshal(t *testing.T) {
	cases := []struct {
		raw  string
		want int64
	}{
		{`1760000000000`, 1760000000000},                   // 旧版：毫秒数字
		{`"1760000000000"`, 1760000000000},                 // 字符串形态的毫秒数字
		{`"2026-10-17T00:15:18.031+08:00"`, 1792167318031}, // 2.x：ISO8601（带毫秒）
		{`"2026-10-17T00:15:18+08:00"`, 1792167318000},     // 2.x：ISO8601（无毫秒）
		{`null`, 0},         // 缺省
		{`""`, 0},           // 空串
		{`"not-a-time"`, 0}, // 脏数据按无效处理，不使整体解析失败
	}
	for _, tc := range cases {
		var parsed flexibleTime
		if err := json.Unmarshal([]byte(tc.raw), &parsed); err != nil {
			t.Fatalf("解析 %s 不应报错: %v", tc.raw, err)
		}
		if int64(parsed) != tc.want {
			t.Errorf("解析 %s = %d, 期望 %d", tc.raw, int64(parsed), tc.want)
		}
	}
}

func TestFlexibleStringUnmarshal(t *testing.T) {
	cases := []struct {
		raw  string
		want string
	}{
		{`"1001"`, "1001"}, // 2.x：ToStringSerializer
		{`1001`, "1001"},   // 旧版：数字 id
		{`null`, ""},
	}
	for _, tc := range cases {
		var parsed flexibleString
		if err := json.Unmarshal([]byte(tc.raw), &parsed); err != nil {
			t.Fatalf("解析 %s 不应报错: %v", tc.raw, err)
		}
		if string(parsed) != tc.want {
			t.Errorf("解析 %s = %q, 期望 %q", tc.raw, string(parsed), tc.want)
		}
	}
}

func TestGetConfigHistoryRevisionNotFound(t *testing.T) {
	server := newMockNacos(t)
	p := newTestPlugin()
	result := callToolRaw(t, p, "get_config_history", map[string]any{
		"dataId":    "gateway.yaml",
		"historyId": "9999",
	}, connectionPayload(t, server))
	if !toolIsError(result) {
		t.Fatalf("不存在的历史版本应返回错误结果: %v", result)
	}
	if !strings.Contains(toolText(t, result), "config revision not found") {
		t.Fatalf("错误信息不符合预期: %s", toolText(t, result))
	}
}

func TestGetConfigHistoryRequiresDataId(t *testing.T) {
	server := newMockNacos(t)
	p := newTestPlugin()
	result := callToolRaw(t, p, "get_config_history", map[string]any{}, connectionPayload(t, server))
	if !toolIsError(result) || !strings.Contains(toolText(t, result), "dataId is required") {
		t.Fatalf("缺少 dataId 应报错: %v", result)
	}
}

func TestFormatMillis(t *testing.T) {
	if got := formatMillis(0); got != "" {
		t.Fatalf("0 应返回空串，得到 %q", got)
	}
	got := formatMillis(1760000000000)
	if !strings.Contains(got, "T") || len(got) < 19 {
		t.Fatalf("时间格式不是 RFC3339: %q", got)
	}
}

func TestWrongCredentials(t *testing.T) {
	server := newMockNacos(t)
	p := newTestPlugin()
	connection := connectionPayload(t, server)
	connection["password"] = "wrong"
	result := callToolRaw(t, p, "get_config", map[string]any{"dataId": "gateway.yaml"}, connection)
	if !toolIsError(result) {
		t.Fatalf("wrong credentials should produce an error result: %v", result)
	}
	if !strings.Contains(toolText(t, result), "wrong username or password") {
		t.Fatalf("unexpected error text: %s", toolText(t, result))
	}
}

func TestAnonymousConnection(t *testing.T) {
	// No credentials configured: the mock rejects every data call without a
	// token, which must surface as a readable tool error (not a crash).
	server := newMockNacos(t)
	p := newTestPlugin()
	connection := connectionPayload(t, server)
	delete(connection, "username")
	delete(connection, "password")
	result := callToolRaw(t, p, "get_config", map[string]any{"dataId": "gateway.yaml"}, connection)
	if !toolIsError(result) {
		t.Fatalf("rejected anonymous read should be an error result: %v", result)
	}
}

func TestNoBoundConnection(t *testing.T) {
	p := newTestPlugin()
	result := callToolRaw(t, p, "get_config", map[string]any{"dataId": "x.yaml"}, nil)
	if !toolIsError(result) {
		t.Fatalf("calls without a bound connection must fail: %v", result)
	}
	if !strings.Contains(toolText(t, result), "connection") {
		t.Fatalf("error should mention the missing connection: %s", toolText(t, result))
	}
}

func TestUnknownTool(t *testing.T) {
	server := newMockNacos(t)
	p := newTestPlugin()
	result := callToolRaw(t, p, "drop_database", map[string]any{}, connectionPayload(t, server))
	if !toolIsError(result) {
		t.Fatalf("unknown tool must be an error result: %v", result)
	}
	if !strings.Contains(toolText(t, result), "unknown tool") {
		t.Fatalf("unexpected error text: %s", toolText(t, result))
	}
}

func TestUnreachableServer(t *testing.T) {
	// Reserve a port and close it so the connection is refused.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()

	p := newTestPlugin()
	connection := map[string]any{
		"id":              "conn-dead",
		"name":            "Nacos dead",
		"host":            "127.0.0.1",
		"port":            float64(port),
		"external_config": map[string]any{"context_path": "/nacos", "api_version": "v1"},
	}
	result := callToolRaw(t, p, "get_config", map[string]any{"dataId": "x.yaml"}, connection)
	if !toolIsError(result) {
		t.Fatalf("unreachable server must be an error result: %v", result)
	}
}

func TestSearchConfigsOmitsContentFromUpstream(t *testing.T) {
	// Regression guard: even if the upstream echoes content in pageItems,
	// the decoded struct drops it, so the tool output stays metadata-only.
	server := newMockNacos(t)
	p := newTestPlugin()
	result := callToolRaw(t, p, "search_configs", map[string]any{}, connectionPayload(t, server))
	if strings.Contains(toolText(t, result), "more payload") {
		t.Fatalf("content leaked into search result: %s", toolText(t, result))
	}
}

func TestToolCatalogueRemoteNames(t *testing.T) {
	// Sanity: tool names are plain identifiers; the host adds the prefixes.
	p := newTestPlugin()
	result, _ := callHandle(t, p, "mcp/tools", map[string]any{})
	tools := result["tools"].([]any)
	for _, entry := range tools {
		name := entry.(map[string]any)["name"].(string)
		if name != strings.ToLower(name) || strings.ContainsAny(name, "-. ") {
			t.Errorf("tool name %q must be lowercase snake_case", name)
		}
	}
}

// --- v3 protocol stack (Nacos 3.x) ------------------------------------------

// newMockNacosV3 starts a pair of fake servers modelling a pure Nacos 3.x
// deployment (no legacy adapter):
//
//	server:  client API + login, context path /nacos
//	console: console API on its own port, no context path, Bearer required
//	         except for the anonymous health probe
//
// 响应形状逐字对齐 3.2.4 真实实例的实测输出。
func newMockNacosV3(t *testing.T) (server *httptest.Server, console *httptest.Server, lastAuth, lastSearch *atomic.Value) {
	t.Helper()
	lastAuth = &atomic.Value{}
	lastAuth.Store("")
	lastSearch = &atomic.Value{}
	lastSearch.Store("")

	serverMux := http.NewServeMux()
	serverMux.HandleFunc("/nacos/v3/auth/user/login", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		if r.FormValue("username") != "nacos" || r.FormValue("password") != "nacos" {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte("User not found! Please check user exist or password is right!"))
			return
		}
		_, _ = w.Write([]byte(`{"accessToken":"v3-token","tokenTtl":18000,"globalAdmin":true,"username":"nacos"}`))
	})
	serverMux.HandleFunc("/nacos/v3/client/cs/config", func(w http.ResponseWriter, r *http.Request) {
		lastAuth.Store(r.Header.Get("Authorization"))
		query := r.URL.Query()
		switch {
		case query.Get("dataId") == "gateway.yaml" && query.Get("namespaceId") == "prod":
			_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"content":"server:\n  port: 8080\nroutes:\n  - id: order\n  - id: user\n","md5":"def"}}`))
		case query.Get("dataId") == "gateway.yaml":
			_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"content":"server:\n  port: 9000\nroutes:\n  - id: order\n","md5":"abc"}}`))
		default:
			_, _ = w.Write([]byte(`{"code":20004,"message":"resource not found","data":null}`))
		}
	})
	serverMux.HandleFunc("/nacos/v3/client/ns/instance/list", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("serviceName") != "order-service" {
			_, _ = w.Write([]byte(`{"code":0,"message":"success","data":[]}`))
			return
		}
		// v3 的 Instance 对象没有 valid 字段。
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":[
			{"ip":"10.0.0.1","port":8080,"weight":1,"healthy":true,"enabled":true,"ephemeral":true,"clusterName":"DEFAULT","serviceName":"DEFAULT_GROUP@@order-service","metadata":{"zone":"az1"}},
			{"ip":"10.0.0.2","port":8080,"weight":0.5,"healthy":false,"enabled":true,"ephemeral":true,"clusterName":"DEFAULT","serviceName":"DEFAULT_GROUP@@order-service","metadata":{}}
		]}`))
	})
	server = httptest.NewServer(serverMux)
	t.Cleanup(server.Close)

	consoleAuth := func(w http.ResponseWriter, r *http.Request) bool {
		if r.Header.Get("Authorization") != "Bearer v3-token" {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"code":10001,"message":"access denied","data":"Code: 401, Message: User not found!"}`))
			return false
		}
		return true
	}
	consoleMux := http.NewServeMux()
	consoleMux.HandleFunc("/v3/console/health/readiness", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":"ok"}`))
	})
	consoleMux.HandleFunc("/v3/console/cs/config/list", func(w http.ResponseWriter, r *http.Request) {
		if !consoleAuth(w, r) {
			return
		}
		lastSearch.Store(r.URL.Query().Get("dataId"))
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"totalCount":2,"pageNumber":1,"pagesAvailable":1,"pageItems":[
			{"id":"1","namespaceId":"dev","groupName":"DEFAULT_GROUP","dataId":"gateway.yaml","md5":"aaa","type":"yaml","appName":""},
			{"id":"2","namespaceId":"dev","groupName":"DEFAULT_GROUP","dataId":"orders.yaml","md5":"bbb","type":"yaml","appName":""}
		]}}`))
	})
	consoleMux.HandleFunc("/v3/console/cs/history/list", func(w http.ResponseWriter, r *http.Request) {
		if !consoleAuth(w, r) {
			return
		}
		// 真实形态：createTime 为 derby 占位值（2010），modifyTime 才是真实时间；
		// opType 带定长尾部空格；md5 为 null。
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"totalCount":2,"pageNumber":1,"pagesAvailable":1,"pageItems":[
			{"id":"180","namespaceId":"dev","groupName":"DEFAULT_GROUP","dataId":"gateway.yaml","md5":null,"type":null,"appName":"","createTime":1272988800000,"modifyTime":1792167318031,"srcIp":"10.1.1.1","srcUser":"alice","opType":"U         ","publishType":"formal"},
			{"id":"179","namespaceId":"dev","groupName":"DEFAULT_GROUP","dataId":"gateway.yaml","md5":null,"type":null,"appName":"","createTime":1272988800000,"modifyTime":1759000000000,"srcIp":"10.1.1.2","srcUser":"bob","opType":"I         ","publishType":"formal"}
		]}}`))
	})
	consoleMux.HandleFunc("/v3/console/cs/history", func(w http.ResponseWriter, r *http.Request) {
		if !consoleAuth(w, r) {
			return
		}
		if r.URL.Query().Get("nid") != "180" {
			_, _ = w.Write([]byte(`{"code":20004,"message":"resource not found","data":null}`))
			return
		}
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"id":"180","namespaceId":"dev","groupName":"DEFAULT_GROUP","dataId":"gateway.yaml","md5":"aba","type":null,"appName":"","createTime":1272988800000,"modifyTime":1792167318031,"srcIp":"10.1.1.1","srcUser":"alice","opType":"U         ","publishType":"formal","content":"server:\n  port: 8999\n"}}`))
	})
	consoleMux.HandleFunc("/v3/console/core/namespace/list", func(w http.ResponseWriter, r *http.Request) {
		if !consoleAuth(w, r) {
			return
		}
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":[
			{"namespace":"public","namespaceShowName":"public","configCount":1,"quota":200,"type":0},
			{"namespace":"dev","namespaceShowName":"dev","configCount":2,"quota":200,"type":2}
		]}`))
	})
	consoleMux.HandleFunc("/v3/console/ns/service/list", func(w http.ResponseWriter, r *http.Request) {
		if !consoleAuth(w, r) {
			return
		}
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"totalCount":2,"pageNumber":1,"pagesAvailable":1,"pageItems":[
			{"name":"order-service","groupName":"DEFAULT_GROUP"},
			{"name":"gateway-service","groupName":"DEFAULT_GROUP"}
		]}}`))
	})
	console = httptest.NewServer(consoleMux)
	t.Cleanup(console.Close)
	return server, console, lastAuth, lastSearch
}

// v3ConnectionPayload 构造指向 v3 mock 的连接载荷。不设置 api_version，
// 让用例真实走一遍自动探测（控制台健康端点命中 -> v3）。
func v3ConnectionPayload(t *testing.T, server, console *httptest.Server) map[string]any {
	t.Helper()
	host, port := mockHostPort(t, server.URL)
	_, consolePort := mockHostPort(t, console.URL)
	return map[string]any{
		"id":       "conn-v3",
		"name":     "Nacos v3",
		"host":     host,
		"port":     float64(port),
		"username": "nacos",
		"password": "nacos",
		"external_config": map[string]any{
			"context_path": "/nacos",
			"namespace":    "dev",
			"console_port": float64(consolePort),
		},
	}
}

// deadPort 返回一个刚被释放的端口，用于模拟不可达目标。
func deadPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	return port
}

// 自动探测命中 v3，并且 client 接口带上 Bearer 头。
func TestV3AutoDetectionAndGetConfig(t *testing.T) {
	server, console, lastAuth, _ := newMockNacosV3(t)
	p := newTestPlugin()
	result := callToolRaw(t, p, "get_config", map[string]any{"dataId": "gateway.yaml"}, v3ConnectionPayload(t, server, console))
	if toolIsError(result) {
		t.Fatalf("unexpected tool error: %s", toolText(t, result))
	}
	text := toolText(t, result)
	if !strings.Contains(text, "port: 9000") || !strings.Contains(text, `"namespace": "dev"`) {
		t.Fatalf("v3 config content missing: %s", text)
	}
	if got := lastAuth.Load(); got != "Bearer v3-token" {
		t.Fatalf("v3 client call must attach a Bearer token, got %q", got)
	}
}

func TestV3GetConfigNotFound(t *testing.T) {
	server, console, _, _ := newMockNacosV3(t)
	p := newTestPlugin()
	result := callToolRaw(t, p, "get_config", map[string]any{"dataId": "nope.yaml"}, v3ConnectionPayload(t, server, console))
	if !toolIsError(result) || !strings.Contains(toolText(t, result), "config not found") {
		t.Fatalf("code 20004 应映射为 config not found: %v", result)
	}
}

func TestV3SearchConfigs(t *testing.T) {
	server, console, _, lastSearch := newMockNacosV3(t)
	p := newTestPlugin()
	result := callToolRaw(t, p, "search_configs", map[string]any{"dataId": "yaml"}, v3ConnectionPayload(t, server, console))
	if toolIsError(result) {
		t.Fatalf("unexpected tool error: %s", toolText(t, result))
	}
	text := toolText(t, result)
	if !strings.Contains(text, "gateway.yaml") || !strings.Contains(text, "orders.yaml") {
		t.Fatalf("expected config items in result: %s", text)
	}
	if strings.Contains(text, "big payload") {
		t.Fatalf("search results must not include config content: %s", text)
	}
	// 关键词应转换为 *kw* 的 blur 模式（v3 的 blur 搜索不会自动补通配符）。
	if got := lastSearch.Load(); got != "*yaml*" {
		t.Fatalf("v3 blur search must wrap the keyword, upstream saw %q", got)
	}
}

func TestV3ListNamespaces(t *testing.T) {
	server, console, _, _ := newMockNacosV3(t)
	p := newTestPlugin()
	result := callToolRaw(t, p, "list_namespaces", map[string]any{}, v3ConnectionPayload(t, server, console))
	if toolIsError(result) {
		t.Fatalf("unexpected tool error: %s", toolText(t, result))
	}
	text := toolText(t, result)
	if !strings.Contains(text, `"showName": "dev"`) || !strings.Contains(text, `"configCount": 2`) {
		t.Fatalf("namespace entries missing: %s", text)
	}
}

func TestV3ListServices(t *testing.T) {
	server, console, _, _ := newMockNacosV3(t)
	p := newTestPlugin()
	result := callToolRaw(t, p, "list_services", map[string]any{}, v3ConnectionPayload(t, server, console))
	if toolIsError(result) {
		t.Fatalf("unexpected tool error: %s", toolText(t, result))
	}
	text := toolText(t, result)
	if !strings.Contains(text, "order-service") || !strings.Contains(text, "gateway-service") {
		t.Fatalf("service names missing: %s", text)
	}
}

func TestV3ListInstances(t *testing.T) {
	server, console, _, _ := newMockNacosV3(t)
	p := newTestPlugin()
	result := callToolRaw(t, p, "list_instances", map[string]any{
		"serviceName": "order-service",
	}, v3ConnectionPayload(t, server, console))
	if toolIsError(result) {
		t.Fatalf("unexpected tool error: %s", toolText(t, result))
	}
	text := toolText(t, result)
	if !strings.Contains(text, `"instanceCount": 2`) || !strings.Contains(text, `"healthyCount": 1`) {
		t.Fatalf("实例统计不符合预期: %s", text)
	}
	// v3 响应没有 valid 字段：工具输出必须省略而不是伪造 false。
	if strings.Contains(text, `"valid"`) {
		t.Fatalf("v3 instance output must omit the valid field: %s", text)
	}
	if !strings.Contains(text, `"zone": "az1"`) {
		t.Fatalf("实例 metadata 缺失: %s", text)
	}
}

func TestV3History(t *testing.T) {
	server, console, _, _ := newMockNacosV3(t)
	p := newTestPlugin()
	result := callToolRaw(t, p, "get_config_history", map[string]any{
		"dataId": "gateway.yaml",
	}, v3ConnectionPayload(t, server, console))
	if toolIsError(result) {
		t.Fatalf("unexpected tool error: %s", toolText(t, result))
	}
	text := toolText(t, result)
	if !strings.Contains(text, `"totalCount": 2`) || !strings.Contains(text, `"historyId": "180"`) {
		t.Fatalf("历史列表字段缺失: %s", text)
	}
	if !strings.Contains(text, `"opType": "U"`) {
		t.Fatalf("opType 应清除定长空格: %s", text)
	}
	// derby 占位 createTime（2010）绝不能出现在展示时间中；应取 modifyTime。
	if strings.Contains(text, "2010") || !strings.Contains(text, "2026-10-17T00:15:18") {
		t.Fatalf("展示时间应取 modifyTime: %s", text)
	}
	// 详情：nid=180 命中，返回内容。
	result = callToolRaw(t, p, "get_config_history", map[string]any{
		"dataId": "gateway.yaml", "historyId": "180",
	}, v3ConnectionPayload(t, server, console))
	if toolIsError(result) || !strings.Contains(toolText(t, result), "port: 8999") {
		t.Fatalf("v3 历史详情应返回内容: %v", result)
	}
	// 不存在的 nid -> 资源不存在错误。
	result = callToolRaw(t, p, "get_config_history", map[string]any{
		"dataId": "gateway.yaml", "historyId": "999",
	}, v3ConnectionPayload(t, server, console))
	if !toolIsError(result) || !strings.Contains(toolText(t, result), "not found") {
		t.Fatalf("不存在的历史版本应报错: %v", result)
	}
}

// v3 + 无凭据：console 接口的 403 必须翻译成可操作的错误提示。
func TestV3ConsoleRequiresCredentials(t *testing.T) {
	server, console, _, _ := newMockNacosV3(t)
	p := newTestPlugin()
	connection := v3ConnectionPayload(t, server, console)
	delete(connection, "username")
	delete(connection, "password")
	result := callToolRaw(t, p, "search_configs", map[string]any{}, connection)
	if !toolIsError(result) {
		t.Fatalf("匿名 console 调用应报错: %v", result)
	}
	text := toolText(t, result)
	if !strings.Contains(text, "access denied") || !strings.Contains(text, "403") {
		t.Fatalf("错误提示应说明鉴权要求: %s", text)
	}
}

// v3 + 错误密码：登录拒绝信息应包含可读原因。
func TestV3WrongCredentials(t *testing.T) {
	server, console, _, _ := newMockNacosV3(t)
	p := newTestPlugin()
	connection := v3ConnectionPayload(t, server, console)
	connection["password"] = "wrong"
	result := callToolRaw(t, p, "search_configs", map[string]any{}, connection)
	if !toolIsError(result) || !strings.Contains(toolText(t, result), "wrong username or password") {
		t.Fatalf("错误密码应返回登录拒绝提示: %v", result)
	}
}

// 自动探测在 v3 探针失败时回退 v1（覆盖 Nacos 2.x 与无 console 端口配置的场景）。
func TestAutoDetectionFallbackToV1(t *testing.T) {
	server := newMockNacos(t)
	host, port := mockHostPort(t, server.URL)
	connection := map[string]any{
		"id": "conn-auto-v1", "name": "auto",
		"host": host, "port": float64(port),
		"username": "nacos", "password": "nacos",
		"external_config": map[string]any{
			"context_path": "/nacos",
			"namespace":    "dev",
			"console_port": float64(deadPort(t)),
		},
	}
	p := newTestPlugin()
	if _, pluginErr := callHandle(t, p, "connection/connect", map[string]any{"connection": connection}); pluginErr != nil {
		t.Fatalf("connect: %+v", pluginErr)
	}
	result := callToolRaw(t, p, "get_config", map[string]any{"dataId": "gateway.yaml"}, connection)
	if toolIsError(result) {
		t.Fatalf("v1 回退失败: %s", toolText(t, result))
	}
	ping, pluginErr := callHandle(t, p, "nacos/ping", map[string]any{"connectionId": "conn-auto-v1"})
	if pluginErr != nil {
		t.Fatalf("ping: %+v", pluginErr)
	}
	if ping["api"] != "v1" {
		t.Fatalf("探测结果应为 v1，得到 %v", ping["api"])
	}
}

// 双探针都失败时，错误信息应引导用户检查 console 端口。
func TestDetectionErrorHintsConsolePort(t *testing.T) {
	p := newTestPlugin()
	connection := map[string]any{
		"id": "conn-nowhere", "name": "nowhere",
		"host": "127.0.0.1", "port": float64(deadPort(t)),
		"external_config": map[string]any{"context_path": "/nacos"},
	}
	result := callToolRaw(t, p, "get_config", map[string]any{"dataId": "x.yaml"}, connection)
	if !toolIsError(result) || !strings.Contains(toolText(t, result), "console port") {
		t.Fatalf("探测失败应提示 console 端口: %v", result)
	}
}

// --- regressions: security & limits -----------------------------------------

// A transport failure must not carry the accessToken into the message that the
// AI model and the UI see.
func TestTransportErrorDoesNotLeakAccessToken(t *testing.T) {
	const secret = "SUPER-SECRET-TOKEN"
	mux := http.NewServeMux()
	mux.HandleFunc("/nacos/v1/auth/login", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"accessToken":"` + secret + `","tokenTtl":18000}`))
	})
	mux.HandleFunc("/nacos/v1/cs/configs", func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(2 * time.Second) // force the client deadline to fire
		_, _ = w.Write([]byte("server:\n  port: 9000\n"))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	host, port := mockHostPort(t, server.URL)
	client, err := newNacosClient(clientOptions{
		host: host, port: port, contextPath: "/nacos",
		apiVersion: "v1", username: "nacos", password: "nacos",
	})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	client.http.Timeout = 250 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = client.getConfig(ctx, "dev", "gateway.yaml", "")
	if err == nil {
		t.Fatal("expected a transport error")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("access token leaked into the error message: %s", err.Error())
	}
	if !strings.Contains(err.Error(), "request to") {
		t.Fatalf("unexpected error text: %s", err.Error())
	}
}

// An oversized upstream response must fail loudly instead of handing the model
// a silently truncated config.
func TestOversizedConfigIsRejectedNotTruncated(t *testing.T) {
	big := strings.Repeat("x", int(maxResponseBytes)+10)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(big))
	}))
	defer server.Close()

	host, port := mockHostPort(t, server.URL)
	p := newTestPlugin()
	result := callToolRaw(t, p, "get_config", map[string]any{"dataId": "huge.yaml"}, map[string]any{
		"id": "conn-big", "host": host, "port": float64(port), "name": "Nacos big",
		"external_config": map[string]any{"api_version": "v1"},
	})

	if !toolIsError(result) {
		t.Fatalf("oversized config must be an error result: %v", result)
	}
	if !strings.Contains(toolText(t, result), "larger than") {
		t.Fatalf("error should explain the size limit: %s", toolText(t, result))
	}
}

// Upstream error bodies stay out of the tool result text.
func TestUpstreamErrorBodyIsNotEchoed(t *testing.T) {
	const internal = "/var/nacos/internal/path"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(internal))
	}))
	defer server.Close()

	host, port := mockHostPort(t, server.URL)
	p := newTestPlugin()
	result := callToolRaw(t, p, "get_config", map[string]any{"dataId": "x.yaml"}, map[string]any{
		"id": "conn-500", "host": host, "port": float64(port), "name": "Nacos 500",
		"external_config": map[string]any{"api_version": "v1"},
	})
	if strings.Contains(toolText(t, result), internal) {
		t.Fatalf("upstream body leaked into the tool result: %s", toolText(t, result))
	}
	if !strings.Contains(toolText(t, result), "HTTP 500") {
		t.Fatalf("status code should still be reported: %s", toolText(t, result))
	}
}

// --- regressions: performance & correctness ---------------------------------

func TestPageSizeIsClampedToMaximum(t *testing.T) {
	var seen atomic.Value
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen.Store(r.URL.Query().Get("pageSize"))
		_, _ = w.Write([]byte(`{"totalCount":0,"pageNumber":1,"pagesAvailable":0,"pageItems":[]}`))
	}))
	defer server.Close()

	host, port := mockHostPort(t, server.URL)
	p := newTestPlugin()
	result := callToolRaw(t, p, "search_configs", map[string]any{"pageSize": 999}, map[string]any{
		"id": "conn-page", "host": host, "port": float64(port), "name": "Nacos page",
		"external_config": map[string]any{"api_version": "v1"},
	})
	if toolIsError(result) {
		t.Fatalf("unexpected tool error: %s", toolText(t, result))
	}
	if got := seen.Load(); got != "500" {
		t.Fatalf("pageSize must clamp to 500, upstream saw %v", got)
	}
}

// Repeated tool calls on one connection must reuse the client instead of
// logging in to Nacos every time.
func TestToolCallsReuseCachedSession(t *testing.T) {
	var logins int64
	mux := http.NewServeMux()
	mux.HandleFunc("/nacos/v1/auth/login", func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt64(&logins, 1)
		_, _ = w.Write([]byte(`{"accessToken":"tok","tokenTtl":18000}`))
	})
	mux.HandleFunc("/nacos/v1/cs/configs", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("a: 1\n"))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	p := newTestPlugin()
	connection := connectionPayload(t, server)
	if _, err := callHandle(t, p, "connection/connect", map[string]any{"connection": connection}); err != nil {
		t.Fatalf("connect: %+v", err)
	}
	for index := 0; index < 3; index++ {
		result := callToolRaw(t, p, "get_config", map[string]any{"dataId": "x.yaml"}, connection)
		if toolIsError(result) {
			t.Fatalf("call %d failed: %s", index, toolText(t, result))
		}
	}
	if got := atomic.LoadInt64(&logins); got != 1 {
		t.Fatalf("3 tool calls must cost 1 login, got %d", got)
	}
}

// Disconnecting must drop the cached client, so the next call starts clean.
func TestDisconnectDropsCachedSession(t *testing.T) {
	var logins int64
	mux := http.NewServeMux()
	mux.HandleFunc("/nacos/v1/auth/login", func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt64(&logins, 1)
		_, _ = w.Write([]byte(`{"accessToken":"tok","tokenTtl":18000}`))
	})
	mux.HandleFunc("/nacos/v1/cs/configs", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("a: 1\n"))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	p := newTestPlugin()
	connection := connectionPayload(t, server)
	if _, err := callHandle(t, p, "connection/connect", map[string]any{"connection": connection}); err != nil {
		t.Fatalf("connect: %+v", err)
	}
	_ = callToolRaw(t, p, "get_config", map[string]any{"dataId": "x.yaml"}, connection)
	if _, err := callHandle(t, p, "connection/disconnect", map[string]any{"connection": connection}); err != nil {
		t.Fatalf("disconnect: %+v", err)
	}
	_ = callToolRaw(t, p, "get_config", map[string]any{"dataId": "x.yaml"}, connection)

	if got := atomic.LoadInt64(&logins); got != 2 {
		t.Fatalf("expected a fresh login after disconnect, got %d login(s)", got)
	}
}

// The address field accepts full URLs and pasted context paths; the port is
// appended only when the address does not already carry one.
func TestServerAddressParsing(t *testing.T) {
	cases := []struct {
		host        string
		port        int
		contextPath string
		want        string
	}{
		{"nacos.example.com", 8848, "", "http://nacos.example.com:8848/nacos"},
		{"nacos.example.com:8848", 8848, "", "http://nacos.example.com:8848/nacos"},
		{"https://nacos.example.com", 8848, "", "https://nacos.example.com:8848/nacos"},
		{"https://nacos.example.com/nacos", 8848, "", "https://nacos.example.com:8848/nacos"},
		{"https://nacos.example.com/nacos", 0, "", "https://nacos.example.com/nacos"},
		{"nacos.example.com/nacos", 8848, "", "http://nacos.example.com:8848/nacos"},
		{"http://nacos.example.com:8848/nacos", 8848, "", "http://nacos.example.com:8848/nacos"},
		{"nacos.example.com/nacos", 8848, "/custom", "http://nacos.example.com:8848/custom"},
		{"[::1]", 8848, "", "http://[::1]:8848/nacos"},
		{"[::1]:8848", 8848, "", "http://[::1]:8848/nacos"},
		{"10.0.0.1", 8848, "custom", "http://10.0.0.1:8848/custom"},
	}
	for _, item := range cases {
		client, err := newNacosClient(clientOptions{host: item.host, port: item.port, contextPath: item.contextPath})
		if err != nil {
			t.Errorf("%q: unexpected error: %v", item.host, err)
			continue
		}
		if client.serverBase != item.want {
			t.Errorf("%q: got %q want %q", item.host, client.serverBase, item.want)
		}
	}

	for _, bad := range []string{"", "ftp://nacos.example.com", "https://"} {
		if _, err := newNacosClient(clientOptions{host: bad, port: 8848}); err == nil {
			t.Errorf("%q should be rejected", bad)
		}
	}
}

// consoleBase 由 server 地址派生出同主机、独立端口、无 context path 的基址。
func TestConsoleBaseDerivation(t *testing.T) {
	cases := []struct {
		host        string
		port        int
		consolePort int
		want        string
	}{
		{"https://nacos.example.com/nacos", 8848, 18080, "https://nacos.example.com:18080"},
		{"nacos.example.com:8848", 8848, 0, "http://nacos.example.com:8080"}, // 未配置按默认 8080
		{"[::1]:8848", 8848, 9090, "http://[::1]:9090"},
	}
	for _, item := range cases {
		client, err := newNacosClient(clientOptions{host: item.host, port: item.port, consolePort: item.consolePort})
		if err != nil {
			t.Errorf("%q: unexpected error: %v", item.host, err)
			continue
		}
		if client.consoleBase != item.want {
			t.Errorf("%q: consoleBase got %q want %q", item.host, client.consoleBase, item.want)
		}
	}
}

// truncate must count runes, not bytes, or Chinese messages get mangled.
func TestTruncateIsRuneSafe(t *testing.T) {
	if got := truncate("配置文件内容", 2); got != "配置…" {
		t.Fatalf("got %q", got)
	}
	if got := truncate("short", 10); got != "short" {
		t.Fatalf("got %q", got)
	}
}

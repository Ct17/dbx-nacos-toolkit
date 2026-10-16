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
// injects it, pointing at the mock server.
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
	if !ok || len(tools) != 6 {
		t.Fatalf("expected 6 tools, got %v", result["tools"])
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
	for _, want := range []string{"get_config", "search_configs", "diff_config", "list_namespaces", "list_services", "list_instances"} {
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
		"external_config": map[string]any{"context_path": "/nacos"},
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
	client, err := newNacosClient(host, port, "/nacos", "nacos", "nacos")
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
	result := callToolRaw(t, p, "search_configs", map[string]any{"pageSize": 500}, map[string]any{
		"id": "conn-page", "host": host, "port": float64(port), "name": "Nacos page",
	})
	if toolIsError(result) {
		t.Fatalf("unexpected tool error: %s", toolText(t, result))
	}
	if got := seen.Load(); got != "200" {
		t.Fatalf("pageSize must clamp to 200, upstream saw %v", got)
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
		client, err := newNacosClient(item.host, item.port, item.contextPath, "", "")
		if err != nil {
			t.Errorf("%q: unexpected error: %v", item.host, err)
			continue
		}
		if client.baseURL != item.want {
			t.Errorf("%q: got %q want %q", item.host, client.baseURL, item.want)
		}
	}

	for _, bad := range []string{"", "ftp://nacos.example.com", "https://"} {
		if _, err := newNacosClient(bad, 8848, "", "", ""); err == nil {
			t.Errorf("%q should be rejected", bad)
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

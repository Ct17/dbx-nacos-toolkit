package main

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

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
		switch query.Get("dataId") {
		case "gateway.yaml":
			_, _ = w.Write([]byte("server:\n  port: 9000\nroutes:\n  - id: order\n"))
		case "hidden.yaml":
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
	return &plugin{connections: map[string]connectionRecord{}}
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
	if !ok || len(tools) != 4 {
		t.Fatalf("expected 4 tools, got %v", result["tools"])
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
	for _, want := range []string{"get_config", "search_configs", "list_namespaces", "list_services"} {
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

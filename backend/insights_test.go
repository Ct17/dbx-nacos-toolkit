package main

// insights_test.go —— v0.3 四个只读能力的单元测试。
// mock 服务器与协议用例共享 main_test.go 中的 newMockNacos / newMockNacosV3。

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func decodeToolJSON(t *testing.T, result map[string]any) map[string]any {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal([]byte(toolText(t, result)), &payload); err != nil {
		t.Fatalf("tool result is not JSON: %v\n%s", err, toolText(t, result))
	}
	return payload
}

func jsonArray(value any) []any {
	items, _ := value.([]any)
	return items
}

func jsonObject(value any) map[string]any {
	object, _ := value.(map[string]any)
	return object
}

// --- F1: find_in_configs ----------------------------------------------------

func TestFindInConfigsV1(t *testing.T) {
	server := newMockNacos(t)
	p := newTestPlugin()
	result := callToolRaw(t, p, "find_in_configs", map[string]any{"keyword": "port: 9000"}, connectionPayload(t, server))
	if toolIsError(result) {
		t.Fatalf("unexpected tool error: %s", toolText(t, result))
	}
	payload := decodeToolJSON(t, result)
	matches := jsonArray(payload["matches"])
	if len(matches) != 1 {
		t.Fatalf("expected one matching config, got %s", toolText(t, result))
	}
	match := jsonObject(matches[0])
	if match["dataId"] != "gateway.yaml" || match["namespace"] != "dev" {
		t.Fatalf("unexpected match: %v", match)
	}
	if int(match["matchCount"].(float64)) != 1 {
		t.Fatalf("expected one matching line: %v", match)
	}
	snippets := jsonArray(match["snippets"])
	if len(snippets) != 1 || !strings.Contains(jsonObject(snippets[0])["text"].(string), "port: 9000") {
		t.Fatalf("snippet missing: %v", match)
	}
	if payload["candidateCount"].(float64) != 2 || payload["scannedConfigs"].(float64) != 1 {
		t.Fatalf("unexpected scan counts: %v", payload)
	}
	if payload["truncated"] != false {
		t.Fatalf("full scan must not be truncated: %v", payload)
	}
}

func TestFindInConfigsV1CaseSensitive(t *testing.T) {
	server := newMockNacos(t)
	p := newTestPlugin()

	insensitive := callToolRaw(t, p, "find_in_configs", map[string]any{"keyword": "SERVER"}, connectionPayload(t, server))
	if toolIsError(insensitive) || len(jsonArray(decodeToolJSON(t, insensitive)["matches"])) != 1 {
		t.Fatalf("case-insensitive search should match: %s", toolText(t, insensitive))
	}

	sensitive := callToolRaw(t, p, "find_in_configs", map[string]any{"keyword": "SERVER", "caseSensitive": true}, connectionPayload(t, server))
	if toolIsError(sensitive) || len(jsonArray(decodeToolJSON(t, sensitive)["matches"])) != 0 {
		t.Fatalf("case-sensitive search should not match: %s", toolText(t, sensitive))
	}
}

func TestFindInConfigsV1ScanLimit(t *testing.T) {
	server := newMockNacos(t)
	p := newTestPlugin()
	result := callToolRaw(t, p, "find_in_configs", map[string]any{
		"keyword": "port", "scanLimit": 1,
	}, connectionPayload(t, server))
	if toolIsError(result) {
		t.Fatalf("unexpected tool error: %s", toolText(t, result))
	}
	payload := decodeToolJSON(t, result)
	if payload["truncated"] != true || !strings.Contains(payload["truncatedReason"].(string), "scanLimit 1") {
		t.Fatalf("expected scanLimit truncation: %v", payload)
	}
}

func TestFindInConfigsV3(t *testing.T) {
	server, console, _, _ := newMockNacosV3(t)
	p := newTestPlugin()
	result := callToolRaw(t, p, "find_in_configs", map[string]any{"keyword": "port"}, v3ConnectionPayload(t, server, console))
	if toolIsError(result) {
		t.Fatalf("unexpected tool error: %s", toolText(t, result))
	}
	payload := decodeToolJSON(t, result)
	if len(jsonArray(payload["matches"])) != 1 || payload["candidateCount"].(float64) != 1 {
		t.Fatalf("v3 content search result mismatch: %s", toolText(t, result))
	}

	empty := callToolRaw(t, p, "find_in_configs", map[string]any{"keyword": "not-there"}, v3ConnectionPayload(t, server, console))
	if toolIsError(empty) {
		t.Fatalf("no matches must not be an error: %s", toolText(t, empty))
	}
	if len(jsonArray(decodeToolJSON(t, empty)["matches"])) != 0 {
		t.Fatalf("expected no matches: %s", toolText(t, empty))
	}
}

func TestFindInConfigsRequiresKeyword(t *testing.T) {
	server := newMockNacos(t)
	p := newTestPlugin()
	result := callToolRaw(t, p, "find_in_configs", map[string]any{}, connectionPayload(t, server))
	if !toolIsError(result) || !strings.Contains(toolText(t, result), "keyword is required") {
		t.Fatalf("missing keyword should fail: %v", result)
	}
}

// TestScanConfigCandidatesTimeout 覆盖扫描中途超时的路径：派发循环与 worker
// 会并发置位超时标记，`go test -race` 据此验证二者的同步方式。
func TestScanConfigCandidatesTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(20 * time.Millisecond)
		_, _ = w.Write([]byte("server:\n  port: 9000\n"))
	}))
	defer server.Close()

	client := &nacosClient{serverBase: server.URL + "/nacos", http: server.Client(), family: familyV1}
	candidates := make([]configItem, 0, 40)
	for i := 0; i < 40; i++ {
		candidates = append(candidates, configItem{
			DataID: fmt.Sprintf("app-%02d.yaml", i),
			Group:  "DEFAULT_GROUP",
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	matches, scanned, failures, timedOut := scanConfigCandidates(ctx, client, "", candidates, "port", 50, false)
	if !timedOut {
		t.Fatalf("expected the timeout flag to be set, scanned=%d matches=%d", scanned, len(matches))
	}
	if scanned >= len(candidates) {
		t.Fatalf("expected a partial scan, got %d/%d", scanned, len(candidates))
	}
	if len(failures) != 0 {
		t.Fatalf("cancellation must not be reported as read failures: %v", failures)
	}
}

// --- F2: diff_envs ----------------------------------------------------------

func TestDiffEnvsV1(t *testing.T) {
	server := newMockNacos(t)
	p := newTestPlugin()
	result := callToolRaw(t, p, "diff_envs", map[string]any{
		"namespaces": []any{"dev", "test", "prod"},
	}, connectionPayload(t, server))
	if toolIsError(result) {
		t.Fatalf("unexpected tool error: %s", toolText(t, result))
	}
	payload := decodeToolJSON(t, result)
	if payload["baseline"] != "dev" {
		t.Fatalf("unexpected baseline: %v", payload)
	}
	summaries := map[string]map[string]any{}
	for _, entry := range jsonArray(payload["summaries"]) {
		summary := jsonObject(entry)
		summaries[summary["namespace"].(string)] = summary
	}
	if summaries["test"]["identical"].(float64) != 2 || summaries["test"]["different"].(float64) != 0 {
		t.Fatalf("dev vs test should be identical: %v", summaries["test"])
	}
	prod := summaries["prod"]
	if prod["different"].(float64) != 1 || prod["onlyInBaseline"].(float64) != 1 || prod["onlyInTarget"].(float64) != 1 {
		t.Fatalf("unexpected prod summary: %v", prod)
	}
	statuses := map[string]string{}
	for _, entry := range jsonArray(payload["changed"]) {
		change := jsonObject(entry)
		statuses[change["dataId"].(string)] = change["status"].(string)
	}
	if statuses["gateway.yaml"] != "different" || statuses["orders.yaml"] != "only-in-baseline" || statuses["extra.yaml"] != "only-in-target" {
		t.Fatalf("unexpected changed entries: %v", statuses)
	}
}

func TestDiffEnvsIncludeContentDiff(t *testing.T) {
	server := newMockNacos(t)
	p := newTestPlugin()
	result := callToolRaw(t, p, "diff_envs", map[string]any{
		"namespaces":         []any{"dev", "prod"},
		"includeContentDiff": true,
	}, connectionPayload(t, server))
	if toolIsError(result) {
		t.Fatalf("unexpected tool error: %s", toolText(t, result))
	}
	payload := decodeToolJSON(t, result)
	for _, entry := range jsonArray(payload["changed"]) {
		change := jsonObject(entry)
		if change["dataId"] != "gateway.yaml" {
			continue
		}
		diff, _ := change["diff"].(string)
		if !strings.Contains(diff, "+   port: 8080") || !strings.Contains(diff, "-   port: 9000") {
			t.Fatalf("content diff missing: %v", change)
		}
		if change["added"].(float64) != 2 || change["removed"].(float64) != 1 {
			t.Fatalf("unexpected change counts: %v", change)
		}
		return
	}
	t.Fatalf("gateway.yaml change not found: %s", toolText(t, result))
}

func TestDiffEnvsV1FillsMissingMD5(t *testing.T) {
	// Nacos 2.4.3 的 blur 搜索响应 md5 为 null；插件必须抓正文计算 MD5，
	// 否则所有配置都会被误判为 identical。
	mux := http.NewServeMux()
	mux.HandleFunc("/nacos/v1/cs/configs", func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		if query.Get("search") == "blur" {
			_, _ = w.Write([]byte(`{"totalCount":1,"pageNumber":1,"pagesAvailable":1,"pageItems":[
				{"dataId":"gateway.yaml","group":"DEFAULT_GROUP","tenant":"dev","type":"yaml","md5":null}
			]}`))
			return
		}
		if query.Get("tenant") == "prod" {
			_, _ = w.Write([]byte("server:\n  port: 8080\n"))
			return
		}
		_, _ = w.Write([]byte("server:\n  port: 9000\n"))
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	host, port := mockHostPort(t, server.URL)
	connection := map[string]any{
		"id": "conn-md5", "name": "md5 fallback",
		"host": host, "port": float64(port),
		"external_config": map[string]any{"context_path": "/nacos", "namespace": "dev", "api_version": "v1"},
	}
	p := newTestPlugin()
	result := callToolRaw(t, p, "diff_envs", map[string]any{
		"namespaces": []any{"dev", "prod"},
	}, connection)
	if toolIsError(result) {
		t.Fatalf("unexpected tool error: %s", toolText(t, result))
	}
	payload := decodeToolJSON(t, result)
	summaries := jsonArray(payload["summaries"])
	if len(summaries) != 1 || jsonObject(summaries[0])["different"].(float64) != 1 {
		t.Fatalf("missing md5 fallback did not detect the difference: %s", toolText(t, result))
	}
}

func TestDiffEnvsRejectsDuplicateNamespaces(t *testing.T) {
	server := newMockNacos(t)
	p := newTestPlugin()
	result := callToolRaw(t, p, "diff_envs", map[string]any{
		"namespaces": []any{"dev", "dev"},
	}, connectionPayload(t, server))
	if !toolIsError(result) || !strings.Contains(toolText(t, result), "distinct") {
		t.Fatalf("duplicate namespaces should fail: %v", result)
	}
}

func TestDiffEnvsV3(t *testing.T) {
	server, console, _, _ := newMockNacosV3(t)
	p := newTestPlugin()
	result := callToolRaw(t, p, "diff_envs", map[string]any{
		"namespaces": []any{"dev", "prod"},
	}, v3ConnectionPayload(t, server, console))
	if toolIsError(result) {
		t.Fatalf("unexpected tool error: %s", toolText(t, result))
	}
	payload := decodeToolJSON(t, result)
	summaries := jsonArray(payload["summaries"])
	if len(summaries) != 1 {
		t.Fatalf("expected one target summary: %s", toolText(t, result))
	}
	prod := jsonObject(summaries[0])
	if prod["different"].(float64) != 1 || prod["onlyInBaseline"].(float64) != 1 || prod["onlyInTarget"].(float64) != 1 {
		t.Fatalf("unexpected v3 diff summary: %v", prod)
	}
}

// --- F3: diff_history -------------------------------------------------------

func TestDiffHistoryCurrentV1(t *testing.T) {
	server := newMockNacos(t)
	p := newTestPlugin()
	result := callToolRaw(t, p, "diff_history", map[string]any{
		"dataId": "gateway.yaml", "fromHistoryId": "1001",
	}, connectionPayload(t, server))
	if toolIsError(result) {
		t.Fatalf("unexpected tool error: %s", toolText(t, result))
	}
	payload := decodeToolJSON(t, result)
	if payload["status"] != "different" || payload["added"].(float64) != 3 || payload["removed"].(float64) != 1 {
		t.Fatalf("unexpected history diff: %s", toolText(t, result))
	}
	from := jsonObject(payload["from"])
	if from["historyId"] != "1001" || from["operator"] != "alice" {
		t.Fatalf("from metadata missing: %v", from)
	}
	to := jsonObject(payload["to"])
	if to["source"] != "current" || to["exists"] != true {
		t.Fatalf("to metadata missing: %v", to)
	}
	diff := payload["diff"].(string)
	if !strings.Contains(diff, "+   port: 9000") || !strings.Contains(diff, "-   port: 8999") {
		t.Fatalf("unexpected diff: %s", diff)
	}
}

func TestDiffHistoryTwoRevisionsV1(t *testing.T) {
	server := newMockNacos(t)
	p := newTestPlugin()
	result := callToolRaw(t, p, "diff_history", map[string]any{
		"dataId": "gateway.yaml", "fromHistoryId": "1001", "toHistoryId": "1000",
	}, connectionPayload(t, server))
	if toolIsError(result) {
		t.Fatalf("unexpected tool error: %s", toolText(t, result))
	}
	payload := decodeToolJSON(t, result)
	to := jsonObject(payload["to"])
	if to["source"] != "history" || to["historyId"] != "1000" || to["operator"] != "bob" {
		t.Fatalf("target revision metadata missing: %v", to)
	}
	if payload["status"] != "different" {
		t.Fatalf("expected different: %s", toolText(t, result))
	}
}

func TestDiffHistoryMissingCurrentV1(t *testing.T) {
	server := newMockNacos(t)
	p := newTestPlugin()
	result := callToolRaw(t, p, "diff_history", map[string]any{
		"dataId": "gateway.yaml", "namespace": "empty-ns", "fromHistoryId": "1001",
	}, connectionPayload(t, server))
	if toolIsError(result) {
		t.Fatalf("missing current config should be a status, not a tool error: %s", toolText(t, result))
	}
	payload := decodeToolJSON(t, result)
	if payload["status"] != "missing-in-target" {
		t.Fatalf("unexpected status: %s", toolText(t, result))
	}
	if jsonObject(payload["to"])["exists"] != false {
		t.Fatalf("to.exists should be false: %s", toolText(t, result))
	}
}

func TestDiffHistoryRevisionNotFound(t *testing.T) {
	server := newMockNacos(t)
	p := newTestPlugin()
	result := callToolRaw(t, p, "diff_history", map[string]any{
		"dataId": "gateway.yaml", "fromHistoryId": "999",
	}, connectionPayload(t, server))
	if !toolIsError(result) || !strings.Contains(strings.ToLower(toolText(t, result)), "not found") {
		t.Fatalf("missing revision should fail: %v", result)
	}
}

func TestDiffHistoryV3(t *testing.T) {
	server, console, _, _ := newMockNacosV3(t)
	p := newTestPlugin()
	result := callToolRaw(t, p, "diff_history", map[string]any{
		"dataId": "gateway.yaml", "fromHistoryId": "180",
	}, v3ConnectionPayload(t, server, console))
	if toolIsError(result) {
		t.Fatalf("unexpected tool error: %s", toolText(t, result))
	}
	payload := decodeToolJSON(t, result)
	if payload["status"] != "different" || !strings.Contains(payload["diff"].(string), "+   port: 9000") {
		t.Fatalf("unexpected v3 history diff: %s", toolText(t, result))
	}
}

// --- F4: diagnose -----------------------------------------------------------

func TestDiagnoseV1(t *testing.T) {
	server := newMockNacos(t)
	p := newTestPlugin()
	result := callToolRaw(t, p, "diagnose", map[string]any{}, connectionPayload(t, server))
	if toolIsError(result) {
		t.Fatalf("diagnose must return a report, not an error: %s", toolText(t, result))
	}
	payload := decodeToolJSON(t, result)
	if payload["overall"] != "ok" {
		t.Fatalf("expected ok diagnosis: %s", toolText(t, result))
	}
	serverInfo := jsonObject(payload["server"])
	if serverInfo["version"] != "2.4.3" || serverInfo["protocol"] != "v1" {
		t.Fatalf("unexpected server info: %v", serverInfo)
	}
	checks := map[string]string{}
	for _, entry := range jsonArray(payload["checks"]) {
		check := jsonObject(entry)
		checks[check["id"].(string)] = check["status"].(string)
	}
	for _, id := range []string{"protocol", "health", "server-state", "credentials", "namespace-access", "config-read"} {
		if checks[id] != "ok" {
			t.Fatalf("check %s should be ok: %v", id, checks)
		}
	}
}

func TestDiagnoseV1AnonymousAuthDisabled(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/nacos/v1/console/health/readiness", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"status":"UP"}`))
	})
	mux.HandleFunc("/nacos/v1/console/server/state", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"version":"2.4.3","serverEnv":"standalone","auth_enabled":"false"}`))
	})
	mux.HandleFunc("/nacos/v1/console/namespaces", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"code":200,"data":[{"namespace":"","namespaceShowName":"public"},{"namespace":"dev","namespaceShowName":"dev"}]}`))
	})
	mux.HandleFunc("/nacos/v1/cs/configs", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	host, port := mockHostPort(t, server.URL)
	connection := map[string]any{
		"id": "conn-anon", "name": "anonymous",
		"host": host, "port": float64(port),
		"external_config": map[string]any{"context_path": "/nacos", "namespace": "dev", "api_version": "v1"},
	}
	p := newTestPlugin()
	result := callToolRaw(t, p, "diagnose", map[string]any{}, connection)
	if toolIsError(result) {
		t.Fatalf("diagnose must return a report, not an error: %s", toolText(t, result))
	}
	payload := decodeToolJSON(t, result)
	if payload["overall"] != "ok" {
		t.Fatalf("auth-disabled anonymous v1 should be ok: %s", toolText(t, result))
	}
}

func TestDiagnoseV1WrongPassword(t *testing.T) {
	server := newMockNacos(t)
	p := newTestPlugin()
	connection := connectionPayload(t, server)
	connection["password"] = "wrong"
	result := callToolRaw(t, p, "diagnose", map[string]any{}, connection)
	if toolIsError(result) {
		t.Fatalf("diagnose must return a report, not an error: %s", toolText(t, result))
	}
	payload := decodeToolJSON(t, result)
	if payload["overall"] != "fail" {
		t.Fatalf("wrong password should fail diagnosis: %s", toolText(t, result))
	}
}

func TestDiagnoseV3(t *testing.T) {
	server, console, _, _ := newMockNacosV3(t)
	p := newTestPlugin()
	result := callToolRaw(t, p, "diagnose", map[string]any{}, v3ConnectionPayload(t, server, console))
	if toolIsError(result) {
		t.Fatalf("diagnose must return a report, not an error: %s", toolText(t, result))
	}
	payload := decodeToolJSON(t, result)
	if payload["overall"] != "ok" {
		t.Fatalf("expected ok diagnosis: %s", toolText(t, result))
	}
	serverInfo := jsonObject(payload["server"])
	if serverInfo["version"] != "3.2.4" || serverInfo["protocol"] != "v3" || serverInfo["authEnabled"] != true {
		t.Fatalf("unexpected v3 server info: %v", serverInfo)
	}
}

func TestDiagnoseV3WithoutCredentials(t *testing.T) {
	server, console, _, _ := newMockNacosV3(t)
	p := newTestPlugin()
	connection := v3ConnectionPayload(t, server, console)
	delete(connection, "username")
	delete(connection, "password")
	result := callToolRaw(t, p, "diagnose", map[string]any{}, connection)
	if toolIsError(result) {
		t.Fatalf("diagnose must return a report, not an error: %s", toolText(t, result))
	}
	payload := decodeToolJSON(t, result)
	if payload["overall"] != "warn" {
		t.Fatalf("anonymous v3 console should warn: %s", toolText(t, result))
	}
	checks := map[string]string{}
	for _, entry := range jsonArray(payload["checks"]) {
		check := jsonObject(entry)
		checks[check["id"].(string)] = check["status"].(string)
	}
	if checks["credentials"] != "warn" || checks["namespace-access"] != "warn" {
		t.Fatalf("expected credential warnings: %v", checks)
	}
}

func TestDiagnoseUnreachable(t *testing.T) {
	p := newTestPlugin()
	port := deadPort(t)
	connection := map[string]any{
		"id": "conn-dead", "name": "dead",
		"host": "127.0.0.1", "port": float64(port),
		"external_config": map[string]any{"context_path": "/nacos", "api_version": "v1"},
	}
	result := callToolRaw(t, p, "diagnose", map[string]any{}, connection)
	if toolIsError(result) {
		t.Fatalf("unreachable server should be a failed report, not a tool error: %s", toolText(t, result))
	}
	payload := decodeToolJSON(t, result)
	if payload["overall"] != "fail" {
		t.Fatalf("unreachable server should fail diagnosis: %s", toolText(t, result))
	}
}

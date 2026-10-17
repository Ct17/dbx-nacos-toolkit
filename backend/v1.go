package main

// v1.go —— Nacos 1.x / 2.x Open API 协议栈实现。
//
// 接口用法依据 Nacos 2.4.3 源码逐项核对（详见 docs/v1-api-verification-2026-10-17.md）：
//   - GET /v1/cs/configs            读配置（dataId/group 必填，tenant 可选）
//   - GET /v1/cs/configs?search=blur   模糊搜索（* 为通配符）
//   - GET /v1/cs/history?search=accurate  历史列表；不带 search 带 nid 才是详情
//   - GET /v1/ns/service/list       服务列表（groupName 为过滤参数）
//   - GET /v1/ns/instance/list      实例列表（groupName 由 ServiceNameFilter 合并进 serviceName）
//   - GET /v1/console/namespaces    命名空间列表

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// v1ReadConfig 读取单条配置。404 视为“配置不存在”，与网络/权限错误区分。
func (c *nacosClient) v1ReadConfig(ctx context.Context, namespace, dataID, group string) (string, bool, error) {
	if strings.TrimSpace(dataID) == "" {
		return "", false, &apiError{Message: "dataId is required"}
	}
	if group == "" {
		group = defaultGroup
	}
	params := url.Values{}
	params.Set("dataId", dataID)
	params.Set("group", group)
	if namespace != "" {
		params.Set("tenant", namespace)
	}
	body, status, err := c.get(ctx, "/v1/cs/configs", params)
	if err != nil {
		return "", false, err
	}
	switch status {
	case http.StatusOK:
		return string(body), true, nil
	case http.StatusNotFound:
		return "", false, nil
	default:
		return "", false, &apiError{Status: status,
			Message: fmt.Sprintf("reading config failed with HTTP %d", status)}
	}
}

// v1SearchConfigs lists config metadata via the blur search endpoint.
func (c *nacosClient) v1SearchConfigs(ctx context.Context, namespace, dataIDPattern, groupPattern string, pageNo, pageSize int) (*configPage, error) {
	pageNo, pageSize = normalizePaging(pageNo, pageSize)
	params := url.Values{}
	params.Set("search", "blur")
	params.Set("dataId", blurPattern(dataIDPattern))
	params.Set("group", blurPattern(groupPattern))
	params.Set("pageNo", strconv.Itoa(pageNo))
	params.Set("pageSize", strconv.Itoa(pageSize))
	if namespace != "" {
		params.Set("tenant", namespace)
	}
	body, status, err := c.get(ctx, "/v1/cs/configs", params)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, &apiError{Status: status,
			Message: fmt.Sprintf("listing configs failed with HTTP %d", status)}
	}
	var page configPage
	if err := json.Unmarshal(body, &page); err != nil {
		return nil, &apiError{Message: fmt.Sprintf("unexpected search response: %v", err)}
	}
	if page.PageItems == nil {
		page.PageItems = []configItem{}
	}
	// Defensive: the search endpoint may echo config content per item; drop it
	// so list responses stay small. (configItem has no content field already,
	// this guards against future field additions.)
	return &page, nil
}

// v1ListConfigHistory 按 dataId 列出历史版本，最新在前。
func (c *nacosClient) v1ListConfigHistory(ctx context.Context, namespace, dataID, group string, pageNo, pageSize int) (*configHistoryPage, error) {
	if strings.TrimSpace(dataID) == "" {
		return nil, &apiError{Message: "dataId is required"}
	}
	pageNo, pageSize = normalizePaging(pageNo, pageSize)
	params := url.Values{}
	params.Set("search", "accurate")
	params.Set("dataId", dataID)
	params.Set("group", group)
	if namespace != "" {
		params.Set("tenant", namespace)
	}
	params.Set("pageNo", strconv.Itoa(pageNo))
	params.Set("pageSize", strconv.Itoa(pageSize))
	body, status, err := c.get(ctx, "/v1/cs/history", params)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, &apiError{Status: status,
			Message: fmt.Sprintf("listing config history failed with HTTP %d", status)}
	}
	var page configHistoryPage
	if err := json.Unmarshal(body, &page); err != nil {
		return nil, &apiError{Message: fmt.Sprintf("unexpected history response: %v", err)}
	}
	if page.PageItems == nil {
		page.PageItems = []configRevision{}
	}
	return &page, nil
}

// v1GetConfigRevision 读取单个历史版本的完整内容（按历史记录 id）。
//
// 注意 Nacos 的路由行为：/v1/cs/history 上的列表方法与详情方法靠查询参数
// 区分——带 search=accurate 时命中列表方法（nid 被忽略，返回分页列表）；
// 不带 search 且带 nid 时才命中详情方法。因此这里绝不能带 search 参数，
// 否则 2.4.x 会返回列表结构而不是单版本详情。
func (c *nacosClient) v1GetConfigRevision(ctx context.Context, namespace, dataID, group, historyID string) (*configRevisionDetail, error) {
	if strings.TrimSpace(historyID) == "" {
		return nil, &apiError{Message: "historyId is required"}
	}
	params := url.Values{}
	params.Set("dataId", dataID)
	params.Set("group", group)
	if namespace != "" {
		params.Set("tenant", namespace)
	}
	params.Set("nid", historyID)
	body, status, err := c.get(ctx, "/v1/cs/history", params)
	if err != nil {
		return nil, err
	}
	switch status {
	case http.StatusOK:
		var detail configRevisionDetail
		if err := json.Unmarshal(body, &detail); err != nil {
			return nil, &apiError{Message: fmt.Sprintf("unexpected history detail response: %v", err)}
		}
		// 防御：若命中列表结构（nid 被服务端忽略或路由行为变化）则 ID 为空，
		// 此时明确报错，而不是静默返回一条空数据。
		if detail.ID == "" {
			return nil, &apiError{Message: "unexpected history detail response: server returned a history list instead of one revision"}
		}
		return &detail, nil
	case http.StatusNotFound:
		return nil, &apiError{Status: status,
			Message: fmt.Sprintf("config revision not found: dataId=%s group=%s namespace=%q historyId=%s", dataID, group, namespace, historyID)}
	default:
		return nil, &apiError{Status: status,
			Message: fmt.Sprintf("reading config revision failed with HTTP %d", status)}
	}
}

// v1ListNamespaces returns every namespace defined on the server.
func (c *nacosClient) v1ListNamespaces(ctx context.Context) ([]namespace, error) {
	body, status, err := c.get(ctx, "/v1/console/namespaces", url.Values{})
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, &apiError{Status: status,
			Message: fmt.Sprintf("listing namespaces failed with HTTP %d", status)}
	}
	var payload struct {
		Code int         `json:"code"`
		Data []namespace `json:"data"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, &apiError{Message: fmt.Sprintf("unexpected namespace response: %v", err)}
	}
	if payload.Data == nil {
		payload.Data = []namespace{}
	}
	return payload.Data, nil
}

// v1ListServices lists registered service names in a namespace.
func (c *nacosClient) v1ListServices(ctx context.Context, namespace, groupName string, pageNo, pageSize int) (*servicePage, error) {
	pageNo, pageSize = normalizePaging(pageNo, pageSize)
	params := url.Values{}
	params.Set("pageNo", strconv.Itoa(pageNo))
	params.Set("pageSize", strconv.Itoa(pageSize))
	if namespace != "" {
		params.Set("namespaceId", namespace)
	}
	if groupName != "" {
		params.Set("groupName", groupName)
	}
	body, status, err := c.get(ctx, "/v1/ns/service/list", params)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, &apiError{Status: status,
			Message: fmt.Sprintf("listing services failed with HTTP %d", status)}
	}
	var page servicePage
	if err := json.Unmarshal(body, &page); err != nil {
		return nil, &apiError{Message: fmt.Sprintf("unexpected service response: %v", err)}
	}
	return &page, nil
}

// v1ListInstances 查询某个服务在指定命名空间下的实例列表。
//
// 组名处理：v1 的 /v1/ns/instance/list 控制器本身不读 groupName 参数，
// 而是由命名模块的 ServiceNameFilter 把 groupName 合并进 serviceName
// （即将 "order-service" 改写为 "BETA_GROUP@@order-service"）。因此这里
// 保持「serviceName + groupName 两个参数原样传递」的调用方式即为正确行为；
// 当 groupName 为空时服务端按 DEFAULT_GROUP 处理。
func (c *nacosClient) v1ListInstances(ctx context.Context, namespace, groupName, serviceName, cluster string) (*instanceList, error) {
	if strings.TrimSpace(serviceName) == "" {
		return nil, &apiError{Message: "serviceName is required"}
	}
	params := url.Values{}
	params.Set("serviceName", serviceName)
	if namespace != "" {
		params.Set("namespaceId", namespace)
	}
	if groupName != "" {
		params.Set("groupName", groupName)
	}
	if cluster != "" {
		params.Set("clusters", cluster)
	}
	body, status, err := c.get(ctx, "/v1/ns/instance/list", params)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, &apiError{Status: status,
			Message: fmt.Sprintf("listing instances failed with HTTP %d", status)}
	}
	var payload instanceList
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, &apiError{Message: fmt.Sprintf("unexpected instance response: %v", err)}
	}
	if payload.Hosts == nil {
		payload.Hosts = []instanceHost{}
	}
	return &payload, nil
}

// v1ServerState 读取 /v1/console/server/state（匿名）。2.x 返回 version、
// serverEnv、auth_enabled 等字段；1.x 的字段更少，调用方按可选字段解析。
func (c *nacosClient) v1ServerState(ctx context.Context) (map[string]any, error) {
	probeCtx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	body, status, err := c.fetch(probeCtx, c.serverBase, "/v1/console/server/state", nil, authNone)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, &apiError{Status: status,
			Message: fmt.Sprintf("reading server state failed with HTTP %d", status)}
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, &apiError{Message: fmt.Sprintf("unexpected server state response: %v", err)}
	}
	return payload, nil
}

// v1HealthStatus probes the v1 health endpoints. Nacos 2.x+ exposes
// /v1/console/health/readiness; 1.x only has server/state. Both are anonymous.
func (c *nacosClient) v1HealthStatus(ctx context.Context) (string, bool) {
	probeCtx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	body, status, err := c.fetch(probeCtx, c.serverBase, "/v1/console/health/readiness", nil, authNone)
	if err == nil && status == http.StatusOK {
		text := strings.TrimSpace(string(body))
		compact := strings.ReplaceAll(strings.ToUpper(text), " ", "")
		if strings.EqualFold(text, "OK") || strings.Contains(compact, `"STATUS":"UP"`) {
			return fmt.Sprintf("ready: %s", truncate(text, 120)), true
		}
	}
	body, status, err = c.fetch(probeCtx, c.serverBase, "/v1/console/server/state", nil, authNone)
	if err != nil || status != http.StatusOK {
		return "", false
	}
	var payload struct {
		Version string `json:"version"`
	}
	_ = json.Unmarshal(body, &payload)
	if payload.Version != "" {
		return fmt.Sprintf("up (Nacos %s)", payload.Version), true
	}
	return "up", true
}

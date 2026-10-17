package main

// v3.go —— Nacos 3.x 原生 API 协议栈实现。
//
// 端点、参数与鉴权行为均由 3.2.4 源码与真实实例（无 adapter 部署）实测确认：
//
//	client 接口（服务端口，{serverBase}/v3/client/**）：
//	  GET /v3/client/cs/config        读配置（dataId/groupName/namespaceId；
//	                                 不存在时 HTTP 200 + code 20004）
//	  GET /v3/client/ns/instance/list 实例列表（不返回 enabled=false 的实例）
//
//	console 接口（独立控制台端口，{consoleBase}/v3/console/**，无 context path）：
//	  GET  /v3/console/health/readiness     健康检查（匿名）
//	  GET  /v3/console/cs/config/list       模糊搜索
//	  GET  /v3/console/cs/history/list      历史列表
//	  GET  /v3/console/cs/history?nid=      历史详情（含内容）
//	  GET  /v3/console/core/namespace/list  命名空间列表
//	  GET  /v3/console/ns/service/list      服务列表
//	  POST /v3/auth/user/login              登录（服务端口、控制台端口均可用）
//
// console 接口（除健康检查外）强制 Bearer 鉴权——即使服务端关闭了鉴权开关，
// 也需要先初始化管理员账号并登录；client 接口在关闭鉴权时匿名可用。
//
// 统一响应封装（服务端 Result<T>）：{"code":0,"message":"success","data":…}。
// 失败时 data 可能是与成功态完全不同的形状（例如鉴权失败时是字符串），
// 因此解码分两段：先解封装头部，成功后再按类型解 data。

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// Nacos 3.x 响应封装中的业务码（HTTP 状态通常为 200）。
const (
	v3CodeSuccess          = 0
	v3CodeAccessDenied     = 10001
	v3CodeResourceNotFound = 20004
)

// v3Payload 是解码后的统一响应头部 + 原始 data。
type v3Payload struct {
	Code    int
	Message string
	Data    json.RawMessage
}

// decodeV3 解出封装头部。业务码非 0（或 HTTP 非 200）时返回 (payload, apiError)，
// 调用方可以通过 payload.Code 识别需要特殊处理的场景（如 20004 等价于“资源不存在”）。
// 非 200 且响应不是标准封装时（例如网关错误页），错误信息回落到 HTTP 状态。
func decodeV3(body []byte, status int, action string) (*v3Payload, error) {
	var raw struct {
		Code    int             `json:"code"`
		Message string          `json:"message"`
		Data    json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, &apiError{Status: status,
			Message: fmt.Sprintf("unexpected %s response (HTTP %d): %v", action, status, err)}
	}
	payload := &v3Payload{Code: raw.Code, Message: raw.Message, Data: raw.Data}
	if payload.Code == v3CodeSuccess && status == http.StatusOK {
		return payload, nil
	}
	if payload.Code == v3CodeSuccess {
		return payload, &apiError{Status: status,
			Message: fmt.Sprintf("%s failed with HTTP %d", action, status)}
	}
	return payload, &apiError{Status: status,
		Message: v3FailureMessage(action, status, payload.Code, payload.Message)}
}

// decodeV3Data 在封装成功之后把 data 解析为目标类型。
func decodeV3Data[T any](payload *v3Payload, action string) (T, error) {
	var out T
	if len(payload.Data) == 0 || string(payload.Data) == "null" {
		return out, &apiError{Message: fmt.Sprintf("unexpected %s response: empty data", action)}
	}
	if err := json.Unmarshal(payload.Data, &out); err != nil {
		return out, &apiError{Message: fmt.Sprintf("unexpected %s response: %v", action, err)}
	}
	return out, nil
}

// v3FailureMessage 把业务码翻译成可读错误，重点覆盖鉴权与不存在两类。
func v3FailureMessage(action string, status, code int, message string) string {
	switch code {
	case v3CodeAccessDenied:
		return fmt.Sprintf("%s failed: access denied (HTTP %d); Nacos 3.x requires authentication — "+
			"configure username/password and make sure the account has access", action, status)
	case v3CodeResourceNotFound:
		return fmt.Sprintf("%s failed: resource not found", action)
	default:
		if strings.TrimSpace(message) == "" {
			return fmt.Sprintf("%s failed with code %d", action, code)
		}
		return fmt.Sprintf("%s failed: %s (code %d)", action, message, code)
	}
}

// --- health ---------------------------------------------------------------

// v3ReadinessProbe 匿名探测控制台健康端点（供协议识别与 health 复用）。
func (c *nacosClient) v3ReadinessProbe(ctx context.Context) (int, []byte, error) {
	probeCtx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	body, status, err := c.fetch(probeCtx, c.consoleBase, "/v3/console/health/readiness", nil, authNone)
	return status, body, err
}

// parseV3Readiness 判断响应是否为 {"code":0,"data":"ok"}。
// 形状校验避免把恰好监听 console 端口的其他服务误判为 Nacos。
func parseV3Readiness(body []byte) bool {
	var payload struct {
		Code int    `json:"code"`
		Data string `json:"data"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return false
	}
	return payload.Code == v3CodeSuccess && strings.EqualFold(strings.TrimSpace(payload.Data), "ok")
}

// checkV3Readiness 判断目标是否是 Nacos 3.x。
func (c *nacosClient) checkV3Readiness(ctx context.Context) bool {
	status, body, err := c.v3ReadinessProbe(ctx)
	if err != nil || status != http.StatusOK {
		return false
	}
	return parseV3Readiness(body)
}

// v3Health 返回健康描述。
func (c *nacosClient) v3Health(ctx context.Context) (string, error) {
	status, body, err := c.v3ReadinessProbe(ctx)
	if err != nil {
		return "", err
	}
	if status != http.StatusOK || !parseV3Readiness(body) {
		return "", &apiError{Status: status,
			Message: fmt.Sprintf("Nacos 3.x console health check failed (HTTP %d)", status)}
	}
	return "ready: ok", nil
}

// --- config ---------------------------------------------------------------

// v3ReadConfig 读取单条配置。code 20004 等价于 v1 的 404（配置不存在）。
func (c *nacosClient) v3ReadConfig(ctx context.Context, namespace, dataID, group string) (string, bool, error) {
	if strings.TrimSpace(dataID) == "" {
		return "", false, &apiError{Message: "dataId is required"}
	}
	if group == "" {
		group = defaultGroup
	}
	params := url.Values{}
	params.Set("dataId", dataID)
	params.Set("groupName", group)
	if namespace != "" {
		params.Set("namespaceId", namespace)
	}
	body, status, err := c.getV3(ctx, c.serverBase, "/v3/client/cs/config", params)
	if err != nil {
		return "", false, err
	}
	// 读接口的 data 是 ConfigQueryResponse：content/md5/contentType 等。
	// 非 200（如鉴权开启时的 403）也走解码，让业务错误码优先翻译成可读信息。
	payload, err := decodeV3(body, status, "reading config")
	if err != nil {
		if payload != nil && payload.Code == v3CodeResourceNotFound {
			return "", false, nil
		}
		return "", false, err
	}
	data, err := decodeV3Data[struct {
		Content string `json:"content"`
	}](payload, "reading config")
	if err != nil {
		return "", false, err
	}
	return data.Content, true, nil
}

// v3SearchConfigs 通过控制台接口列出配置元数据。
func (c *nacosClient) v3SearchConfigs(ctx context.Context, namespace, dataIDPattern, groupPattern string, pageNo, pageSize int) (*configPage, error) {
	pageNo, pageSize = normalizePaging(pageNo, pageSize)
	params := url.Values{}
	params.Set("search", "blur")
	params.Set("dataId", blurPattern(dataIDPattern))
	params.Set("groupName", blurPattern(groupPattern))
	params.Set("pageNo", strconv.Itoa(pageNo))
	params.Set("pageSize", strconv.Itoa(pageSize))
	if namespace != "" {
		params.Set("namespaceId", namespace)
	}
	body, status, err := c.getV3(ctx, c.consoleBase, "/v3/console/cs/config/list", params)
	if err != nil {
		return nil, err
	}
	payload, err := decodeV3(body, status, "listing configs")
	if err != nil {
		return nil, err
	}
	type v3ConfigItem struct {
		NamespaceID string `json:"namespaceId"`
		GroupName   string `json:"groupName"`
		DataID      string `json:"dataId"`
		AppName     string `json:"appName"`
		Type        string `json:"type"`
		Md5         string `json:"md5"`
	}
	type v3ConfigPage struct {
		TotalCount     int            `json:"totalCount"`
		PageNumber     int            `json:"pageNumber"`
		PagesAvailable int            `json:"pagesAvailable"`
		PageItems      []v3ConfigItem `json:"pageItems"`
	}
	data, err := decodeV3Data[v3ConfigPage](payload, "listing configs")
	if err != nil {
		return nil, err
	}
	page := &configPage{
		TotalCount:     data.TotalCount,
		PageNumber:     data.PageNumber,
		PagesAvailable: data.PagesAvailable,
		PageItems:      make([]configItem, 0, len(data.PageItems)),
	}
	for _, item := range data.PageItems {
		// 与 v1 保持同一输出形状：content 不在列表中携带。
		page.PageItems = append(page.PageItems, configItem{
			DataID:  item.DataID,
			Group:   item.GroupName,
			Tenant:  item.NamespaceID,
			AppName: item.AppName,
			Type:    item.Type,
			Md5:     item.Md5,
		})
	}
	return page, nil
}

// --- history --------------------------------------------------------------

// v3HistoryItem 对齐 /v3/console/cs/history 系列响应。
// 注意：createTime 在 derby 存储下是 2010 占位值，真实变更时间在 modifyTime，
// 因此映射到 configRevision 时放进 LastModifiedTime（展示时优先）。
type v3HistoryItem struct {
	ID          flexibleString `json:"id"`
	NamespaceID string         `json:"namespaceId"`
	GroupName   string         `json:"groupName"`
	DataID      string         `json:"dataId"`
	AppName     string         `json:"appName"`
	Md5         flexibleString `json:"md5"`
	SrcIP       string         `json:"srcIp"`
	SrcUser     string         `json:"srcUser"`
	OpType      string         `json:"opType"` // 定长字段，带尾部空格（U         ）
	CreateTime  flexibleTime   `json:"createTime"`
	ModifyTime  flexibleTime   `json:"modifyTime"`
}

func (item v3HistoryItem) toRevision() configRevision {
	return configRevision{
		ID:               item.ID,
		DataID:           item.DataID,
		Group:            item.GroupName,
		Tenant:           item.NamespaceID,
		AppName:          item.AppName,
		Md5:              string(item.Md5),
		SrcIP:            item.SrcIP,
		SrcUser:          item.SrcUser,
		OpType:           item.OpType,
		CreatedTime:      item.CreateTime,
		LastModifiedTime: item.ModifyTime,
	}
}

// v3ListConfigHistory 按 dataId 列出历史版本。
func (c *nacosClient) v3ListConfigHistory(ctx context.Context, namespace, dataID, group string, pageNo, pageSize int) (*configHistoryPage, error) {
	if strings.TrimSpace(dataID) == "" {
		return nil, &apiError{Message: "dataId is required"}
	}
	if group == "" {
		group = defaultGroup
	}
	pageNo, pageSize = normalizePaging(pageNo, pageSize)
	params := url.Values{}
	params.Set("dataId", dataID)
	params.Set("groupName", group)
	params.Set("pageNo", strconv.Itoa(pageNo))
	params.Set("pageSize", strconv.Itoa(pageSize))
	if namespace != "" {
		params.Set("namespaceId", namespace)
	}
	body, status, err := c.getV3(ctx, c.consoleBase, "/v3/console/cs/history/list", params)
	if err != nil {
		return nil, err
	}
	payload, err := decodeV3(body, status, "listing config history")
	if err != nil {
		return nil, err
	}
	type v3HistoryPage struct {
		TotalCount     int             `json:"totalCount"`
		PageNumber     int             `json:"pageNumber"`
		PagesAvailable int             `json:"pagesAvailable"`
		PageItems      []v3HistoryItem `json:"pageItems"`
	}
	data, err := decodeV3Data[v3HistoryPage](payload, "listing config history")
	if err != nil {
		return nil, err
	}
	page := &configHistoryPage{
		TotalCount:     data.TotalCount,
		PageNumber:     data.PageNumber,
		PagesAvailable: data.PagesAvailable,
		PageItems:      make([]configRevision, 0, len(data.PageItems)),
	}
	for _, item := range data.PageItems {
		page.PageItems = append(page.PageItems, item.toRevision())
	}
	return page, nil
}

// v3GetConfigRevision 读取单个历史版本的完整内容。
// 与 v1 不同，v3 的详情是独立端点：GET /v3/console/cs/history?nid=…
func (c *nacosClient) v3GetConfigRevision(ctx context.Context, namespace, dataID, group, historyID string) (*configRevisionDetail, error) {
	if strings.TrimSpace(historyID) == "" {
		return nil, &apiError{Message: "historyId is required"}
	}
	if group == "" {
		group = defaultGroup
	}
	params := url.Values{}
	params.Set("nid", historyID)
	params.Set("dataId", dataID)
	params.Set("groupName", group)
	if namespace != "" {
		params.Set("namespaceId", namespace)
	}
	body, status, err := c.getV3(ctx, c.consoleBase, "/v3/console/cs/history", params)
	if err != nil {
		return nil, err
	}
	payload, err := decodeV3(body, status, "reading config revision")
	if err != nil {
		return nil, err
	}
	type v3HistoryDetail struct {
		v3HistoryItem
		Content string `json:"content"`
	}
	data, err := decodeV3Data[v3HistoryDetail](payload, "reading config revision")
	if err != nil {
		return nil, err
	}
	detail := &configRevisionDetail{
		configRevision: data.v3HistoryItem.toRevision(),
		Content:        data.Content,
	}
	// 防御：与 v1 一致，命中空结构时明确报错而不是静默返回空数据。
	if detail.ID == "" {
		return nil, &apiError{Message: "unexpected history detail response: server returned no revision"}
	}
	return detail, nil
}

// --- namespaces / services / instances -------------------------------------

// v3ListNamespaces 列出全部命名空间（命名空间 id 为 "public" 表示默认）。
func (c *nacosClient) v3ListNamespaces(ctx context.Context) ([]namespace, error) {
	body, status, err := c.getV3(ctx, c.consoleBase, "/v3/console/core/namespace/list", url.Values{})
	if err != nil {
		return nil, err
	}
	payload, err := decodeV3(body, status, "listing namespaces")
	if err != nil {
		return nil, err
	}
	type v3Namespace struct {
		Namespace   string `json:"namespace"`
		ShowName    string `json:"namespaceShowName"`
		ConfigCount int    `json:"configCount"`
		Quota       int    `json:"quota"`
		Type        int    `json:"type"`
	}
	data, err := decodeV3Data[[]v3Namespace](payload, "listing namespaces")
	if err != nil {
		return nil, err
	}
	out := make([]namespace, 0, len(data))
	for _, item := range data {
		out = append(out, namespace{
			NamespaceID:   item.Namespace,
			ShowName:      item.ShowName,
			ConfigCount:   item.ConfigCount,
			Quota:         item.Quota,
			NamespaceType: item.Type,
		})
	}
	return out, nil
}

// v3ListServices 列出命名空间内的服务名。
// 组过滤参数名为 groupNameParam；留空时与 v1 保持一致的默认行为
// （v1 服务端把缺省组名按 DEFAULT_GROUP 处理）。
func (c *nacosClient) v3ListServices(ctx context.Context, namespace, groupName string, pageNo, pageSize int) (*servicePage, error) {
	pageNo, pageSize = normalizePaging(pageNo, pageSize)
	if groupName == "" {
		groupName = defaultGroup
	}
	params := url.Values{}
	params.Set("groupNameParam", groupName)
	params.Set("pageNo", strconv.Itoa(pageNo))
	params.Set("pageSize", strconv.Itoa(pageSize))
	if namespace != "" {
		params.Set("namespaceId", namespace)
	}
	body, status, err := c.getV3(ctx, c.consoleBase, "/v3/console/ns/service/list", params)
	if err != nil {
		return nil, err
	}
	payload, err := decodeV3(body, status, "listing services")
	if err != nil {
		return nil, err
	}
	type v3ServiceItem struct {
		Name      string `json:"name"`
		GroupName string `json:"groupName"`
	}
	type v3ServicePage struct {
		TotalCount     int             `json:"totalCount"`
		PageNumber     int             `json:"pageNumber"`
		PagesAvailable int             `json:"pagesAvailable"`
		PageItems      []v3ServiceItem `json:"pageItems"`
	}
	data, err := decodeV3Data[v3ServicePage](payload, "listing services")
	if err != nil {
		return nil, err
	}
	page := &servicePage{Count: data.TotalCount}
	for _, item := range data.PageItems {
		page.Domains = append(page.Domains, item.Name)
	}
	return page, nil
}

// v3ListInstances 查询服务实例列表。
// 注意：client 接口不返回 enabled=false 的实例（Nacos 官方语义），
// 与 v1 会把这部分实例一并带 enabled 标记返回的行为不同。
func (c *nacosClient) v3ListInstances(ctx context.Context, namespace, groupName, serviceName, cluster string) (*instanceList, error) {
	if strings.TrimSpace(serviceName) == "" {
		return nil, &apiError{Message: "serviceName is required"}
	}
	if groupName == "" {
		groupName = defaultGroup
	}
	params := url.Values{}
	params.Set("serviceName", serviceName)
	params.Set("groupName", groupName)
	if namespace != "" {
		params.Set("namespaceId", namespace)
	}
	if cluster != "" {
		params.Set("clusterName", cluster)
	}
	body, status, err := c.getV3(ctx, c.serverBase, "/v3/client/ns/instance/list", params)
	if err != nil {
		return nil, err
	}
	payload, err := decodeV3(body, status, "listing instances")
	if err != nil {
		return nil, err
	}
	type v3Instance struct {
		IP          string         `json:"ip"`
		Port        int            `json:"port"`
		Weight      float64        `json:"weight"`
		Healthy     bool           `json:"healthy"`
		Enabled     bool           `json:"enabled"`
		ClusterName string         `json:"clusterName"`
		Metadata    map[string]any `json:"metadata"`
	}
	data, err := decodeV3Data[[]v3Instance](payload, "listing instances")
	if err != nil {
		return nil, err
	}
	result := &instanceList{
		Name:  groupName + "@@" + serviceName,
		Hosts: make([]instanceHost, 0, len(data)),
	}
	for _, item := range data {
		result.Hosts = append(result.Hosts, instanceHost{
			IP:      item.IP,
			Port:    item.Port,
			Healthy: item.Healthy,
			Enabled: item.Enabled,
			// v3 的 Instance 对象没有 valid 字段——保持 nil，工具输出里省略。
			Weight:      item.Weight,
			ClusterName: item.ClusterName,
			Metadata:    item.Metadata,
		})
	}
	return result, nil
}

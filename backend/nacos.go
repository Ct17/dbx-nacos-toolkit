package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// The plugin talks to Nacos through the Open API v1 compatible endpoints,
// which Nacos 1.x, 2.x and 3.x all serve. Only the standard library is used:
// no Nacos SDK dependency keeps the sidecar small and portable.

const (
	// defaultGroup is the Nacos default config group.
	defaultGroup = "DEFAULT_GROUP"

	// defaultContextPath is the standard Nacos context path.
	defaultContextPath = "/nacos"

	// requestTimeout bounds every upstream HTTP call.
	requestTimeout = 15 * time.Second

	// maxResponseBytes caps how much of an upstream response is read. It is
	// deliberately conservative: a larger response is reported as an error
	// instead of being silently truncated, and the encoded tool result has to
	// stay below the sidecar protocol frame limit.
	maxResponseBytes = 2 << 20

	// tokenRefreshMargin re-logs in slightly before the token expires.
	tokenRefreshMargin = 60 * time.Second

	// maxPageSize is the largest page Nacos serves for list endpoints.
	maxPageSize = 200
)

// apiError is a structured error from the Nacos API or transport layer.
type apiError struct {
	Status  int    // HTTP status, 0 for transport errors
	Message string // human readable detail
}

func (e *apiError) Error() string { return e.Message }

// nacosClient is a minimal Nacos Open API client.
type nacosClient struct {
	baseURL  string // scheme://host:port + context path, no trailing slash
	username string
	password string
	http     *http.Client

	mu          sync.Mutex
	token       string
	tokenExpiry time.Time
}

// newNacosClient builds a client from connection fields.
//
// host accepts "nacos.example.com", "nacos.example.com:8848", "10.0.0.1/nacos"
// or a full URL such as "https://nacos.example.com/nacos". The scheme selects
// http/https, a path embedded in host becomes the fallback context path, and
// port is appended unless the address already carries one (IPv6 aware).
func newNacosClient(host string, port int, contextPath, username, password string) (*nacosClient, error) {
	raw := strings.TrimSpace(host)
	if raw == "" {
		return nil, &apiError{Message: "connection has no server address"}
	}
	scheme := "http"
	address := raw
	embeddedPath := ""
	if strings.Contains(raw, "://") {
		parsed, err := url.Parse(raw)
		if err != nil || parsed.Host == "" {
			return nil, &apiError{Message: fmt.Sprintf("invalid server address %q", raw)}
		}
		scheme = strings.ToLower(parsed.Scheme)
		if scheme != "http" && scheme != "https" {
			return nil, &apiError{Message: fmt.Sprintf("unsupported scheme %q: use http or https", parsed.Scheme)}
		}
		address = parsed.Host
		embeddedPath = strings.TrimSuffix(parsed.Path, "/")
	}
	address = strings.TrimSuffix(strings.TrimSpace(address), "/")
	// A path pasted into the address field ("nacos.example.com/nacos") would
	// otherwise be mistaken for part of the host name.
	if index := strings.Index(address, "/"); index >= 0 {
		if embeddedPath == "" {
			embeddedPath = strings.TrimSuffix(address[index:], "/")
		}
		address = address[:index]
	}
	if address == "" {
		return nil, &apiError{Message: "connection has no server address"}
	}
	if port > 0 && !addressHasPort(address) {
		// JoinHostPort re-adds the brackets a bare IPv6 literal needs.
		bare := strings.TrimSuffix(strings.TrimPrefix(address, "["), "]")
		address = net.JoinHostPort(bare, strconv.Itoa(port))
	}

	path := strings.TrimSpace(contextPath)
	if path == "" {
		path = embeddedPath
	}
	if path == "" {
		path = defaultContextPath
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	path = strings.TrimSuffix(path, "/")

	return &nacosClient{
		baseURL:  scheme + "://" + address + path,
		username: strings.TrimSpace(username),
		password: password,
		http:     &http.Client{Timeout: requestTimeout},
	}, nil
}

// addressHasPort reports whether the address already carries a port. A bare
// IPv6 literal ("[::1]") has none and still needs one appended.
func addressHasPort(address string) bool {
	_, _, err := net.SplitHostPort(address)
	return err == nil
}

// --- authentication ---------------------------------------------------------

// ensureToken returns a valid access token, logging in when needed.
// It returns "" when the connection is configured without credentials.
func (c *nacosClient) ensureToken(ctx context.Context) (string, error) {
	if c.username == "" {
		return "", nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.token != "" && time.Now().Before(c.tokenExpiry) {
		return c.token, nil
	}
	form := url.Values{}
	form.Set("username", c.username)
	form.Set("password", c.password)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.baseURL+"/v1/auth/login", strings.NewReader(form.Encode()))
	if err != nil {
		return "", &apiError{Message: err.Error()}
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := c.http.Do(req)
	if err != nil {
		return "", &apiError{Message: fmt.Sprintf("login request failed: %s", transportError(err))}
	}
	defer resp.Body.Close()
	// The login body is never echoed into an error message: it carries the token.
	body, truncated, err := readBounded(resp.Body, maxResponseBytes)
	if err != nil {
		return "", &apiError{Message: fmt.Sprintf("reading login response failed: %s", transportError(err))}
	}
	if truncated {
		return "", &apiError{Message: "login response is unexpectedly large"}
	}
	if resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusUnauthorized {
		return "", &apiError{Status: resp.StatusCode, Message: "login rejected: wrong username or password"}
	}
	if resp.StatusCode != http.StatusOK {
		return "", &apiError{Status: resp.StatusCode,
			Message: fmt.Sprintf("login failed with HTTP %d", resp.StatusCode)}
	}
	var payload struct {
		AccessToken string `json:"accessToken"`
		TokenTTL    int    `json:"tokenTtl"`
	}
	if err := json.Unmarshal(body, &payload); err != nil || payload.AccessToken == "" {
		return "", &apiError{Message: "login response did not contain an access token; is authentication really enabled?"}
	}
	ttl := time.Duration(payload.TokenTTL) * time.Second
	if ttl <= tokenRefreshMargin {
		// Some Nacos builds answer tokenTtl=0 (or a value shorter than our
		// safety margin). Assume the documented default instead of treating the
		// token as instantly stale; a wrong guess costs one retry.
		ttl = time.Hour
	}
	c.token = payload.AccessToken
	c.tokenExpiry = time.Now().Add(ttl - tokenRefreshMargin)
	return c.token, nil
}

// invalidateToken drops the cached token so the next call logs in again.
func (c *nacosClient) invalidateToken() {
	c.mu.Lock()
	c.token = ""
	c.tokenExpiry = time.Time{}
	c.mu.Unlock()
}

// --- transport --------------------------------------------------------------

// get performs an authenticated GET and retries once with a fresh token when
// the server rejects the cached one. 401 and 403 both retry because Nacos
// answers an expired token with either; when the rejection is really an ACL
// denial the retry only costs one extra login round trip.
func (c *nacosClient) get(ctx context.Context, path string, params url.Values) ([]byte, int, error) {
	body, status, err := c.getOnce(ctx, path, params)
	if err != nil {
		return body, status, err
	}
	if (status == http.StatusUnauthorized || status == http.StatusForbidden) && c.username != "" {
		c.invalidateToken()
		body, status, err = c.getOnce(ctx, path, params)
	}
	return body, status, err
}

func (c *nacosClient) getOnce(ctx context.Context, path string, params url.Values) ([]byte, int, error) {
	token, err := c.ensureToken(ctx)
	if err != nil {
		return nil, 0, err
	}
	query := url.Values{}
	for key, values := range params {
		for _, value := range values {
			query.Add(key, value)
		}
	}
	if token != "" {
		query.Set("accessToken", token)
	}
	requestURL := c.baseURL + path
	if encoded := query.Encode(); encoded != "" {
		requestURL += "?" + encoded
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return nil, 0, &apiError{Message: err.Error()}
	}
	resp, err := c.http.Do(req)
	if err != nil {
		// transportError strips the *url.Error wrapper, whose message embeds the
		// full request URL — including the accessToken query parameter. This
		// message reaches the AI model and the UI, so the token must not get in.
		return nil, 0, &apiError{Message: fmt.Sprintf("request to %s failed: %s", c.baseURL, transportError(err))}
	}
	defer resp.Body.Close()
	body, truncated, err := readBounded(resp.Body, maxResponseBytes)
	if err != nil {
		return nil, resp.StatusCode, &apiError{Status: resp.StatusCode,
			Message: fmt.Sprintf("reading response failed: %s", transportError(err))}
	}
	if truncated {
		return nil, resp.StatusCode, &apiError{Status: resp.StatusCode,
			Message: fmt.Sprintf("upstream response is larger than %d bytes; refusing to hand back truncated data", maxResponseBytes)}
	}
	if resp.StatusCode != http.StatusOK {
		logUpstream(fmt.Sprintf("GET %s -> HTTP %d: %s", path, resp.StatusCode, truncate(string(body), 300)))
	}
	return body, resp.StatusCode, nil
}

// transportError unwraps *url.Error so the returned text cannot leak the
// request URL — and with it the accessToken query parameter.
func transportError(err error) string {
	var urlErr *url.Error
	if errors.As(err, &urlErr) && urlErr.Err != nil {
		return urlErr.Err.Error()
	}
	return err.Error()
}

// readBounded reads at most limit bytes and reports whether the stream was
// longer, so callers can fail loudly instead of returning cut-off data.
func readBounded(reader io.Reader, limit int64) ([]byte, bool, error) {
	body, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return nil, false, err
	}
	if int64(len(body)) > limit {
		return body[:limit], true, nil
	}
	return body, false, nil
}

// logUpstream writes upstream error detail to stderr, which the DBX host keeps
// in the plugin log. Response bodies stay out of error messages so they never
// end up in the AI conversation.
func logUpstream(detail string) {
	fmt.Fprintf(os.Stderr, "[nacos-toolkit] %s\n", detail)
}

// --- configuration APIs -----------------------------------------------------

// configPage is the paginated result of a config search.
type configPage struct {
	TotalCount     int          `json:"totalCount"`
	PageNumber     int          `json:"pageNumber"`
	PagesAvailable int          `json:"pagesAvailable"`
	PageItems      []configItem `json:"pageItems"`
}

// configItem is one config metadata record. Content is intentionally omitted
// from list results; agents fetch full content with getConfig.
type configItem struct {
	DataID  string `json:"dataId"`
	Group   string `json:"group"`
	Tenant  string `json:"tenant"`
	AppName string `json:"appName"`
	Type    string `json:"type"`
	Md5     string `json:"md5"`
}

// readConfig 读取单条配置。exists 为 false 表示该配置不存在（404），
// 与网络或权限错误区分开——跨环境对比需要把"缺少配置"当作一种正常状态处理。
func (c *nacosClient) readConfig(ctx context.Context, namespace, dataID, group string) (content string, exists bool, err error) {
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

// getConfig returns the full content of one config.
func (c *nacosClient) getConfig(ctx context.Context, namespace, dataID, group string) (string, error) {
	content, exists, err := c.readConfig(ctx, namespace, dataID, group)
	if err != nil {
		return "", err
	}
	if !exists {
		return "", &apiError{Status: http.StatusNotFound,
			Message: fmt.Sprintf("config not found: dataId=%s group=%s namespace=%q", dataID, group, namespace)}
	}
	return content, nil
}

// searchConfigs lists config metadata. Empty patterns match everything; the
// Nacos blur search treats "*" as a wildcard.
func (c *nacosClient) searchConfigs(ctx context.Context, namespace, dataIDPattern, groupPattern string, pageNo, pageSize int) (*configPage, error) {
	pageNo, pageSize = normalizePaging(pageNo, pageSize)
	if strings.TrimSpace(dataIDPattern) == "" {
		dataIDPattern = "*"
	} else if !strings.Contains(dataIDPattern, "*") {
		dataIDPattern = "*" + dataIDPattern + "*"
	}
	if strings.TrimSpace(groupPattern) == "" {
		groupPattern = "*"
	} else if !strings.Contains(groupPattern, "*") {
		groupPattern = "*" + groupPattern + "*"
	}
	params := url.Values{}
	params.Set("search", "blur")
	params.Set("dataId", dataIDPattern)
	params.Set("group", groupPattern)
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

// normalizePaging clamps caller supplied paging values. pageSize is clamped to
// the Nacos maximum rather than silently reset to the default, so a caller that
// asks for more than allowed still gets as much as the server will serve.
func normalizePaging(pageNo, pageSize int) (int, int) {
	if pageNo <= 0 {
		pageNo = 1
	}
	switch {
	case pageSize <= 0:
		pageSize = 20
	case pageSize > maxPageSize:
		pageSize = maxPageSize
	}
	return pageNo, pageSize
}

// configRevision 是一条配置变更历史记录（对齐 /v1/cs/history 响应字段）。
type configRevision struct {
	ID          string `json:"id"`
	DataID      string `json:"dataId"`
	Group       string `json:"group"`
	Tenant      string `json:"tenant"`
	AppName     string `json:"appName"`
	Md5         string `json:"md5"`
	SrcIP       string `json:"srcIp"`
	SrcUser     string `json:"srcUser"`
	OpType      string `json:"opType"` // I 新增 / U 更新 / D 删除
	CreatedTime int64  `json:"createdTime"`
}

// configRevisionDetail 是单个历史版本的详情（含配置内容）。
type configRevisionDetail struct {
	configRevision
	Content string `json:"content"`
}

// configHistoryPage 是历史列表的分页响应。
type configHistoryPage struct {
	TotalCount     int              `json:"totalCount"`
	PageNumber     int              `json:"pageNumber"`
	PagesAvailable int              `json:"pagesAvailable"`
	PageItems      []configRevision `json:"pageItems"`
}

// listConfigHistory 按 dataId 列出历史版本，最新在前。
func (c *nacosClient) listConfigHistory(ctx context.Context, namespace, dataID, group string, pageNo, pageSize int) (*configHistoryPage, error) {
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

// getConfigRevision 读取单个历史版本的完整内容（按历史记录 id）。
func (c *nacosClient) getConfigRevision(ctx context.Context, namespace, dataID, group, historyID string) (*configRevisionDetail, error) {
	if strings.TrimSpace(historyID) == "" {
		return nil, &apiError{Message: "historyId is required"}
	}
	params := url.Values{}
	params.Set("search", "accurate")
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
		return &detail, nil
	case http.StatusNotFound:
		return nil, &apiError{Status: status,
			Message: fmt.Sprintf("config revision not found: dataId=%s group=%s namespace=%q historyId=%s", dataID, group, namespace, historyID)}
	default:
		return nil, &apiError{Status: status,
			Message: fmt.Sprintf("reading config revision failed with HTTP %d", status)}
	}
}

// namespace is one Nacos namespace entry.
type namespace struct {
	NamespaceID   string `json:"namespace"`
	ShowName      string `json:"namespaceShowName"`
	ConfigCount   int    `json:"configCount"`
	Quota         int    `json:"quota"`
	NamespaceType int    `json:"type"`
}

// listNamespaces returns every namespace defined on the server.
func (c *nacosClient) listNamespaces(ctx context.Context) ([]namespace, error) {
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

// servicePage is the paginated result of a service list call.
type servicePage struct {
	Count   int      `json:"count"`
	Domains []string `json:"doms"`
	Also    []string `json:"serviceList"` // some versions use this key
}

// services returns the merged service name list across known response shapes.
func (p *servicePage) services() []string {
	out := make([]string, 0, len(p.Domains)+len(p.Also))
	out = append(out, p.Domains...)
	out = append(out, p.Also...)
	return out
}

// listServices lists registered service names in a namespace.
func (c *nacosClient) listServices(ctx context.Context, namespace, groupName string, pageNo, pageSize int) (*servicePage, error) {
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

// instanceHost 是服务实例的关键字段（对齐 Nacos /v1/ns/instance/list 响应）。
type instanceHost struct {
	IP          string         `json:"ip"`
	Port        int            `json:"port"`
	Healthy     bool           `json:"healthy"`
	Enabled     bool           `json:"enabled"`
	Valid       bool           `json:"valid"`
	Weight      float64        `json:"weight"`
	ClusterName string         `json:"clusterName"`
	Metadata    map[string]any `json:"metadata"`
}

// instanceList 是实例列表接口的响应。
type instanceList struct {
	// Name 形如 "DEFAULT_GROUP@@service-name"。
	Name      string         `json:"name"`
	Hosts     []instanceHost `json:"hosts"`
	Reachable bool           `json:"reachable"`
}

// listInstances 查询某个服务在指定命名空间下的实例列表。
func (c *nacosClient) listInstances(ctx context.Context, namespace, groupName, serviceName string) (*instanceList, error) {
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

// --- health -----------------------------------------------------------------

// health probes the server and returns a short status description.
// Nacos 2.x+ exposes /v1/console/health/readiness; 1.x only has server/state.
func (c *nacosClient) health(ctx context.Context) (string, error) {
	body, status, err := c.get(ctx, "/v1/console/health/readiness", url.Values{})
	if err != nil {
		return "", err
	}
	if status == http.StatusOK {
		return fmt.Sprintf("ready: %s", truncate(string(body), 120)), nil
	}
	body, state, err := c.get(ctx, "/v1/console/server/state", url.Values{})
	if err != nil {
		return "", err
	}
	if state != http.StatusOK {
		return "", &apiError{Status: state,
			Message: fmt.Sprintf("server did not answer health checks (HTTP %d)", state)}
	}
	var payload struct {
		Version string `json:"version"`
	}
	_ = json.Unmarshal(body, &payload)
	if payload.Version != "" {
		return fmt.Sprintf("up (Nacos %s)", payload.Version), nil
	}
	return "up", nil
}

// truncate shortens strings embedded into messages. The limit counts runes so
// multi-byte text (Chinese error messages) is never cut mid-character.
func truncate(value string, limit int) string {
	value = strings.TrimSpace(value)
	runes := []rune(value)
	if len(runes) <= limit {
		return value
	}
	return string(runes[:limit]) + "…"
}

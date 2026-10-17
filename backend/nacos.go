package main

// nacos.go —— 客户端基础设施：地址解析、协议栈探测（v1 / v3）、鉴权与
// HTTP 传输，以及十一个读取能力的分发入口。
//
// 插件支持两套协议栈，连接时自动探测：
//
//	v1：Nacos 1.x / 2.x 的 Open API（3.x 搭配 legacy adapter 时也走这条）；
//	v3：Nacos 3.x 原生 API —— client 接口在服务端口（/v3/client/**），
//	    console 接口在独立的控制台端口（/v3/console/**，除健康检查外强制
//	    Bearer 鉴权，即使服务端关闭了鉴权开关）。
//
// 具体实现分别见 v1.go 与 v3.go。仅使用标准库，零第三方依赖。

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

const (
	// defaultGroup is the Nacos default config/service group.
	defaultGroup = "DEFAULT_GROUP"

	// defaultContextPath is the standard Nacos context path.
	defaultContextPath = "/nacos"

	// defaultConsolePort 是 Nacos 3.x 控制台（console API）的默认端口。
	// 连接未显式配置 console 端口时，探测阶段会在该端口尝试一次。
	defaultConsolePort = 8080

	// requestTimeout bounds every upstream HTTP call.
	requestTimeout = 15 * time.Second

	// probeTimeout bounds each protocol-detection probe.
	probeTimeout = 5 * time.Second

	// maxResponseBytes caps how much of an upstream response is read. It is
	// deliberately conservative: a larger response is reported as an error
	// instead of being silently truncated, and the encoded tool result has to
	// stay below the sidecar protocol frame limit.
	maxResponseBytes = 2 << 20

	// tokenRefreshMargin re-logs in slightly before the token expires.
	tokenRefreshMargin = 60 * time.Second

	// maxPageSize 是列表接口接受的最大分页大小。取值与 Nacos 3.x console
	// 历史接口的服务端上限（500）一致；1.x/2.x 对更大的值也不再报错，
	// 这里统一按 500 收敛，避免生成超出单次工具结果预算的响应。
	maxPageSize = 500
)

// apiError is a structured error from the Nacos API or transport layer.
type apiError struct {
	Status  int    // HTTP status, 0 for transport errors
	Message string // human readable detail
}

func (e *apiError) Error() string { return e.Message }

// --- 协议栈 ---------------------------------------------------------------

// apiFamily 标识客户端使用的 Nacos API 协议栈。
type apiFamily int

const (
	// familyAuto 表示尚未探测，按连接配置在 v3/v1 之间自动判定。
	familyAuto apiFamily = iota
	// familyV1 是 Nacos 1.x/2.x Open API 栈。
	familyV1
	// familyV3 是 Nacos 3.x 原生 API 栈。
	familyV3
)

func (f apiFamily) name() string {
	switch f {
	case familyV1:
		return "v1"
	case familyV3:
		return "v3"
	default:
		return "auto"
	}
}

// parseAPIVersion 解析连接上的 api_version 覆盖值（auto / v1 / v3）。
func parseAPIVersion(raw string) apiFamily {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "v1", "1":
		return familyV1
	case "v3", "3":
		return familyV3
	default:
		return familyAuto
	}
}

// authStyle 选择请求携带凭据的方式。
type authStyle int

const (
	// authNone：不带凭据（探测类请求）。
	authNone authStyle = iota
	// authQuery：v1 方式，accessToken 查询参数。
	authQuery
	// authBearer：v3 方式，Authorization: Bearer 头。
	authBearer
)

// nacosClient is a minimal Nacos Open API client covering both the v1 and the
// v3 protocol stacks.
type nacosClient struct {
	// serverBase 是服务端口基址（含 context path），如 http://host:8848/nacos。
	serverBase string
	// consoleBase 是 Nacos 3.x 控制台基址（无 context path），如 http://host:8080。
	consoleBase string
	// consolePortConfigured 记录 console 端口是否来自连接配置：未配置时
	// 探测失败的错误信息要提示用户补充端口。
	consolePortConfigured bool

	// familyPref 来自连接的显式覆盖；family 是探测结果（二者都由 familyMu 保护）。
	familyPref apiFamily
	familyMu   sync.Mutex
	family     apiFamily

	username string
	password string
	http     *http.Client

	mu          sync.Mutex
	token       string
	tokenExpiry time.Time
}

// clientOptions 是新客户端的全部输入。
type clientOptions struct {
	host        string
	port        int
	contextPath string
	consolePort int    // 0 表示未配置（探测时按默认 8080 尝试）
	apiVersion  string // 空 / auto / v1 / v3
	username    string
	password    string
}

// newNacosClient builds a client from connection fields.
//
// host accepts "nacos.example.com", "nacos.example.com:8848", "10.0.0.1/nacos"
// or a full URL such as "https://nacos.example.com/nacos". The scheme selects
// http/https, a path embedded in host becomes the fallback context path, and
// port is appended unless the address already carries one (IPv6 aware).
func newNacosClient(opts clientOptions) (*nacosClient, error) {
	raw := strings.TrimSpace(opts.host)
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
	if opts.port > 0 && !addressHasPort(address) {
		// JoinHostPort re-adds the brackets a bare IPv6 literal needs.
		bare := strings.TrimSuffix(strings.TrimPrefix(address, "["), "]")
		address = net.JoinHostPort(bare, strconv.Itoa(opts.port))
	}

	path := strings.TrimSpace(opts.contextPath)
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

	// console 基址：与 server 同主机、独立端口、无 context path。
	consolePort := opts.consolePort
	if consolePort <= 0 {
		consolePort = defaultConsolePort
	}
	consoleHost := address
	if host, _, err := net.SplitHostPort(address); err == nil {
		consoleHost = host
	}
	consoleHost = strings.TrimSuffix(strings.TrimPrefix(consoleHost, "["), "]")

	return &nacosClient{
		serverBase:            scheme + "://" + address + path,
		consoleBase:           scheme + "://" + net.JoinHostPort(consoleHost, strconv.Itoa(consolePort)),
		consolePortConfigured: opts.consolePort > 0,
		familyPref:            parseAPIVersion(opts.apiVersion),
		username:              strings.TrimSpace(opts.username),
		password:              opts.password,
		http:                  &http.Client{Timeout: requestTimeout},
	}, nil
}

// addressHasPort reports whether the address already carries a port. A bare
// IPv6 literal ("[::1]") has none and still needs one appended.
func addressHasPort(address string) bool {
	_, _, err := net.SplitHostPort(address)
	return err == nil
}

// resolveFamily 决定使用哪套协议栈：连接显式指定时直接采用；否则先探测
// v3 控制台健康端点（Nacos 3.x），失败再退回 v1 健康端点（1.x/2.x 以及
// 搭配 legacy adapter 的 3.x）。探测结果缓存，失败不缓存以便重试。
func (c *nacosClient) resolveFamily(ctx context.Context) (apiFamily, error) {
	if c.familyPref != familyAuto {
		return c.familyPref, nil
	}
	c.familyMu.Lock()
	defer c.familyMu.Unlock()
	if c.family != familyAuto {
		return c.family, nil
	}
	if c.checkV3Readiness(ctx) {
		c.family = familyV3
		return c.family, nil
	}
	if _, ok := c.v1HealthStatus(ctx); ok {
		c.family = familyV1
		return c.family, nil
	}
	return familyAuto, &apiError{Message: c.detectionError()}
}

// resolvedFamilyName 返回已解析的协议栈名称（用于诊断输出，如 ping）。
func (c *nacosClient) resolvedFamilyName() string {
	c.familyMu.Lock()
	defer c.familyMu.Unlock()
	if c.family != familyAuto {
		return c.family.name()
	}
	return c.familyPref.name()
}

func (c *nacosClient) detectionError() string {
	hint := ""
	if !c.consolePortConfigured {
		hint = fmt.Sprintf("; if this is Nacos 3.x, set the console port (default %d)", defaultConsolePort)
	}
	return fmt.Sprintf("cannot reach Nacos: v3 console health (%s) and v1 health (%s) both failed%s",
		c.consoleBase, c.serverBase, hint)
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

	family, err := c.resolveFamily(ctx)
	if err != nil {
		return "", err
	}
	// v3 的登录端点（/v3/auth/user/login）与 v1 登录端点（/v1/auth/login，
	// 3.x 里同为原生保留）都挂在服务端口上，响应字段一致。
	loginPath := "/v1/auth/login"
	if family == familyV3 {
		loginPath = "/v3/auth/user/login"
	}

	form := url.Values{}
	form.Set("username", c.username)
	form.Set("password", c.password)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.serverBase+loginPath, strings.NewReader(form.Encode()))
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
		message := "login rejected: wrong username or password"
		if family == familyV3 {
			// Nacos 3.x 的 console API 始终要求登录；无鉴权实例也需要先
			// 初始化管理员账号，这里把可操作提示带上。
			message += " (Nacos 3.x console APIs always require an account; if this server has no admin user yet, initialize one via POST /v3/auth/user/admin)"
		}
		return "", &apiError{Status: resp.StatusCode, Message: message}
	}
	if resp.StatusCode != http.StatusOK {
		return "", &apiError{Status: resp.StatusCode,
			Message: fmt.Sprintf("login failed with HTTP %d", resp.StatusCode)}
	}
	var payload struct {
		AccessToken string `json:"accessToken"`
		TokenTTL    int    `json:"tokenTtl"`
		Code        *int   `json:"code"`
		Message     string `json:"message"`
	}
	if err := json.Unmarshal(body, &payload); err != nil || payload.AccessToken == "" {
		if payload.Code != nil && *payload.Code != 0 {
			return "", &apiError{Message: fmt.Sprintf("login failed: %s (code %d)", payload.Message, *payload.Code)}
		}
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

// fetch performs one GET against base, attaching credentials per style.
func (c *nacosClient) fetch(ctx context.Context, base, path string, params url.Values, style authStyle) ([]byte, int, error) {
	token := ""
	if style != authNone {
		var err error
		token, err = c.ensureToken(ctx)
		if err != nil {
			return nil, 0, err
		}
	}
	query := url.Values{}
	for key, values := range params {
		for _, value := range values {
			query.Add(key, value)
		}
	}
	if style == authQuery && token != "" {
		query.Set("accessToken", token)
	}
	requestURL := base + path
	if encoded := query.Encode(); encoded != "" {
		requestURL += "?" + encoded
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return nil, 0, &apiError{Message: err.Error()}
	}
	if style == authBearer && token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		// transportError strips the *url.Error wrapper, whose message embeds the
		// full request URL — including the accessToken query parameter. This
		// message reaches the AI model and the UI, so the token must not get in.
		return nil, 0, &apiError{Message: fmt.Sprintf("request to %s failed: %s", base, transportError(err))}
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

// fetchAuthed performs an authenticated GET and retries once with a fresh
// token when the server rejects the cached one. 401 and 403 both retry because
// Nacos answers an expired token with either; when the rejection is really an
// ACL denial the retry only costs one extra login round trip.
func (c *nacosClient) fetchAuthed(ctx context.Context, base, path string, params url.Values, style authStyle) ([]byte, int, error) {
	body, status, err := c.fetch(ctx, base, path, params, style)
	if err != nil {
		return body, status, err
	}
	if (status == http.StatusUnauthorized || status == http.StatusForbidden) && c.username != "" {
		c.invalidateToken()
		body, status, err = c.fetch(ctx, base, path, params, style)
	}
	return body, status, err
}

// get 是 v1 栈对 serverBase 的带鉴权 GET（accessToken 查询参数）。
func (c *nacosClient) get(ctx context.Context, path string, params url.Values) ([]byte, int, error) {
	return c.fetchAuthed(ctx, c.serverBase, path, params, authQuery)
}

// getV3 是 v3 栈的带鉴权 GET（Bearer 头），base 由调用方选择
// serverBase（client 接口）或 consoleBase（console 接口）。
func (c *nacosClient) getV3(ctx context.Context, base, path string, params url.Values) ([]byte, int, error) {
	return c.fetchAuthed(ctx, base, path, params, authBearer)
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

// --- shared response types --------------------------------------------------

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

// flexibleTime 表示来自 Nacos 的时间戳，兼容接口的多种返回形态：
// 毫秒数字（旧版）、毫秒数字字符串、ISO8601 字符串（2.x 将 java.sql.Timestamp
// 序列化后的形态，例如 "2026-10-17T00:15:18.031+08:00"）。
// 统一按毫秒存储；无法解析时按 0 处理，避免单条脏数据使整个列表解析失败。
type flexibleTime int64

func (t *flexibleTime) UnmarshalJSON(data []byte) error {
	raw := strings.TrimSpace(string(data))
	switch {
	case raw == "" || raw == "null":
		*t = 0
	case raw[0] == '"':
		unquoted, err := strconv.Unquote(raw)
		if err != nil {
			*t = 0
			return nil
		}
		*t = parseTimeValue(unquoted)
	default:
		if millis, err := strconv.ParseInt(raw, 10, 64); err == nil {
			*t = flexibleTime(millis)
		} else {
			*t = 0
		}
	}
	return nil
}

// parseTimeValue 解析字符串形态的时间值：纯毫秒数或 ISO8601 时间。
func parseTimeValue(value string) flexibleTime {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if millis, err := strconv.ParseInt(value, 10, 64); err == nil {
		return flexibleTime(millis)
	}
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return flexibleTime(parsed.UnixMilli())
	}
	return 0
}

// flexibleString 兼容字符串与数字两种 JSON 形态。历史记录的 id 字段即是这种
// 情况：2.x/3.x 用 ToStringSerializer 序列化为字符串，更早的实现则直接输出数字。
type flexibleString string

func (s *flexibleString) UnmarshalJSON(data []byte) error {
	raw := strings.TrimSpace(string(data))
	if raw == "" || raw == "null" {
		*s = ""
		return nil
	}
	if raw[0] == '"' {
		var value string
		if err := json.Unmarshal(data, &value); err != nil {
			return err
		}
		*s = flexibleString(value)
		return nil
	}
	*s = flexibleString(raw)
	return nil
}

// configRevision 是一条配置变更历史记录。
// 字段兼容性：不同版本的 createdTime 可能是毫秒数字或 ISO8601 字符串；
// 2.x/3.x 起新增 lastModifiedTime（v3 console 里叫 modifyTime，由 v3 栈适配），
// 且部分存储形态（derby）下 createdTime 是占位值（2010-05-05），
// 真实变更时间以 lastModifiedTime 为准。
type configRevision struct {
	ID               flexibleString `json:"id"`
	DataID           string         `json:"dataId"`
	Group            string         `json:"group"`
	Tenant           string         `json:"tenant"`
	AppName          string         `json:"appName"`
	Md5              string         `json:"md5"`
	SrcIP            string         `json:"srcIp"`
	SrcUser          string         `json:"srcUser"`
	OpType           string         `json:"opType"` // I 新增 / U 更新 / D 删除
	CreatedTime      flexibleTime   `json:"createdTime"`
	LastModifiedTime flexibleTime   `json:"lastModifiedTime"`
}

// revisionTime 返回该历史条目的展示时间（毫秒）。
// lastModifiedTime 有效时优先，否则退回 createdTime（旧版本只有该字段）。
func (r configRevision) revisionTime() int64 {
	if r.LastModifiedTime > 0 {
		return int64(r.LastModifiedTime)
	}
	return int64(r.CreatedTime)
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

// namespace is one Nacos namespace entry.
type namespace struct {
	NamespaceID   string `json:"namespace"`
	ShowName      string `json:"namespaceShowName"`
	ConfigCount   int    `json:"configCount"`
	Quota         int    `json:"quota"`
	NamespaceType int    `json:"type"`
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

// instanceHost 是服务实例的关键字段。
// Valid 只在 v1 响应中出现（v3 的 Instance 对象没有该字段），因此用指针
// 区分“服务端给了 false”与“服务端没这个字段”。
type instanceHost struct {
	IP          string         `json:"ip"`
	Port        int            `json:"port"`
	Healthy     bool           `json:"healthy"`
	Enabled     bool           `json:"enabled"`
	Valid       *bool          `json:"valid"`
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

// --- read capability dispatch ----------------------------------------------

// readConfig 读取单条配置。exists 为 false 表示该配置不存在（v1 为 404，
// v3 client 接口为 code 20004），与网络或权限错误区分开——跨环境对比需要把
// “缺少配置”当作一种正常状态处理。
func (c *nacosClient) readConfig(ctx context.Context, namespace, dataID, group string) (content string, exists bool, err error) {
	family, err := c.resolveFamily(ctx)
	if err != nil {
		return "", false, err
	}
	if family == familyV3 {
		return c.v3ReadConfig(ctx, namespace, dataID, group)
	}
	return c.v1ReadConfig(ctx, namespace, dataID, group)
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
// blur search treats "*" as a wildcard on both protocol stacks.
func (c *nacosClient) searchConfigs(ctx context.Context, namespace, dataIDPattern, groupPattern string, pageNo, pageSize int) (*configPage, error) {
	family, err := c.resolveFamily(ctx)
	if err != nil {
		return nil, err
	}
	if family == familyV3 {
		return c.v3SearchConfigs(ctx, namespace, dataIDPattern, groupPattern, pageNo, pageSize)
	}
	return c.v1SearchConfigs(ctx, namespace, dataIDPattern, groupPattern, pageNo, pageSize)
}

// searchConfigsByContent 是 v3 控制台的正文搜索入口（Nacos 1.x/2.x 没有
// 原生正文搜索，调用方会改走“列表 + 抓取”的回退路径）。
func (c *nacosClient) searchConfigsByContent(ctx context.Context, namespace, dataIDPattern, groupPattern, keyword string, pageNo, pageSize int) (*configPage, error) {
	family, err := c.resolveFamily(ctx)
	if err != nil {
		return nil, err
	}
	if family == familyV3 {
		return c.v3SearchConfigsByContent(ctx, namespace, dataIDPattern, groupPattern, keyword, pageNo, pageSize)
	}
	return nil, &apiError{Message: "native config content search is not available on the v1 protocol stack"}
}

// serverState 读取服务端基础状态（版本/模式/鉴权开关等），供连接体检使用。
// 两个协议栈上的状态端点都是匿名可访问的，因此这里不带凭据。
func (c *nacosClient) serverState(ctx context.Context) (map[string]any, error) {
	family, err := c.resolveFamily(ctx)
	if err != nil {
		return nil, err
	}
	if family == familyV3 {
		return c.v3ServerState(ctx)
	}
	return c.v1ServerState(ctx)
}

// listConfigHistory 按 dataId 列出历史版本，最新在前。
func (c *nacosClient) listConfigHistory(ctx context.Context, namespace, dataID, group string, pageNo, pageSize int) (*configHistoryPage, error) {
	family, err := c.resolveFamily(ctx)
	if err != nil {
		return nil, err
	}
	if family == familyV3 {
		return c.v3ListConfigHistory(ctx, namespace, dataID, group, pageNo, pageSize)
	}
	return c.v1ListConfigHistory(ctx, namespace, dataID, group, pageNo, pageSize)
}

// getConfigRevision 读取单个历史版本的完整内容（按历史记录 id）。
func (c *nacosClient) getConfigRevision(ctx context.Context, namespace, dataID, group, historyID string) (*configRevisionDetail, error) {
	family, err := c.resolveFamily(ctx)
	if err != nil {
		return nil, err
	}
	if family == familyV3 {
		return c.v3GetConfigRevision(ctx, namespace, dataID, group, historyID)
	}
	return c.v1GetConfigRevision(ctx, namespace, dataID, group, historyID)
}

// listNamespaces returns every namespace defined on the server.
func (c *nacosClient) listNamespaces(ctx context.Context) ([]namespace, error) {
	family, err := c.resolveFamily(ctx)
	if err != nil {
		return nil, err
	}
	if family == familyV3 {
		return c.v3ListNamespaces(ctx)
	}
	return c.v1ListNamespaces(ctx)
}

// listServices lists registered service names in a namespace.
func (c *nacosClient) listServices(ctx context.Context, namespace, groupName string, pageNo, pageSize int) (*servicePage, error) {
	family, err := c.resolveFamily(ctx)
	if err != nil {
		return nil, err
	}
	if family == familyV3 {
		return c.v3ListServices(ctx, namespace, groupName, pageNo, pageSize)
	}
	return c.v1ListServices(ctx, namespace, groupName, pageNo, pageSize)
}

// listInstances 查询某个服务在指定命名空间下的实例列表。
func (c *nacosClient) listInstances(ctx context.Context, namespace, groupName, serviceName, cluster string) (*instanceList, error) {
	family, err := c.resolveFamily(ctx)
	if err != nil {
		return nil, err
	}
	if family == familyV3 {
		return c.v3ListInstances(ctx, namespace, groupName, serviceName, cluster)
	}
	return c.v1ListInstances(ctx, namespace, groupName, serviceName, cluster)
}

// health probes the server and returns a short status description.
func (c *nacosClient) health(ctx context.Context) (string, error) {
	family, err := c.resolveFamily(ctx)
	if err != nil {
		return "", err
	}
	if family == familyV3 {
		return c.v3Health(ctx)
	}
	status, ok := c.v1HealthStatus(ctx)
	if !ok {
		return "", &apiError{Message: "server did not answer health checks"}
	}
	return status, nil
}

// --- helpers ----------------------------------------------------------------

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

// blurPattern 把用户输入的关键词转换为 Nacos blur 搜索模式：
// 空输入匹配全部（*）；不含通配符时按包含匹配（*kw*）；已带 * 的原样保留。
// v1 与 v3 的 blur 搜索对 * 的语义一致（实测确认）。
func blurPattern(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "*"
	}
	if !strings.Contains(value, "*") {
		return "*" + value + "*"
	}
	return value
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

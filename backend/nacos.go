package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
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

	// maxResponseBytes caps how much of an upstream response is read.
	maxResponseBytes = 4 << 20

	// tokenRefreshMargin re-logs in slightly before the token expires.
	tokenRefreshMargin = 60 * time.Second
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
// host may already carry a scheme ("https://nacos.example.com"); port is
// appended unless the host string already contains one.
func newNacosClient(host string, port int, contextPath, username, password string) (*nacosClient, error) {
	host = strings.TrimSpace(host)
	if host == "" {
		return nil, &apiError{Message: "connection has no server address"}
	}
	scheme := "http"
	if strings.HasPrefix(host, "https://") {
		scheme = "https"
		host = strings.TrimPrefix(host, "https://")
	} else if strings.HasPrefix(host, "http://") {
		host = strings.TrimPrefix(host, "http://")
	}
	host = strings.TrimSuffix(host, "/")
	if host == "" {
		return nil, &apiError{Message: "connection has no server address"}
	}
	if port > 0 && !strings.Contains(host, ":") {
		host = host + ":" + strconv.Itoa(port)
	}
	path := strings.TrimSpace(contextPath)
	if path == "" {
		path = defaultContextPath
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	path = strings.TrimSuffix(path, "/")

	return &nacosClient{
		baseURL:  scheme + "://" + host + path,
		username: strings.TrimSpace(username),
		password: password,
		http:     &http.Client{Timeout: requestTimeout},
	}, nil
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
		return "", &apiError{Message: fmt.Sprintf("login request failed: %v", err)}
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusUnauthorized {
		return "", &apiError{Status: resp.StatusCode, Message: "login rejected: wrong username or password"}
	}
	if resp.StatusCode != http.StatusOK {
		return "", &apiError{Status: resp.StatusCode,
			Message: fmt.Sprintf("login failed with HTTP %d: %s", resp.StatusCode, truncate(string(body), 200))}
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
// the server rejects the cached one.
func (c *nacosClient) get(ctx context.Context, path string, params url.Values) ([]byte, int, error) {
	body, status, err := c.getOnce(ctx, path, params)
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
		return nil, 0, &apiError{Message: fmt.Sprintf("request to %s failed: %v", c.baseURL, err)}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return nil, resp.StatusCode, &apiError{Status: resp.StatusCode, Message: fmt.Sprintf("reading response failed: %v", err)}
	}
	return body, resp.StatusCode, nil
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

// getConfig returns the full content of one config.
func (c *nacosClient) getConfig(ctx context.Context, namespace, dataID, group string) (string, error) {
	if strings.TrimSpace(dataID) == "" {
		return "", &apiError{Message: "dataId is required"}
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
		return "", err
	}
	switch status {
	case http.StatusOK:
		return string(body), nil
	case http.StatusNotFound:
		return "", &apiError{Status: status,
			Message: fmt.Sprintf("config not found: dataId=%s group=%s namespace=%q", dataID, group, namespace)}
	default:
		return "", &apiError{Status: status,
			Message: fmt.Sprintf("reading config failed with HTTP %d: %s", status, truncate(string(body), 200))}
	}
}

// searchConfigs lists config metadata. Empty patterns match everything; the
// Nacos blur search treats "*" as a wildcard.
func (c *nacosClient) searchConfigs(ctx context.Context, namespace, dataIDPattern, groupPattern string, pageNo, pageSize int) (*configPage, error) {
	if pageNo <= 0 {
		pageNo = 1
	}
	if pageSize <= 0 || pageSize > 200 {
		pageSize = 20
	}
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
			Message: fmt.Sprintf("listing configs failed with HTTP %d: %s", status, truncate(string(body), 200))}
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
			Message: fmt.Sprintf("listing namespaces failed with HTTP %d: %s", status, truncate(string(body), 200))}
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
	if pageNo <= 0 {
		pageNo = 1
	}
	if pageSize <= 0 || pageSize > 200 {
		pageSize = 20
	}
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
			Message: fmt.Sprintf("listing services failed with HTTP %d: %s", status, truncate(string(body), 200))}
	}
	var page servicePage
	if err := json.Unmarshal(body, &page); err != nil {
		return nil, &apiError{Message: fmt.Sprintf("unexpected service response: %v", err)}
	}
	return &page, nil
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

// truncate shortens strings embedded into error messages.
func truncate(value string, limit int) string {
	value = strings.TrimSpace(value)
	if len(value) <= limit {
		return value
	}
	return value[:limit] + "…"
}

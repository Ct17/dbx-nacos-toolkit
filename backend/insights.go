package main

// insights.go —— v0.3 的四个只读“洞察”能力：
//
//	F1 find_in_configs  配置正文搜索（v3 原生 searchDetail；v1 列表+限量抓取）
//	F2 diff_envs        跨命名空间配置漂移总览（按 md5 对比，可选内容 diff）
//	F3 diff_history     当前配置 vs 历史版本、或两个历史版本对比
//	F4 diagnose         连接体检（协议/健康/服务端状态/鉴权/命名空间/数据面）
//
// 这些能力都只读，并且遵守现有边界：单次调用 30 s 预算、上游 2 MiB、
// 工具结果 6 MiB、错误信息不带凭据与上游正文。

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	contentSearchDefaultMaxResults = 20
	contentSearchMaxResults        = 50
	contentSearchDefaultScanLimit  = 200
	contentSearchMaxScanLimit      = 1000
	contentSearchConcurrency       = 8
	contentSearchMaxSnippets       = 3
	contentSearchSnippetRunes      = 200

	envDiffDefaultMaxConfigs     = 1000
	envDiffMaxConfigs            = 2000
	envDiffDefaultMaxDifferences = 100
	envDiffMaxDifferences        = 500
	envDiffMaxContentDiffs       = 20
	envDiffConfigPageSize        = 500

	historyDiffDefaultContext = 3
	historyDiffMaxContext     = 10

	// diagnoseProbeDataID 是一个几乎不可能存在的 dataId，用来验证数据面
	// 是否可读：返回“不存在”说明读接口通，返回 403 说明鉴权/ACL 有问题。
	diagnoseProbeDataID = "__dbx_plugin_diagnose_probe__"
	diagnoseBudget      = 25 * time.Second
	diagnoseCheckLimit  = 5 * time.Second
)

// --- shared helpers ---------------------------------------------------------

// clampInt 把调用方传入的整数收敛到 [minimum, maximum]；<=0 时使用默认值。
func clampInt(value, defaultValue, minimum, maximum int) int {
	if value <= 0 {
		value = defaultValue
	}
	if value < minimum {
		value = minimum
	}
	if value > maximum {
		value = maximum
	}
	return value
}

// displayNamespace 把空命名空间展示为 public（Nacos 1.x/2.x 的默认命名空间
// id 是空串，3.x 是 "public"）。
func displayNamespace(namespace string) string {
	if strings.TrimSpace(namespace) == "" {
		return "public"
	}
	return strings.TrimSpace(namespace)
}

// normalizeNamespace 统一 API 调用使用的命名空间值。
func normalizeNamespace(namespace string) string {
	return displayNamespace(namespace)
}

// truncateRunes 按 rune 截断但保留原始缩进，供正文片段展示使用。
func truncateRunes(value string, limit int) string {
	runes := []rune(value)
	if len(runes) <= limit {
		return value
	}
	return string(runes[:limit]) + "…"
}

// contentMD5 计算配置正文的 MD5（与 Nacos 的 md5 语义一致）。
func contentMD5(content string) string {
	sum := md5.Sum([]byte(content))
	return hex.EncodeToString(sum[:])
}

// --- F1: find_in_configs ----------------------------------------------------

// contentSnippet 是一行命中内容。
type contentSnippet struct {
	Line int    `json:"line"`
	Text string `json:"text"`
}

// contentMatch 是一个命中配置及其片段。
type contentMatch struct {
	DataID     string           `json:"dataId"`
	Group      string           `json:"group"`
	Namespace  string           `json:"namespace"`
	Type       string           `json:"type,omitempty"`
	MD5        string           `json:"md5,omitempty"`
	MatchCount int              `json:"matchCount"`
	Snippets   []contentSnippet `json:"snippets"`
}

// contentSearchFailure 记录扫描期间被跳过的配置（ACL/网络/竞态删除）。
type contentSearchFailure struct {
	DataID string `json:"dataId"`
	Group  string `json:"group"`
	Error  string `json:"error"`
}

// contentSearchResult 是 find_in_configs 的输出。
type contentSearchResult struct {
	Connection      string                 `json:"connection"`
	Namespace       string                 `json:"namespace"`
	Keyword         string                 `json:"keyword"`
	Matches         []contentMatch         `json:"matches"`
	ScannedConfigs  int                    `json:"scannedConfigs"`
	CandidateCount  int                    `json:"candidateCount"`
	FailedConfigs   int                    `json:"failedConfigs"`
	Failures        []contentSearchFailure `json:"failures,omitempty"`
	Truncated       bool                   `json:"truncated"`
	TruncatedReason string                 `json:"truncatedReason,omitempty"`
	Hint            string                 `json:"hint,omitempty"`
}

// findConfigContent 是 find_in_configs 的入口：v3 走控制台正文搜索，v1
// 没有正文搜索接口，改为分页列出元数据后限量抓取正文。
func (p *plugin) findConfigContent(
	ctx context.Context,
	session *connectionSession,
	namespace, dataIDPattern, groupPattern, keyword string,
	maxResults, scanLimit int,
	caseSensitive bool,
) (*contentSearchResult, error) {
	keyword = strings.TrimSpace(keyword)
	if keyword == "" {
		return nil, fmt.Errorf("keyword is required")
	}
	maxResults = clampInt(maxResults, contentSearchDefaultMaxResults, 1, contentSearchMaxResults)
	scanLimit = clampInt(scanLimit, contentSearchDefaultScanLimit, 1, contentSearchMaxScanLimit)

	family, err := session.client.resolveFamily(ctx)
	if err != nil {
		return nil, err
	}

	result := &contentSearchResult{
		Connection: session.name,
		Namespace:  displayNamespace(namespace),
		Keyword:    keyword,
		Matches:    []contentMatch{},
	}
	if family == familyV3 {
		return searchConfigContentV3(ctx, session.client, result, namespace, dataIDPattern, groupPattern, keyword, maxResults, caseSensitive)
	}
	return searchConfigContentV1(ctx, session.client, result, namespace, dataIDPattern, groupPattern, keyword, maxResults, scanLimit, caseSensitive)
}

// searchConfigContentV3 使用 /v3/console/cs/config/searchDetail 做服务端正文
// 搜索，再抓取命中配置的正文生成片段。
func searchConfigContentV3(
	ctx context.Context,
	client *nacosClient,
	result *contentSearchResult,
	namespace, dataIDPattern, groupPattern, keyword string,
	maxResults int,
	caseSensitive bool,
) (*contentSearchResult, error) {
	page, err := client.searchConfigsByContent(ctx, namespace, dataIDPattern, groupPattern, keyword, 1, maxResults)
	if err != nil {
		return nil, err
	}
	result.CandidateCount = page.TotalCount
	if result.CandidateCount == 0 && len(page.PageItems) > 0 {
		result.CandidateCount = len(page.PageItems)
	}
	candidates := page.PageItems
	if len(candidates) > maxResults {
		candidates = candidates[:maxResults]
	}

	matches, scanned, failures, timedOut := scanConfigCandidates(ctx, client, namespace, candidates, keyword, maxResults, caseSensitive)
	result.Matches = matches
	result.ScannedConfigs = scanned
	result.FailedConfigs = len(failures)
	result.Failures = failures
	result.TruncatedReason = contentSearchTruncationReason(timedOut, result.CandidateCount, len(candidates), len(matches), maxResults, false, 0)
	result.Truncated = result.TruncatedReason != ""
	contentSearchHint(result)
	return result, nil
}

// searchConfigContentV1 分页列出候选配置，再并发抓取正文；v1 没有服务端
// 正文搜索，扫描范围由 scanLimit 控制。
func searchConfigContentV1(
	ctx context.Context,
	client *nacosClient,
	result *contentSearchResult,
	namespace, dataIDPattern, groupPattern, keyword string,
	maxResults, scanLimit int,
	caseSensitive bool,
) (*contentSearchResult, error) {
	candidates := make([]configItem, 0, scanLimit)
	candidateCount := 0
	scanLimitReached := false

	for pageNo := 1; ; pageNo++ {
		page, err := client.searchConfigs(ctx, namespace, dataIDPattern, groupPattern, pageNo, envDiffConfigPageSize)
		if err != nil {
			return nil, err
		}
		if pageNo == 1 {
			candidateCount = page.TotalCount
		}
		if len(page.PageItems) == 0 {
			break
		}
		for _, item := range page.PageItems {
			if len(candidates) >= scanLimit {
				scanLimitReached = true
				break
			}
			candidates = append(candidates, item)
		}
		if scanLimitReached {
			break
		}
		if page.PagesAvailable > 0 && pageNo >= page.PagesAvailable {
			break
		}
		// 防御：某些实现不返回 pagesAvailable，最多翻 100 页。
		if pageNo >= 100 {
			scanLimitReached = candidateCount > len(candidates)
			break
		}
	}
	if candidateCount == 0 && len(candidates) > 0 {
		candidateCount = len(candidates)
	}

	matches, scanned, failures, timedOut := scanConfigCandidates(ctx, client, namespace, candidates, keyword, maxResults, caseSensitive)
	result.CandidateCount = candidateCount
	result.Matches = matches
	result.ScannedConfigs = scanned
	result.FailedConfigs = len(failures)
	result.Failures = failures
	result.TruncatedReason = contentSearchTruncationReason(timedOut, candidateCount, len(candidates), len(matches), maxResults, scanLimitReached, scanLimit)
	result.Truncated = result.TruncatedReason != ""
	contentSearchHint(result)
	return result, nil
}

// scanConfigCandidates 并发读取候选配置并逐行匹配关键词。读取失败的配置
// 会记入 failures 并继续扫描，避免一个 ACL 配置拖垮整次搜索。
func scanConfigCandidates(
	ctx context.Context,
	client *nacosClient,
	namespace string,
	candidates []configItem,
	keyword string,
	maxResults int,
	caseSensitive bool,
) (matches []contentMatch, scanned int, failures []contentSearchFailure, timedOut bool) {
	matches = make([]contentMatch, 0, minInt(len(candidates), maxResults))
	failures = make([]contentSearchFailure, 0)
	if len(candidates) == 0 {
		return matches, 0, failures, false
	}

	var (
		mu       sync.Mutex
		wait     sync.WaitGroup
		sem      = make(chan struct{}, contentSearchConcurrency)
		matched  int
		scannedN int
	)
	// timedOut 会被派发循环与 worker 并发写入，必须统一经 markTimedOut 持锁更新。
	markTimedOut := func() {
		mu.Lock()
		timedOut = true
		mu.Unlock()
	}

dispatch:
	for _, candidate := range candidates {
		if ctx.Err() != nil {
			markTimedOut()
			break
		}
		mu.Lock()
		enough := matched >= maxResults
		mu.Unlock()
		if enough {
			break
		}

		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			markTimedOut()
			break dispatch
		}
		if ctx.Err() != nil {
			<-sem
			markTimedOut()
			break
		}

		wait.Add(1)
		go func(item configItem) {
			defer wait.Done()
			defer func() { <-sem }()

			content, exists, err := client.readConfig(ctx, namespace, item.DataID, item.Group)
			if err != nil {
				if ctx.Err() != nil {
					markTimedOut()
					return
				}
				mu.Lock()
				failures = append(failures, contentSearchFailure{
					DataID: item.DataID,
					Group:  item.Group,
					Error:  err.Error(),
				})
				mu.Unlock()
				return
			}
			if !exists {
				return
			}
			count, snippets := scanConfigContent(content, keyword, caseSensitive)
			mu.Lock()
			scannedN++
			if count > 0 && matched < maxResults {
				matches = append(matches, contentMatch{
					DataID:     item.DataID,
					Group:      item.Group,
					Namespace:  displayNamespace(namespace),
					Type:       item.Type,
					MD5:        item.Md5,
					MatchCount: count,
					Snippets:   snippets,
				})
				matched++
			}
			mu.Unlock()
		}(candidate)
	}
	wait.Wait()

	// 稳定输出顺序：按 group/dataId 排序。
	sort.Slice(matches, func(i, j int) bool {
		if matches[i].Group == matches[j].Group {
			return matches[i].DataID < matches[j].DataID
		}
		return matches[i].Group < matches[j].Group
	})
	sort.Slice(failures, func(i, j int) bool {
		if failures[i].Group == failures[j].Group {
			return failures[i].DataID < failures[j].DataID
		}
		return failures[i].Group < failures[j].Group
	})
	return matches, scannedN, failures, timedOut
}

// scanConfigContent 按行匹配关键词，返回命中行数与最多 N 条片段。
func scanConfigContent(content, keyword string, caseSensitive bool) (int, []contentSnippet) {
	needle := keyword
	if !caseSensitive {
		needle = strings.ToLower(needle)
	}
	lines := strings.Split(strings.ReplaceAll(content, "\r\n", "\n"), "\n")
	count := 0
	snippets := make([]contentSnippet, 0, contentSearchMaxSnippets)
	for index, line := range lines {
		haystack := line
		if !caseSensitive {
			haystack = strings.ToLower(line)
		}
		if !strings.Contains(haystack, needle) {
			continue
		}
		count++
		if len(snippets) < contentSearchMaxSnippets {
			snippets = append(snippets, contentSnippet{
				Line: index + 1,
				Text: truncateRunes(strings.TrimSuffix(line, "\r"), contentSearchSnippetRunes),
			})
		}
	}
	return count, snippets
}

// contentSearchTruncationReason 汇总所有截断原因；空串表示结果完整。
func contentSearchTruncationReason(timedOut bool, candidateCount, inspected, matched, maxResults int, scanLimitReached bool, scanLimit int) string {
	reasons := make([]string, 0, 3)
	if timedOut {
		reasons = append(reasons, "time budget reached")
	}
	if candidateCount > inspected {
		if scanLimitReached && scanLimit > 0 {
			reasons = append(reasons, fmt.Sprintf("scanLimit %d reached (inspected %d of %d candidate configs)", scanLimit, inspected, candidateCount))
		} else {
			reasons = append(reasons, fmt.Sprintf("only inspected %d of %d candidate configs", inspected, candidateCount))
		}
	}
	if matched >= maxResults && candidateCount > matched {
		reasons = append(reasons, fmt.Sprintf("maxResults %d reached", maxResults))
	}
	return strings.Join(reasons, "; ")
}

// contentSearchHint 给模型一个可执行的下钻建议。
func contentSearchHint(result *contentSearchResult) {
	if result.Truncated {
		result.Hint = "Narrow dataId/group, raise maxResults/scanLimit, or query a specific namespace to get complete results."
	} else if len(result.Matches) == 0 {
		result.Hint = "No literal match found. Try a shorter keyword, another namespace, or search_configs for metadata."
	}
	if result.FailedConfigs > 0 {
		if result.Hint != "" {
			result.Hint += " "
		}
		result.Hint += fmt.Sprintf("%d config(s) could not be read (see failures).", result.FailedConfigs)
	}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// --- F2: diff_envs ----------------------------------------------------------

type namespaceConfigKey struct {
	Group  string
	DataID string
}

type envDiffSummary struct {
	Namespace      string `json:"namespace"`
	Identical      int    `json:"identical"`
	Different      int    `json:"different"`
	OnlyInBaseline int    `json:"onlyInBaseline"`
	OnlyInTarget   int    `json:"onlyInTarget"`
	ScannedConfigs int    `json:"scannedConfigs"`
	Truncated      bool   `json:"truncated"`
}

type envDiffChange struct {
	Namespace   string `json:"namespace"`
	DataID      string `json:"dataId"`
	Group       string `json:"group"`
	Status      string `json:"status"`
	BaselineMD5 string `json:"baselineMd5,omitempty"`
	TargetMD5   string `json:"targetMd5,omitempty"`
	Added       int    `json:"added,omitempty"`
	Removed     int    `json:"removed,omitempty"`
	Diff        string `json:"diff,omitempty"`
}

type envDiffError struct {
	Namespace string `json:"namespace"`
	Error     string `json:"error"`
}

type envDiffResult struct {
	Connection string           `json:"connection"`
	Baseline   string           `json:"baseline"`
	DataID     string           `json:"dataId,omitempty"`
	Group      string           `json:"group,omitempty"`
	Summaries  []envDiffSummary `json:"summaries"`
	Changed    []envDiffChange  `json:"changed"`
	Errors     []envDiffError   `json:"errors,omitempty"`
	Truncated  bool             `json:"truncated"`
	Hint       string           `json:"hint,omitempty"`
}

// diffEnvironments 对比多个命名空间中的配置集合：基准命名空间为第一个，
// 其余命名空间按 group@@dataId 与 md5 与基准比较。
func (p *plugin) diffEnvironments(
	ctx context.Context,
	session *connectionSession,
	namespaces []string,
	dataIDPattern, groupPattern string,
	includeContentDiff bool,
	maxConfigs, maxDifferences int,
) (*envDiffResult, error) {
	if len(namespaces) < 2 {
		return nil, fmt.Errorf("namespaces requires at least two entries; the first one is the baseline")
	}
	if len(namespaces) > 8 {
		return nil, fmt.Errorf("at most 8 namespaces per diff call")
	}
	normalized := make([]string, 0, len(namespaces))
	seen := make(map[string]bool, len(namespaces))
	for _, namespace := range namespaces {
		name := normalizeNamespace(namespace)
		if seen[name] {
			return nil, fmt.Errorf("namespaces must be distinct; %q appears more than once", name)
		}
		seen[name] = true
		normalized = append(normalized, name)
	}
	maxConfigs = clampInt(maxConfigs, envDiffDefaultMaxConfigs, 1, envDiffMaxConfigs)
	maxDifferences = clampInt(maxDifferences, envDiffDefaultMaxDifferences, 1, envDiffMaxDifferences)

	result := &envDiffResult{
		Connection: session.name,
		Baseline:   normalized[0],
		DataID:     strings.TrimSpace(dataIDPattern),
		Group:      strings.TrimSpace(groupPattern),
		Summaries:  []envDiffSummary{},
		Changed:    []envDiffChange{},
	}

	baseline, _, baselineTruncated, err := loadConfigFingerprints(ctx, session.client, normalized[0], dataIDPattern, groupPattern, maxConfigs)
	if err != nil {
		return nil, fmt.Errorf("reading namespace %q failed: %w", normalized[0], err)
	}
	if baselineTruncated {
		result.Truncated = true
	}

	for _, namespace := range normalized[1:] {
		target, _, truncated, err := loadConfigFingerprints(ctx, session.client, namespace, dataIDPattern, groupPattern, maxConfigs)
		if err != nil {
			result.Errors = append(result.Errors, envDiffError{Namespace: namespace, Error: err.Error()})
			continue
		}
		summary := envDiffSummary{
			Namespace:      namespace,
			ScannedConfigs: len(target),
			Truncated:      truncated || baselineTruncated,
		}
		if truncated || baselineTruncated {
			result.Truncated = true
		}

		for _, key := range sortedConfigKeys(baseline) {
			baseItem := baseline[key]
			targetItem, exists := target[key]
			switch {
			case !exists:
				summary.OnlyInBaseline++
				result.appendEnvDiffChange(envDiffChange{
					Namespace: namespace, DataID: key.DataID, Group: key.Group,
					Status: "only-in-baseline", BaselineMD5: baseItem.Md5,
				}, maxDifferences)
			case baseItem.Md5 != targetItem.Md5:
				summary.Different++
				result.appendEnvDiffChange(envDiffChange{
					Namespace: namespace, DataID: key.DataID, Group: key.Group,
					Status: "different", BaselineMD5: baseItem.Md5, TargetMD5: targetItem.Md5,
				}, maxDifferences)
			default:
				summary.Identical++
			}
		}
		for _, key := range sortedConfigKeys(target) {
			if _, exists := baseline[key]; exists {
				continue
			}
			summary.OnlyInTarget++
			result.appendEnvDiffChange(envDiffChange{
				Namespace: namespace, DataID: key.DataID, Group: key.Group,
				Status: "only-in-target", TargetMD5: target[key].Md5,
			}, maxDifferences)
		}
		result.Summaries = append(result.Summaries, summary)
	}

	if includeContentDiff {
		enrichEnvDiffContent(ctx, session, result, normalized[0], maxDifferences)
	}
	if result.Truncated && result.Hint == "" {
		result.Hint = fmt.Sprintf("Results were capped (maxConfigsPerNamespace=%d, maxDifferences=%d). Narrow dataId/group or raise the caps for a complete comparison.", maxConfigs, maxDifferences)
	}
	if len(result.Errors) > 0 {
		if result.Hint != "" {
			result.Hint += " "
		}
		result.Hint += "Some namespaces or configs could not be read; see errors."
	}
	return result, nil
}

// appendEnvDiffChange 追加差异项，超过 maxDifferences 时停止。
func (r *envDiffResult) appendEnvDiffChange(change envDiffChange, maxDifferences int) {
	if len(r.Changed) >= maxDifferences {
		r.Truncated = true
		return
	}
	r.Changed = append(r.Changed, change)
}

// enrichEnvDiffContent 对前 N 条 different 配置抓正文并生成行级 diff。
func enrichEnvDiffContent(ctx context.Context, session *connectionSession, result *envDiffResult, baselineNamespace string, maxDifferences int) {
	limit := minInt(maxDifferences, envDiffMaxContentDiffs)
	done := 0
	for index := range result.Changed {
		if done >= limit {
			result.Truncated = true
			if result.Hint == "" {
				result.Hint = fmt.Sprintf("Content diff generated for the first %d changed configs; use diff_config for the rest.", limit)
			}
			return
		}
		change := &result.Changed[index]
		if change.Status != "different" {
			continue
		}
		done++
		baselineContent, baselineExists, err := session.client.readConfig(ctx, baselineNamespace, change.DataID, change.Group)
		if err != nil || !baselineExists {
			if err != nil {
				result.Errors = append(result.Errors, envDiffError{Namespace: baselineNamespace, Error: err.Error()})
			}
			continue
		}
		targetContent, targetExists, err := session.client.readConfig(ctx, change.Namespace, change.DataID, change.Group)
		if err != nil || !targetExists {
			if err != nil {
				result.Errors = append(result.Errors, envDiffError{Namespace: change.Namespace, Error: err.Error()})
			}
			continue
		}
		diffs, err := lineDiffs(baselineContent, targetContent)
		if err != nil {
			result.Errors = append(result.Errors, envDiffError{Namespace: change.Namespace, Error: err.Error()})
			continue
		}
		added, removed := countChanges(diffs)
		change.Added = added
		change.Removed = removed
		change.Diff = renderDiff(diffs, diffContext)
	}
}

// loadConfigFingerprints 分页读取一个命名空间的配置元数据，最多 maxConfigs 条。
func loadConfigFingerprints(
	ctx context.Context,
	client *nacosClient,
	namespace, dataIDPattern, groupPattern string,
	maxConfigs int,
) (map[namespaceConfigKey]configItem, int, bool, error) {
	items := make(map[namespaceConfigKey]configItem, minInt(maxConfigs, 512))
	totalCount := 0
	truncated := false
	for pageNo := 1; ; pageNo++ {
		page, err := client.searchConfigs(ctx, namespace, dataIDPattern, groupPattern, pageNo, envDiffConfigPageSize)
		if err != nil {
			return nil, 0, false, err
		}
		if pageNo == 1 {
			totalCount = page.TotalCount
		}
		if len(page.PageItems) == 0 {
			break
		}
		for _, item := range page.PageItems {
			if len(items) >= maxConfigs {
				truncated = true
				break
			}
			items[namespaceConfigKey{Group: item.Group, DataID: item.DataID}] = item
		}
		if truncated {
			break
		}
		if page.PagesAvailable > 0 && pageNo >= page.PagesAvailable {
			break
		}
		if pageNo >= 100 {
			truncated = totalCount > len(items)
			break
		}
	}
	if totalCount == 0 && len(items) > 0 {
		totalCount = len(items)
	}
	if totalCount > len(items) {
		truncated = true
	}
	// Nacos 2.4.3 的 blur 搜索响应里 md5 为 null（v3 正常返回 md5）。没有
	// md5 就无法判断跨命名空间是否一致，这里抓一次正文计算 MD5；有 md5 的
	// 协议栈不会产生额外请求。
	if err := fillMissingFingerprints(ctx, client, namespace, items); err != nil {
		return nil, 0, false, err
	}
	return items, totalCount, truncated, nil
}

// fillMissingFingerprints 为缺少 md5 的配置补算内容 MD5（仅 diff_envs 使用）。
func fillMissingFingerprints(ctx context.Context, client *nacosClient, namespace string, items map[namespaceConfigKey]configItem) error {
	missing := make([]namespaceConfigKey, 0)
	for key, item := range items {
		md5Value := strings.TrimSpace(item.Md5)
		if md5Value == "" || strings.EqualFold(md5Value, "null") {
			missing = append(missing, key)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	sort.Slice(missing, func(i, j int) bool {
		if missing[i].Group == missing[j].Group {
			return missing[i].DataID < missing[j].DataID
		}
		return missing[i].Group < missing[j].Group
	})

	var (
		mu       sync.Mutex
		wait     sync.WaitGroup
		sem      = make(chan struct{}, contentSearchConcurrency)
		firstErr error
		timedOut bool
	)
dispatch:
	for _, key := range missing {
		if ctx.Err() != nil {
			timedOut = true
			break
		}
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			timedOut = true
			break dispatch
		}
		wait.Add(1)
		go func(key namespaceConfigKey) {
			defer wait.Done()
			defer func() { <-sem }()

			mu.Lock()
			item := items[key]
			mu.Unlock()
			content, exists, err := client.readConfig(ctx, namespace, item.DataID, item.Group)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("reading %s/%s to compute md5 failed: %w", item.Group, item.DataID, err)
				}
				return
			}
			if !exists {
				if firstErr == nil {
					firstErr = fmt.Errorf("config %s/%s disappeared while computing md5", item.Group, item.DataID)
				}
				return
			}
			item.Md5 = contentMD5(content)
			items[key] = item
		}(key)
	}
	wait.Wait()
	if firstErr != nil {
		return firstErr
	}
	if timedOut {
		return fmt.Errorf("computing md5 for %d config(s) timed out", len(missing))
	}
	return nil
}

func sortedConfigKeys(items map[namespaceConfigKey]configItem) []namespaceConfigKey {
	keys := make([]namespaceConfigKey, 0, len(items))
	for key := range items {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Group == keys[j].Group {
			return keys[i].DataID < keys[j].DataID
		}
		return keys[i].Group < keys[j].Group
	})
	return keys
}

// --- F3: diff_history -------------------------------------------------------

type historyDiffSide struct {
	Source      string `json:"source"` // current / history
	HistoryID   string `json:"historyId,omitempty"`
	Operator    string `json:"operator,omitempty"`
	SrcIP       string `json:"srcIp,omitempty"`
	OpType      string `json:"opType,omitempty"`
	MD5         string `json:"md5,omitempty"`
	CreatedTime string `json:"createdTime,omitempty"`
	Exists      bool   `json:"exists"`
}

type historyDiffResult struct {
	Connection string          `json:"connection"`
	DataID     string          `json:"dataId"`
	Group      string          `json:"group"`
	Namespace  string          `json:"namespace"`
	Status     string          `json:"status"` // identical / different / missing-in-target
	From       historyDiffSide `json:"from"`
	To         historyDiffSide `json:"to"`
	Added      int             `json:"added"`
	Removed    int             `json:"removed"`
	Diff       string          `json:"diff,omitempty"`
	Hint       string          `json:"hint,omitempty"`
}

// diffConfigHistory 对比一个历史版本与当前版本（或另一个历史版本）。
func (p *plugin) diffConfigHistory(
	ctx context.Context,
	session *connectionSession,
	namespace, dataID, group, fromHistoryID, toHistoryID string,
	contextLines int,
) (*historyDiffResult, error) {
	if strings.TrimSpace(dataID) == "" {
		return nil, fmt.Errorf("dataId is required")
	}
	if strings.TrimSpace(fromHistoryID) == "" {
		return nil, fmt.Errorf("fromHistoryId is required")
	}
	if group == "" {
		group = defaultGroup
	}
	contextLines = clampIntContext(contextLines)

	fromDetail, err := session.client.getConfigRevision(ctx, namespace, dataID, group, fromHistoryID)
	if err != nil {
		return nil, err
	}

	result := &historyDiffResult{
		Connection: session.name,
		DataID:     dataID,
		Group:      group,
		Namespace:  displayNamespace(namespace),
		From:       revisionSide(fromDetail),
	}

	var toContent string
	if strings.TrimSpace(toHistoryID) != "" {
		toDetail, err := session.client.getConfigRevision(ctx, namespace, dataID, group, toHistoryID)
		if err != nil {
			return nil, err
		}
		result.To = revisionSide(toDetail)
		toContent = toDetail.Content
	} else {
		content, exists, err := session.client.readConfig(ctx, namespace, dataID, group)
		if err != nil {
			return nil, err
		}
		result.To = historyDiffSide{Source: "current", Exists: exists}
		if exists {
			result.To.MD5 = contentMD5(content)
			toContent = content
		}
	}

	if !result.To.Exists {
		result.Status = "missing-in-target"
		result.Hint = "The current config does not exist; compare two revisions via toHistoryId instead."
		return result, nil
	}
	if fromDetail.Content == toContent {
		result.Status = "identical"
		return result, nil
	}
	diffs, err := lineDiffs(fromDetail.Content, toContent)
	if err != nil {
		return nil, err
	}
	added, removed := countChanges(diffs)
	result.Status = "different"
	result.Added = added
	result.Removed = removed
	result.Diff = renderDiff(diffs, contextLines)
	return result, nil
}

func revisionSide(detail *configRevisionDetail) historyDiffSide {
	return historyDiffSide{
		Source:      "history",
		HistoryID:   string(detail.ID),
		Operator:    detail.SrcUser,
		SrcIP:       detail.SrcIP,
		OpType:      strings.TrimSpace(detail.OpType),
		MD5:         detail.Md5,
		CreatedTime: formatMillis(detail.revisionTime()),
		Exists:      true,
	}
}

func clampIntContext(value int) int {
	if value <= 0 {
		return historyDiffDefaultContext
	}
	if value > historyDiffMaxContext {
		return historyDiffMaxContext
	}
	return value
}

// --- F4: diagnose -----------------------------------------------------------

type diagnoseCheck struct {
	ID          string `json:"id"`
	Status      string `json:"status"` // ok / warn / fail
	Detail      string `json:"detail"`
	Remediation string `json:"remediation,omitempty"`
}

type diagnoseServer struct {
	Version          string `json:"version,omitempty"`
	Mode             string `json:"mode,omitempty"`
	AuthEnabled      *bool  `json:"authEnabled,omitempty"`
	AuthAdminRequest *bool  `json:"authAdminRequest,omitempty"`
	DefaultMaxSize   int    `json:"defaultMaxSize,omitempty"`
	Protocol         string `json:"protocol,omitempty"`
}

type diagnoseResult struct {
	Connection  string          `json:"connection"`
	Overall     string          `json:"overall"` // ok / warn / fail
	Server      diagnoseServer  `json:"server"`
	Checks      []diagnoseCheck `json:"checks"`
	Suggestions []string        `json:"suggestions"`
}

// diagnoseConnection 执行一组只读检查，任何检查失败都变成可读的 status，
// 而不是让整个工具报错；这样 AI 能直接根据结果给出修复建议。
func (p *plugin) diagnoseConnection(ctx context.Context, session *connectionSession) *diagnoseResult {
	diagCtx, cancel := context.WithTimeout(ctx, diagnoseBudget)
	defer cancel()

	result := &diagnoseResult{
		Connection:  session.name,
		Overall:     "ok",
		Checks:      []diagnoseCheck{},
		Suggestions: []string{},
	}
	addCheck := func(id, status, detail, remediation string) {
		result.Checks = append(result.Checks, diagnoseCheck{ID: id, Status: status, Detail: detail, Remediation: remediation})
		if status == "fail" {
			result.Overall = "fail"
		} else if status == "warn" && result.Overall == "ok" {
			result.Overall = "warn"
		}
	}
	addSuggestion := func(suggestion string) {
		if strings.TrimSpace(suggestion) == "" {
			return
		}
		for _, existing := range result.Suggestions {
			if existing == suggestion {
				return
			}
		}
		result.Suggestions = append(result.Suggestions, suggestion)
	}

	family, err := session.client.resolveFamily(diagCtx)
	if err != nil {
		result.Server.Protocol = session.client.resolvedFamilyName()
		addCheck("protocol", "fail", err.Error(), "Check the server address/port; for Nacos 3.x also set the console port (default 8080).")
		addSuggestion("Verify the connection address, port, context path and (for Nacos 3.x) the console port.")
		return result
	}
	result.Server.Protocol = family.name()
	addCheck("protocol", "ok", fmt.Sprintf("detected %s protocol stack", family.name()), "")

	healthStart := time.Now()
	healthCtx, healthCancel := context.WithTimeout(diagCtx, diagnoseCheckLimit)
	status, healthErr := session.client.health(healthCtx)
	healthCancel()
	if healthErr != nil {
		addCheck("health", "fail", healthErr.Error(), "Check that the Nacos server is running and reachable from DBX.")
		addSuggestion("Make sure Nacos is reachable and the address/port are correct.")
		return result
	}
	addCheck("health", "ok", fmt.Sprintf("%s (%.0f ms)", status, float64(time.Since(healthStart).Milliseconds())), "")

	stateCtx, stateCancel := context.WithTimeout(diagCtx, diagnoseCheckLimit)
	state, stateErr := session.client.serverState(stateCtx)
	stateCancel()
	var authEnabled *bool
	var authAdminRequest *bool
	if stateErr != nil {
		addCheck("server-state", "warn", stateErr.Error(), "The server state endpoint is unavailable; some diagnostics are limited.")
	} else {
		result.Server.Version = stateString(state, "version")
		result.Server.Mode = stateString(state, "startup_mode", "startupMode", "serverEnv", "mode")
		authEnabled = stateBool(state, "auth_enabled", "authEnabled")
		authAdminRequest = stateBool(state, "auth_admin_request", "authAdminRequest")
		result.Server.AuthEnabled = authEnabled
		result.Server.AuthAdminRequest = authAdminRequest
		result.Server.DefaultMaxSize = stateInt(state, "defaultMaxSize", "default_max_size", "maxContent")
		addCheck("server-state", "ok", serverStateDetail(result.Server), "")
	}

	if authAdminRequest != nil && *authAdminRequest {
		addCheck("auth-admin", "fail",
			"Nacos reports that the admin user still needs to be initialized (auth_admin_request=true).",
			"Initialize the admin user (POST /v3/auth/user/admin), then set username/password in the connection.")
		addSuggestion("Initialize the Nacos 3.x admin user, then fill the account into the connection.")
	} else {
		addCheck("auth-admin", "ok", "admin user is initialized (or the server does not require initialization)", "")
	}

	if session.client.username == "" {
		switch {
		case family == familyV3:
			addCheck("credentials", "warn", "no username/password configured; Nacos 3.x console APIs require a login.",
				"Set username/password in the connection to enable search/history/namespaces/services.")
			addSuggestion("Set username/password to unlock Nacos 3.x console tools.")
		case authEnabled != nil && !*authEnabled:
			addCheck("credentials", "ok", "authentication is disabled on the server; anonymous reads are available", "")
		default:
			addCheck("credentials", "warn", "no username/password configured; only anonymous reads will work.", "")
			addSuggestion("If authentication is enabled on Nacos, set username/password.")
		}
	} else {
		loginCtx, loginCancel := context.WithTimeout(diagCtx, diagnoseCheckLimit)
		_, loginErr := session.client.ensureToken(loginCtx)
		loginCancel()
		if loginErr != nil {
			addCheck("credentials", "fail", loginErr.Error(), "Check the username/password and the Nacos authentication settings.")
			addSuggestion("Verify the username/password and whether Nacos authentication is enabled.")
		} else {
			addCheck("credentials", "ok", "login succeeded", "")
		}
	}

	if family == familyV3 && session.client.username == "" {
		addCheck("namespace-access", "warn", "skipped: Nacos 3.x console namespace API requires credentials", "Set username/password.")
	} else {
		namespacesCtx, namespacesCancel := context.WithTimeout(diagCtx, diagnoseCheckLimit)
		namespaces, namespacesErr := session.client.listNamespaces(namespacesCtx)
		namespacesCancel()
		if namespacesErr != nil {
			addCheck("namespace-access", "warn", namespacesErr.Error(), "Check the account permissions for the namespace list API.")
			addSuggestion("Make sure the account can list namespaces.")
		} else {
			current := session.namespace
			found := namespaceExists(namespaces, current)
			state := "was not found"
			if found {
				state = "exists"
			}
			detail := fmt.Sprintf("%d namespace(s) accessible; current namespace %q %s", len(namespaces), displayNamespace(current), state)
			if !found {
				addCheck("namespace-access", "warn", detail, "Check the namespace id/name in the connection.")
				addSuggestion("Check that the configured namespace exists on the server.")
			} else {
				addCheck("namespace-access", "ok", detail, "")
			}
		}
	}

	probeCtx, probeCancel := context.WithTimeout(diagCtx, diagnoseCheckLimit)
	_, probeExists, probeErr := session.client.readConfig(probeCtx, session.namespace, diagnoseProbeDataID, defaultGroup)
	probeCancel()
	if probeErr != nil {
		addCheck("config-read", "fail", probeErr.Error(), "Check authentication, ACLs and the config read API.")
		addSuggestion("Verify that the account can read configs in the target namespace.")
	} else if probeExists {
		addCheck("config-read", "ok", "data plane is reachable (probe config unexpectedly exists)", "")
	} else {
		addCheck("config-read", "ok", "data plane is reachable (probe config not found, as expected)", "")
	}

	if result.Overall == "fail" {
		addSuggestion("Fix the failing checks above, then run diagnose again.")
	}
	return result
}

func serverStateDetail(server diagnoseServer) string {
	parts := make([]string, 0, 4)
	if server.Version != "" {
		parts = append(parts, "version "+server.Version)
	}
	if server.Mode != "" {
		parts = append(parts, "mode "+server.Mode)
	}
	if server.AuthEnabled != nil {
		parts = append(parts, fmt.Sprintf("auth enabled=%t", *server.AuthEnabled))
	}
	if server.DefaultMaxSize > 0 {
		parts = append(parts, fmt.Sprintf("defaultMaxSize=%d", server.DefaultMaxSize))
	}
	if len(parts) == 0 {
		return "server state reachable"
	}
	return strings.Join(parts, ", ")
}

func namespaceExists(namespaces []namespace, current string) bool {
	current = strings.TrimSpace(current)
	if current == "" || current == "public" {
		for _, item := range namespaces {
			if item.NamespaceID == "" || item.NamespaceID == "public" {
				return true
			}
		}
		return false
	}
	for _, item := range namespaces {
		if item.NamespaceID == current || item.ShowName == current {
			return true
		}
	}
	return false
}

func stateString(state map[string]any, keys ...string) string {
	for _, key := range keys {
		value, ok := state[key]
		if !ok {
			continue
		}
		switch typed := value.(type) {
		case string:
			if strings.TrimSpace(typed) != "" {
				return strings.TrimSpace(typed)
			}
		case float64:
			return fmt.Sprintf("%.0f", typed)
		}
	}
	return ""
}

func stateBool(state map[string]any, keys ...string) *bool {
	for _, key := range keys {
		value, ok := state[key]
		if !ok {
			continue
		}
		switch typed := value.(type) {
		case bool:
			return &typed
		case string:
			normalized := strings.ToLower(strings.TrimSpace(typed))
			if normalized == "true" {
				result := true
				return &result
			}
			if normalized == "false" {
				result := false
				return &result
			}
		case float64:
			result := typed != 0
			return &result
		}
	}
	return nil
}

func stateInt(state map[string]any, keys ...string) int {
	for _, key := range keys {
		value, ok := state[key]
		if !ok {
			continue
		}
		switch typed := value.(type) {
		case float64:
			return int(typed)
		case string:
			var parsed int
			if _, err := fmt.Sscanf(strings.TrimSpace(typed), "%d", &parsed); err == nil {
				return parsed
			}
		}
	}
	return 0
}

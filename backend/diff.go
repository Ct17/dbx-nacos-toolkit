package main

// diff.go —— 行级文本差异对比（LCS 动态规划实现，保持零第三方依赖）。
//
// 面向 Nacos 配置文本（YAML / properties / JSON）的对比场景：
// 数千行以内的配置毫秒级完成，输出为带上下文压缩的行级差异。

import (
	"fmt"
	"strings"
)

const (
	// maxDiffLines 限制参与 diff 的最大行数：LCS 表为 O(n*m) 内存，
	// 超出该规模直接给出可操作的报错，而不是吃光内存。
	maxDiffLines = 3000

	// diffContext 是每处差异前后保留的上下文行数。
	diffContext = 3
)

// diffLine 表示一行对比结果：Kind 为 ' '（相同）、'-'（仅基准侧）或 '+'（仅目标侧）。
type diffLine struct {
	Kind byte
	Text string
}

// splitDiffLines 按行切分文本：
// 统一 CRLF（Nacos 上 Windows 环境写入的配置常见），并忽略末尾空行差异。
func splitDiffLines(text string) []string {
	text = strings.ReplaceAll(text, "\r\n", "\n")
	text = strings.TrimRight(text, "\n")
	if strings.TrimSpace(text) == "" {
		return nil
	}
	return strings.Split(text, "\n")
}

// lineDiffs 计算基准文本到目标文本的行级差异（基于最长公共子序列）。
func lineDiffs(baselineText, targetText string) ([]diffLine, error) {
	baseline := splitDiffLines(baselineText)
	target := splitDiffLines(targetText)
	if len(baseline) > maxDiffLines || len(target) > maxDiffLines {
		return nil, fmt.Errorf("config too large to diff: %d vs %d lines (limit %d)",
			len(baseline), len(target), maxDiffLines)
	}

	n, m := len(baseline), len(target)
	// lcs[i][j]：baseline[i:] 与 target[j:] 的最长公共子序列长度，平铺为 int32 节省内存。
	lcs := make([]int32, (n+1)*(m+1))
	stride := m + 1
	at := func(i, j int) int32 { return lcs[i*stride+j] }
	set := func(i, j int, value int32) { lcs[i*stride+j] = value }
	for i := n - 1; i >= 0; i-- {
		for j := m - 1; j >= 0; j-- {
			switch {
			case baseline[i] == target[j]:
				set(i, j, at(i+1, j+1)+1)
			case at(i+1, j) >= at(i, j+1):
				set(i, j, at(i+1, j))
			default:
				set(i, j, at(i, j+1))
			}
		}
	}

	// 回溯生成编辑脚本。
	var out []diffLine
	i, j := 0, 0
	for i < n && j < m {
		switch {
		case baseline[i] == target[j]:
			out = append(out, diffLine{' ', baseline[i]})
			i++
			j++
		case at(i+1, j) >= at(i, j+1):
			out = append(out, diffLine{'-', baseline[i]})
			i++
		default:
			out = append(out, diffLine{'+', target[j]})
			j++
		}
	}
	for ; i < n; i++ {
		out = append(out, diffLine{'-', baseline[i]})
	}
	for ; j < m; j++ {
		out = append(out, diffLine{'+', target[j]})
	}
	return out, nil
}

// countChanges 统计差异中的新增行与删除行数量。
func countChanges(lines []diffLine) (added, removed int) {
	for _, line := range lines {
		switch line.Kind {
		case '+':
			added++
		case '-':
			removed++
		}
	}
	return added, removed
}

// renderDiff 把行级差异渲染成压缩上下文的文本：
// 每处差异保留前后 diffContext 行，中间连续的未变化区域折叠为一行提示。
func renderDiff(lines []diffLine, context int) string {
	keep := make([]bool, len(lines))
	for index, line := range lines {
		if line.Kind == ' ' {
			continue
		}
		start := index - context
		if start < 0 {
			start = 0
		}
		end := index + context
		if end >= len(lines) {
			end = len(lines) - 1
		}
		for cursor := start; cursor <= end; cursor++ {
			keep[cursor] = true
		}
	}

	var builder strings.Builder
	skipped := 0
	written := false
	for index, line := range lines {
		if !keep[index] {
			skipped++
			continue
		}
		// 开头与结尾的未变化区域不输出省略标记，只在差异块之间提示。
		if written && skipped > 0 {
			fmt.Fprintf(&builder, "...（%d 行未变化）\n", skipped)
		}
		skipped = 0
		written = true
		switch line.Kind {
		case '+':
			builder.WriteString("+ " + line.Text + "\n")
		case '-':
			builder.WriteString("- " + line.Text + "\n")
		default:
			builder.WriteString("  " + line.Text + "\n")
		}
	}
	return strings.TrimRight(builder.String(), "\n")
}

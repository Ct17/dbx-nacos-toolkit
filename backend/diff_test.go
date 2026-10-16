package main

import (
	"strings"
	"testing"
)

func TestLineDiffsIdentical(t *testing.T) {
	text := "a: 1\nb: 2\nc: 3"
	diffs, err := lineDiffs(text, text)
	if err != nil {
		t.Fatalf("lineDiffs 失败: %v", err)
	}
	added, removed := countChanges(diffs)
	if added != 0 || removed != 0 {
		t.Fatalf("相同文本不应产生变更: +%d -%d", added, removed)
	}
}

func TestLineDiffsModified(t *testing.T) {
	baseline := "server:\n  port: 8080\n  host: 0.0.0.0\n"
	target := "server:\n  port: 9090\n  host: 0.0.0.0\n"
	diffs, err := lineDiffs(baseline, target)
	if err != nil {
		t.Fatalf("lineDiffs 失败: %v", err)
	}
	added, removed := countChanges(diffs)
	if added != 1 || removed != 1 {
		t.Fatalf("修改一行应产生 +1 -1: 实际 +%d -%d", added, removed)
	}
	rendered := renderDiff(diffs, diffContext)
	if !strings.Contains(rendered, "-   port: 8080") || !strings.Contains(rendered, "+   port: 9090") {
		t.Fatalf("渲染结果缺少变更行:\n%s", rendered)
	}
}

func TestLineDiffsAddedAndRemoved(t *testing.T) {
	baseline := "a\nb\nc\nd\ne"
	target := "a\nc\nd\nf\ne"
	diffs, err := lineDiffs(baseline, target)
	if err != nil {
		t.Fatalf("lineDiffs 失败: %v", err)
	}
	added, removed := countChanges(diffs)
	if added != 1 || removed != 1 {
		t.Fatalf("期望 +1 -1: 实际 +%d -%d", added, removed)
	}
}

func TestLineDiffsEmptySides(t *testing.T) {
	diffs, err := lineDiffs("", "x\ny")
	if err != nil {
		t.Fatalf("lineDiffs 失败: %v", err)
	}
	added, removed := countChanges(diffs)
	if added != 2 || removed != 0 {
		t.Fatalf("空文本对比应为纯新增: +%d -%d", added, removed)
	}

	diffs, err = lineDiffs("x\ny", "")
	if err != nil {
		t.Fatalf("lineDiffs 失败: %v", err)
	}
	added, removed = countChanges(diffs)
	if added != 0 || removed != 2 {
		t.Fatalf("与空文本对比应为纯删除: +%d -%d", added, removed)
	}
}

func TestLineDiffsCRLFNormalized(t *testing.T) {
	// Nacos 上 Windows 写入的配置是 CRLF，不应因此产生虚假差异。
	diffs, err := lineDiffs("a\r\nb\r\n", "a\nb\n")
	if err != nil {
		t.Fatalf("lineDiffs 失败: %v", err)
	}
	added, removed := countChanges(diffs)
	if added != 0 || removed != 0 {
		t.Fatalf("行尾差异不应算作变更: +%d -%d", added, removed)
	}
}

func TestLineDiffsTooLarge(t *testing.T) {
	large := strings.Repeat("x\n", maxDiffLines+1)
	if _, err := lineDiffs(large, "a\n"); err == nil {
		t.Fatal("超过行数上限应报错")
	}
}

func TestRenderDiffCompactsContext(t *testing.T) {
	// 单差异块：50 行未变化 + 1 行变更 + 50 行未变化。
	// 期望只保留差异附近的上下文，且开头/结尾不输出折叠提示。
	var baseline, target strings.Builder
	for i := 0; i < 50; i++ {
		baseline.WriteString("same line\n")
		target.WriteString("same line\n")
	}
	baseline.WriteString("old value\n")
	target.WriteString("new value\n")
	for i := 0; i < 50; i++ {
		baseline.WriteString("same line\n")
		target.WriteString("same line\n")
	}

	diffs, err := lineDiffs(baseline.String(), target.String())
	if err != nil {
		t.Fatalf("lineDiffs 失败: %v", err)
	}
	rendered := renderDiff(diffs, diffContext)
	lines := strings.Split(rendered, "\n")

	// 保留 3 行上下文 × 2 + 2 行变更 = 8 行，远小于 102 行
	if len(lines) > 12 {
		t.Fatalf("上下文压缩未生效，输出 %d 行:\n%s", len(lines), rendered)
	}
	if strings.Contains(rendered, "行未变化）") {
		t.Fatalf("单差异块不应出现折叠提示:\n%s", rendered)
	}
	if !strings.Contains(rendered, "+ new value") || !strings.Contains(rendered, "- old value") {
		t.Fatalf("缺少变更行:\n%s", rendered)
	}
}

func TestRenderDiffFoldsBetweenBlocks(t *testing.T) {
	// 两个差异块：中间的连续未变化区域应折叠为一行提示。
	var baseline, target strings.Builder
	for i := 0; i < 50; i++ {
		baseline.WriteString("same head\n")
		target.WriteString("same head\n")
	}
	baseline.WriteString("first: old\n")
	target.WriteString("first: new\n")
	for i := 0; i < 50; i++ {
		baseline.WriteString("same middle\n")
		target.WriteString("same middle\n")
	}
	baseline.WriteString("second: old\n")
	target.WriteString("second: new\n")
	for i := 0; i < 50; i++ {
		baseline.WriteString("same tail\n")
		target.WriteString("same tail\n")
	}

	diffs, err := lineDiffs(baseline.String(), target.String())
	if err != nil {
		t.Fatalf("lineDiffs 失败: %v", err)
	}
	rendered := renderDiff(diffs, diffContext)

	if !strings.Contains(rendered, "行未变化）") {
		t.Fatalf("两个差异块之间应出现折叠提示:\n%s", rendered)
	}
	if !strings.Contains(rendered, "- first: old") || !strings.Contains(rendered, "+ second: new") {
		t.Fatalf("缺少变更行:\n%s", rendered)
	}
	lines := strings.Split(rendered, "\n")
	// 两个差异块各 8 行左右 + 1 行折叠提示
	if len(lines) > 20 {
		t.Fatalf("折叠未生效，输出 %d 行:\n%s", len(lines), rendered)
	}
}

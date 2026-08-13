package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
)

// printTable renders rows as an aligned text table (similar to kubectl get).
func printTable(headers []string, rows [][]string) {
	// 逻辑说明：将表头和每行用 tabwriter 对齐写到 stdout，并在末尾 Flush 保证交互式 CLI 立即可见。
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, strings.Join(headers, "\t"))
	for _, row := range rows {
		fmt.Fprintln(w, strings.Join(row, "\t"))
	}
	w.Flush()
}

// KeyValue is a label-value pair for detail output.
type KeyValue struct {
	Key   string
	Value string
}

// printDetail renders a single resource in "Key: Value" format.
func printDetail(fields []KeyValue) {
	// 逻辑说明：先计算最长键宽度，再跳过空值按统一宽度输出，资源详情保持紧凑且可读。
	maxKey := 0
	for _, f := range fields {
		if len(f.Key) > maxKey {
			maxKey = len(f.Key)
		}
	}
	for _, f := range fields {
		if f.Value != "" {
			fmt.Printf("%-*s  %s\n", maxKey+1, f.Key+":", f.Value)
		}
	}
}

// printJSON outputs v as indented JSON.
func printJSON(v interface{}) {
	// 逻辑说明：缩进序列化到 stdout；序列化失败只把诊断写 stderr，不输出半截 JSON。
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: marshal JSON: %v\n", err)
		return
	}
	fmt.Println(string(data))
}

// or returns fallback if s is empty.
func or(s, fallback string) string {
	// 逻辑说明：只在空字符串时使用展示 fallback，非空实际状态不被默认文本覆盖。
	if s == "" {
		return fallback
	}
	return s
}

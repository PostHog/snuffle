package snuffle

import "github.com/prometheus/prometheus/promql/parser"

func init() {
	if _, ok := parser.Functions["running_sum"]; ok {
		return
	}
	parser.Functions["running_sum"] = &parser.Function{
		Name:       "running_sum",
		ArgTypes:   []parser.ValueType{parser.ValueTypeVector},
		ReturnType: parser.ValueTypeVector,
	}
}

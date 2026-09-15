package snuffle

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/VictoriaMetrics/metricsql"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/util/annotations"
)

func init() {
	parser.Functions["__snuffle_running_sum"] = &parser.Function{
		Name: "__snuffle_running_sum", ArgTypes: []parser.ValueType{parser.ValueTypeMatrix, parser.ValueTypeScalar}, ReturnType: parser.ValueTypeVector,
	}
	promql.FunctionCalls["__snuffle_running_sum"] = func(v []promql.Vector, m promql.Matrix, _ parser.Expressions, enh *promql.EvalNodeHelper) (promql.Vector, annotations.Annotations) {
		var sum float64
		seen := false
		for _, p := range m[0].Floats {
			if p.T >= int64(v[0][0].F) && !math.IsNaN(p.F) {
				sum += p.F
				seen = true
			}
		}
		if !seen {
			return enh.Out, nil
		}
		return append(enh.Out, promql.Sample{F: sum}), nil
	}
	parser.Functions["running_sum"] = &parser.Function{
		Name: "running_sum", ArgTypes: []parser.ValueType{parser.ValueTypeVector}, ReturnType: parser.ValueTypeVector,
	}
	for _, name := range []string{"increase", "rate", "delta", "irate", "idelta", "default_rollup"} {
		internalName := "__snuffle_" + name
		parser.Functions[internalName] = &parser.Function{
			Name:       internalName,
			ArgTypes:   []parser.ValueType{parser.ValueTypeMatrix, parser.ValueTypeScalar, parser.ValueTypeScalar, parser.ValueTypeScalar, parser.ValueTypeScalar},
			ReturnType: parser.ValueTypeVector,
		}
		if name == "default_rollup" {
			// The engine keeps metric names for last_over_time. Use its name
			// for this internal signature, while retaining the public function.
			parser.Functions[internalName].Name = "last_over_time"
			original := promql.FunctionCalls["last_over_time"]
			rollup := metricsQLRollup(name, original)
			promql.FunctionCalls["last_over_time"] = func(v []promql.Vector, m promql.Matrix, args parser.Expressions, enh *promql.EvalNodeHelper) (promql.Vector, annotations.Annotations) {
				if len(args) > 1 {
					return rollup(v, m, args, enh)
				}
				// The storage adapter preserves stale markers for default_rollup.
				// Public last_over_time ignores those markers.
				series := m[0]
				series.Floats = make([]promql.FPoint, 0, len(m[0].Floats))
				for _, p := range m[0].Floats {
					if !math.IsNaN(p.F) {
						series.Floats = append(series.Floats, p)
					}
				}
				if len(series.Floats)+len(series.Histograms) == 0 {
					return enh.Out, nil
				}
				return original(v, promql.Matrix{series}, args, enh)
			}
		} else {
			promql.FunctionCalls[internalName] = metricsQLRollup(name, promql.FunctionCalls[name])
		}
	}
}

// prepareMetricsQLQuery expands MetricsQL syntax before either execution path.
// Counter functions use separate names so Prometheus SQL optimizations cannot
// replace their calculations with extrapolated PromQL results.
func prepareMetricsQLQuery(query string, step, lookback time.Duration, start, end time.Time) (string, error) {
	expr, err := metricsql.Parse(query)
	if err != nil {
		return "", err
	}
	if step <= 0 {
		step = 5 * time.Minute
	}
	if step < time.Millisecond {
		return "", fmt.Errorf("step must be at least 1ms")
	}
	if lookback <= 0 {
		lookback = 5 * time.Minute
	}
	expr = metricsQLDefaultSelectors(expr)
	var rewriteErr error
	metricsql.VisitAll(expr, func(e metricsql.Expr) {
		if rewriteErr != nil {
			return
		}
		switch e := e.(type) {
		case *metricsql.DurationExpr:
			millis := e.Duration(step.Milliseconds())
			if millis > math.MaxInt64/int64(time.Millisecond) || millis < math.MinInt64/int64(time.Millisecond) {
				rewriteErr = fmt.Errorf("the query duration is too large")
			}
		case *metricsql.RollupExpr:
			// Prometheus requires an explicit subquery step after expressions.
			if _, ok := e.Expr.(*metricsql.MetricExpr); !ok && e.Window != nil && e.Step == nil {
				e.Step = metricsQLDuration(step)
			}
			e.Window = resolveMetricsQLDuration(e.Window, step)
			e.Step = resolveMetricsQLDuration(e.Step, step)
			e.Offset = resolveMetricsQLDuration(e.Offset, step)
		case *metricsql.FuncExpr:
			if strings.HasPrefix(e.Name, "__snuffle_") {
				rewriteErr = fmt.Errorf("function %q is reserved for query execution", e.Name)
				return
			}
			if e.Name == "running_sum" && len(e.Args) == 1 {
				if start.Equal(end) || start.UnixMilli()%step.Milliseconds() != 0 {
					rewriteErr = fmt.Errorf("running_sum requires a range query with start aligned to step")
					return
				}
				if end.Sub(start) > time.Duration(math.MaxInt64)-step {
					rewriteErr = fmt.Errorf("the running_sum range is too large")
					return
				}
				e.Name = "__snuffle_running_sum"
				e.Args = []metricsql.Expr{&metricsql.RollupExpr{
					Expr: e.Args[0], Window: metricsQLDuration(end.Sub(start) + step), Step: metricsQLDuration(step),
				}, &metricsql.NumberExpr{N: float64(start.UnixMilli())}}
				return
			}
			if !metricsql.IsRollupFunc(e.Name) {
				if fn := parser.Functions[e.Name]; fn != nil {
					for i, arg := range e.Args {
						if i < len(fn.ArgTypes) && fn.ArgTypes[i] == parser.ValueTypeVector {
							if _, numeric := arg.(*metricsql.NumberExpr); numeric {
								e.Args[i] = &metricsql.FuncExpr{Name: "vector", Args: []metricsql.Expr{arg}}
							}
						}
					}
				}
				return
			}
			// Most rollups take the series first. Quantile rollups take it last.
			argIndex := 0
			if e.Name == "quantile_over_time" {
				argIndex = 1
			}
			if len(e.Args) <= argIndex {
				return
			}
			r, ok := e.Args[argIndex].(*metricsql.RollupExpr)
			if !ok {
				r = &metricsql.RollupExpr{Expr: e.Args[argIndex]}
				e.Args[argIndex] = r
			}
			implicit := r.Window == nil
			window := step
			if !implicit {
				window = time.Duration(r.Window.Duration(step.Milliseconds())) * time.Millisecond
			}
			if window <= 0 {
				rewriteErr = fmt.Errorf("the query window must be at least 1ms")
				return
			}
			r.Window = metricsQLDuration(window)
			if _, ok := r.Expr.(*metricsql.MetricExpr); !ok && r.Step == nil {
				r.Step = metricsQLDuration(step)
			}
			switch e.Name {
			case "increase", "rate", "delta", "irate", "idelta", "default_rollup":
				if len(e.Args) != 1 || e.KeepMetricNames {
					rewriteErr = fmt.Errorf("%s requires one series argument; keep_metric_names is not supported", e.Name)
					return
				}
				if window > time.Duration(math.MaxInt64)-lookback {
					rewriteErr = fmt.Errorf("the query window is too large")
					return
				}
				// Fetch history before the window for the previous sample and
				// the sample interval. Explicit windows keep their original size.
				r.Window = metricsQLDuration(window + lookback)
				windowArg := window.Milliseconds()
				if implicit && (e.Name == "rate" || e.Name == "default_rollup") {
					windowArg = 0
				}
				e.Name = "__snuffle_" + e.Name
				instant := 0.0
				if start.Equal(end) {
					instant = 1
				}
				e.Args = append(e.Args, &metricsql.NumberExpr{N: float64(windowArg)}, &metricsql.NumberExpr{N: float64(step.Milliseconds())}, &metricsql.NumberExpr{N: float64(lookback.Milliseconds())}, &metricsql.NumberExpr{N: instant})
			}
		}
	})
	if rewriteErr != nil {
		return "", rewriteErr
	}
	return string(expr.AppendString(nil)), nil
}

func metricsQLDuration(d time.Duration) *metricsql.DurationExpr {
	// DurationExpr has no public constructor. Parse only our generated duration.
	e, err := metricsql.Parse("m offset " + fmt.Sprintf("%dms", d.Milliseconds()))
	if err != nil {
		panic(err)
	}
	return e.(*metricsql.RollupExpr).Offset
}

func resolveMetricsQLDuration(d *metricsql.DurationExpr, step time.Duration) *metricsql.DurationExpr {
	if d == nil {
		return nil
	}
	return metricsQLDuration(time.Duration(d.Duration(step.Milliseconds())) * time.Millisecond)
}

func metricsQLRollup(name string, histogramFallback promql.FunctionCall) promql.FunctionCall {
	return func(vectors []promql.Vector, matrix promql.Matrix, args parser.Expressions, enh *promql.EvalNodeHelper) (promql.Vector, annotations.Annotations) {
		ms := args[0].(*parser.MatrixSelector)
		vs := ms.VectorSelector.(*parser.VectorSelector)
		window := int64(vectors[0][0].F)
		step := int64(vectors[1][0].F)
		lookback := int64(vectors[2][0].F)
		points := matrix[0].Floats
		maxPrev := step
		if vectors[3][0].F == 0 {
			maxPrev = metricsQLSampleInterval(points, step)
		}
		maxPrev = min(maxPrev, lookback)
		if window == 0 {
			window = max(step, maxPrev)
			if name == "default_rollup" {
				window = min(window, lookback)
			}
		}
		end := enh.Ts - vs.Offset.Milliseconds()
		if vs.Timestamp != nil {
			end = *vs.Timestamp - vs.OriginalOffset.Milliseconds()
		}
		start := end - window
		first := sort.Search(len(points), func(i int) bool { return points[i].T > start })
		if name == "default_rollup" {
			series := matrix[0]
			series.Floats = points[first:]
			h := series.Histograms
			series.Histograms = h[sort.Search(len(h), func(i int) bool { return h[i].T > start }):]
			if len(series.Floats)+len(series.Histograms) == 0 {
				return enh.Out, nil
			}
			if len(series.Floats) > 0 && math.IsNaN(series.Floats[len(series.Floats)-1].F) {
				if len(series.Histograms) == 0 || series.Histograms[len(series.Histograms)-1].T < series.Floats[len(series.Floats)-1].T {
					return enh.Out, nil
				}
			}
			return histogramFallback(nil, promql.Matrix{series}, args[:1], enh)
		}
		if len(matrix[0].Histograms) > 0 {
			// Native histograms retain the existing Prometheus calculations.
			copySelector := *ms
			copySelector.Range = time.Duration(window) * time.Millisecond
			copySeries := matrix[0]
			copySeries.Floats = points[first:]
			h := copySeries.Histograms
			copySeries.Histograms = h[sort.Search(len(h), func(i int) bool { return h[i].T > start }):]
			return histogramFallback(nil, promql.Matrix{copySeries}, parser.Expressions{&copySelector}, enh)
		}
		value := metricsQLCounterValue(name, points, first, start, maxPrev, lookback)
		if math.IsNaN(value) {
			return enh.Out, nil
		}
		return append(enh.Out, promql.Sample{F: value}), nil
	}
}

// VictoriaMetrics estimates the sample interval with the 0.6 quantile of the
// last 20 intervals, then adds a margin for timestamp variation.
// Reference: VictoriaMetrics v1.152.0, app/vmselect/promql/rollup.go.
func metricsQLSampleInterval(points []promql.FPoint, fallback int64) int64 {
	if len(points) < 2 {
		return fallback
	}
	intervals := make([]int64, 0, 20)
	for i := max(1, len(points)-20); i < len(points); i++ {
		intervals = append(intervals, points[i].T-points[i-1].T)
	}
	sort.Slice(intervals, func(i, j int) bool { return intervals[i] < intervals[j] })
	position := 0.6 * float64(len(intervals)-1)
	index := int(position)
	interval := intervals[index]
	if index+1 < len(intervals) {
		interval += int64(float64(intervals[index+1]-interval) * (position - float64(index)))
	}
	if interval <= 0 {
		return fallback
	}
	switch {
	case interval <= 2000:
		return interval * 5
	case interval <= 4000:
		return interval * 3
	case interval <= 8000:
		return interval * 2
	case interval <= 16000:
		return interval + interval/2
	case interval <= 32000:
		return interval + interval/4
	default:
		return interval + interval/8
	}
}

func metricsQLCounterValue(name string, points []promql.FPoint, first int, start, maxPrev, lookback int64) float64 {
	prev := first - 1
	havePrev := prev >= 0 && points[prev].T > start-maxPrev
	isRate := name == "rate" || name == "irate"
	if first == len(points) {
		return math.NaN()
	}
	if !havePrev && (name == "increase" || name == "delta") && prev >= 0 && points[first].T-points[prev].T < lookback {
		havePrev = true
	}
	begin := first
	baseline := points[first].F
	if havePrev {
		begin = prev
		baseline = points[prev].F
	} else if !isRate {
		var delta float64
		if first+1 < len(points) {
			delta = points[first+1].F - points[first].F
		}
		if math.Abs(baseline) < 10*(math.Abs(delta)+1) {
			baseline = 0
		}
	}
	if name == "irate" || name == "idelta" {
		begin = max(begin, len(points)-2)
		baseline = points[begin].F
		if name == "idelta" && len(points)-begin == 1 {
			baseline = 0
		}
	}
	if isRate && len(points)-begin < 2 {
		return math.NaN()
	}
	value := points[len(points)-1].F - baseline
	if name == "increase" || isRate {
		previous := baseline
		for _, point := range points[begin:] {
			if point.F < previous {
				// MetricsQL treats small drops as partial counter resets.
				if (previous-point.F)*8 < previous {
					value += previous - point.F
				} else {
					value += previous
				}
			}
			previous = point.F
		}
	}
	if isRate {
		value /= float64(points[len(points)-1].T-points[begin].T) / 1000
	}
	return value
}

// MetricsQL returns numeric constants as vectors and removes NaN output points.
func metricsQLValue(value parser.Value) parser.Value {
	switch v := value.(type) {
	case promql.Scalar:
		if math.IsNaN(v.V) {
			return promql.Vector{}
		}
		return promql.Vector{{T: v.T, F: v.V}}
	case promql.Vector:
		out := v[:0]
		for _, p := range v {
			if p.H != nil || !math.IsNaN(p.F) {
				out = append(out, p)
			}
		}
		return out
	case promql.Matrix:
		out := v[:0]
		for _, series := range v {
			points := series.Floats[:0]
			for _, p := range series.Floats {
				if !math.IsNaN(p.F) {
					points = append(points, p)
				}
			}
			series.Floats = points
			if len(points)+len(series.Histograms) > 0 {
				out = append(out, series)
			}
		}
		return out
	default:
		return value
	}
}

// The write interval cannot guarantee sample times for external writers.
// Disable query optimizations that require samples at exact interval boundaries.
func (s *Server) metricsQLServer() *Server {
	copy := *s
	copy.cfg.RemoteWriteInterval = 0
	copy.queryable = NewCHQueryable(s.client, copy.cfg)
	copy.queryable.preserveRollupStaleness = true
	return &copy
}

// Raw selectors are rollup inputs. Selectors in vector expressions use
// default_rollup, including selectors inside aggregates and binary operations.
func metricsQLDefaultSelectors(expr metricsql.Expr) metricsql.Expr {
	switch e := expr.(type) {
	case *metricsql.MetricExpr:
		return &metricsql.FuncExpr{Name: "default_rollup", Args: []metricsql.Expr{e}}
	case *metricsql.RollupExpr:
		if _, ok := e.Expr.(*metricsql.MetricExpr); ok {
			if e.Window == nil && !e.ForSubquery() {
				return &metricsql.FuncExpr{Name: "default_rollup", Args: []metricsql.Expr{e}}
			}
		} else {
			e.Expr = metricsQLDefaultSelectors(e.Expr)
		}
	case *metricsql.FuncExpr:
		if metricsql.IsRollupFunc(e.Name) {
			for i, arg := range e.Args {
				if r, ok := arg.(*metricsql.RollupExpr); ok {
					if _, raw := r.Expr.(*metricsql.MetricExpr); !raw {
						r.Expr = metricsQLDefaultSelectors(r.Expr)
					}
				} else if _, raw := arg.(*metricsql.MetricExpr); !raw {
					e.Args[i] = metricsQLDefaultSelectors(arg)
				}
			}
		} else if e.Name != "timestamp" {
			for i, arg := range e.Args {
				e.Args[i] = metricsQLDefaultSelectors(arg)
			}
		}
	case *metricsql.AggrFuncExpr:
		for i, arg := range e.Args {
			e.Args[i] = metricsQLDefaultSelectors(arg)
		}
	case *metricsql.BinaryOpExpr:
		e.Left = metricsQLDefaultSelectors(e.Left)
		e.Right = metricsQLDefaultSelectors(e.Right)
	}
	return expr
}

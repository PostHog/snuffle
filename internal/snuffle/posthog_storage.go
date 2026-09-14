package snuffle

import (
	"context"
	"fmt"
	"strings"

	"github.com/prometheus/prometheus/model/labels"
)

// PostHog metrics live in two tables: metric_series2 holds one row per series
// (labels keyed by series_fingerprint) and metrics2 holds the samples keyed by
// the same fingerprint. Queries select series from the series table, then
// read samples by fingerprint and join the labels back in.

const postHogSeriesLabelColumns = "metric_name, service_name, resource_attributes, attributes"

func (q *CHQuerier) selectPostHogSeries(ctx context.Context, mint, maxt int64, matchers ...*labels.Matcher) ([]*seriesMeta, error) {
	sql := postHogSelectedSeriesSQL(q.queryable.cfg, matchers, mint, maxt, q.queryable.cfg.MaxSeries, nil)
	series := make([]*seriesMeta, 0, 1024)
	err := q.queryable.client.QueryRows(ctx, sql, func(row clickHouseRow) error {
		meta, err := scanPostHogSeries(row, matchers)
		if err != nil || meta == nil {
			return err
		}
		series = append(series, meta)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(series) >= q.queryable.cfg.MaxSeries {
		return nil, fmt.Errorf("series limit exceeded (%d); tighten matchers or increase CH_MAX_SERIES", q.queryable.cfg.MaxSeries)
	}
	return series, nil
}

func (q *CHQuerier) selectPostHogSeriesSamples(ctx context.Context, mint, maxt int64, latestOnly bool, matchers ...*labels.Matcher) ([]*seriesMeta, error) {
	sql := postHogSeriesSamplesSQL(q.queryable.cfg, matchers, mint, maxt, latestOnly)
	series := make([]*seriesMeta, 0, 1024)
	byID := make(map[uint64]*seriesMeta, 1024)
	err := q.queryable.client.QueryRows(ctx, sql, func(row clickHouseRow) error {
		var id uint64
		var metricName string
		var serviceName string
		var resourceAttrs map[string]string
		var attrs map[string]string
		var ts int64
		var value float64
		if err := row.Scan(&id, &metricName, &serviceName, &resourceAttrs, &attrs, &ts, &value); err != nil {
			return err
		}
		meta := byID[id]
		if meta == nil {
			labelMap := postHogLabelMap(metricName, serviceName, resourceAttrs, attrs)
			if !matchesAll(labelMap, matchers) {
				return nil
			}
			if len(series) >= q.queryable.cfg.MaxSeries {
				return fmt.Errorf("series limit exceeded (%d); tighten matchers or increase CH_MAX_SERIES", q.queryable.cfg.MaxSeries)
			}
			meta = &seriesMeta{
				id:         id,
				metricName: metricName,
				labelMap:   labelMap,
				labels:     labels.FromMap(labelMap),
			}
			byID[id] = meta
			series = append(series, meta)
		}
		meta.samples = append(meta.samples, samplePoint{t: ts, v: value})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return series, nil
}

func scanPostHogSeries(row clickHouseRow, matchers []*labels.Matcher) (*seriesMeta, error) {
	var id uint64
	var metricName string
	var serviceName string
	var resourceAttrs map[string]string
	var attrs map[string]string
	if err := row.Scan(&id, &metricName, &serviceName, &resourceAttrs, &attrs); err != nil {
		return nil, err
	}
	labelMap := postHogLabelMap(metricName, serviceName, resourceAttrs, attrs)
	if !matchesAll(labelMap, matchers) {
		return nil, nil
	}
	return &seriesMeta{
		id:         id,
		metricName: metricName,
		labelMap:   labelMap,
		labels:     labels.FromMap(labelMap),
	}, nil
}

func (q *CHQuerier) loadPostHogSamples(ctx context.Context, series []*seriesMeta, mint, maxt int64, latestOnly bool, matchers []*labels.Matcher) error {
	if len(series) == 0 {
		return nil
	}
	byID, ids := seriesIndex(series)
	for _, batch := range idBatches(ids, q.queryable.cfg.IDChunkSize) {
		metricNames := make(map[string]struct{}, 8)
		for _, id := range batch {
			if s := byID[id]; s != nil {
				metricNames[s.metricName] = struct{}{}
			}
		}
		sql := postHogLoadSamplesSQL(q.queryable.cfg, batch, sortedLimited(metricNames, 0), matchers, mint, maxt, latestOnly)
		if err := q.queryable.client.QueryRows(ctx, sql, func(row clickHouseRow) error {
			var id uint64
			var ts int64
			var value float64
			if err := row.Scan(&id, &ts, &value); err != nil {
				return err
			}
			if s := byID[id]; s != nil {
				s.samples = append(s.samples, samplePoint{t: ts, v: value})
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

func postHogLoadSamplesSQL(cfg Config, ids []uint64, metricNames []string, matchers []*labels.Matcher, mint, maxt int64, latestOnly bool) string {
	where := postHogSampleFilters(cfg, matchers, mint, maxt)
	if exactMetricName(matchers) == "" {
		if condition := metricNamesCondition(metricNames); condition != "" {
			where = append(where, condition)
		}
	}
	where = append(where, "series_fingerprint IN ("+joinUint64(ids)+")")
	source := fmt.Sprintf(
		"SELECT series_fingerprint AS series_id, timestamp, value FROM %s WHERE %s",
		postHogSamplesTable(cfg),
		strings.Join(where, " AND "),
	)
	if latestOnly {
		latest := fmt.Sprintf(
			"SELECT series_id, max(timestamp) AS ts_col, argMax(value, timestamp) AS value FROM (%s) GROUP BY series_id",
			source,
		)
		return fmt.Sprintf(
			"SELECT series_id, toUnixTimestamp64Milli(ts_col) AS ts, value FROM (%s) WHERE %s ORDER BY series_id",
			latest,
			nonStaleSampleSQL("value"),
		)
	}
	return fmt.Sprintf(
		"SELECT series_id, toUnixTimestamp64Milli(timestamp) AS ts, value FROM (%s) ORDER BY series_id, timestamp",
		source,
	)
}

func postHogSeriesSamplesSQL(cfg Config, matchers []*labels.Matcher, mint, maxt int64, latestOnly bool) string {
	plan := newPostHogQueryPlan(cfg, matchers, nil, mint, maxt, true)
	if latestOnly && postHogExactSampleTimestamp(cfg, maxt) {
		plan.mint = maxt
	}
	var perSeries string
	if latestOnly {
		perSeries = fmt.Sprintf(
			"SELECT series_fingerprint AS series_id, max(timestamp) AS ts_col, argMax(value, timestamp) AS value FROM %s WHERE %s GROUP BY series_id",
			postHogSamplesTable(cfg),
			strings.Join(plan.sampleWhere(), " AND "),
		)
		perSeries = fmt.Sprintf("SELECT series_id, ts_col AS timestamp, value FROM (%s) WHERE %s", perSeries, nonStaleSampleSQL("value"))
	} else {
		perSeries = fmt.Sprintf(
			"SELECT series_fingerprint AS series_id, timestamp, value FROM %s WHERE %s",
			postHogSamplesTable(cfg),
			strings.Join(plan.sampleWhere(), " AND "),
		)
	}
	sql := fmt.Sprintf(
		"SELECT series_id, %s, toUnixTimestamp64Milli(timestamp) AS ts, value FROM %s ORDER BY series_id, timestamp",
		postHogSeriesLabelColumns,
		plan.joinSeries(perSeries),
	)
	return plan.withSelectedSeries(sql)
}

// postHogSelectedSeriesSQL returns one row per series matching the matchers,
// with the label columns and any extra select expressions. The series table
// is a ReplacingMergeTree keyed by fingerprint, so unmerged duplicates are
// collapsed with LIMIT 1 BY.
func postHogSelectedSeriesSQL(cfg Config, matchers []*labels.Matcher, mint, maxt int64, limit int, extraSelects []string) string {
	return postHogSelectedSeriesWhereSQL(cfg, postHogSeriesFilters(cfg, matchers, mint, maxt), limit, extraSelects)
}

func postHogSeriesTable(cfg Config) string {
	return tableName(cfg.CHDatabase, cfg.SeriesTable)
}

func postHogSamplesTable(cfg Config) string {
	return tableName(cfg.CHDatabase, cfg.SamplesTable)
}

// postHogSeriesFilters filters the series table. last_seen is the newest
// sample time of a series, so a series last seen before mint has no samples
// in the window.
func postHogSeriesFilters(cfg Config, matchers []*labels.Matcher, mint, _ int64) []string {
	filters := []string{teamFilter(cfg), "last_seen >= " + chTimeMillis(mint)}
	for _, matcher := range matchers {
		if matcherIsNoop(matcher) || postHogMatcherCanSkip(matcher) {
			continue
		}
		if condition, ok := postHogMatcherCondition(matcher); ok {
			filters = append(filters, condition)
		}
	}
	return filters
}

// postHogSampleFilters filters the samples table. Only metric_name and
// service_name exist there; other label matchers go through the series
// table.
func postHogSampleFilters(cfg Config, matchers []*labels.Matcher, mint, maxt int64) []string {
	filters := []string{teamFilter(cfg)}
	filters = append(filters, sampleTimeFilters(cfg, mint, maxt)...)
	for _, matcher := range matchers {
		if matcherIsNoop(matcher) || postHogMatcherCanSkip(matcher) {
			continue
		}
		if condition, ok := postHogSampleMatcherCondition(matcher); ok {
			filters = append(filters, condition)
		}
	}
	return filters
}

func postHogExactSampleTimestamp(cfg Config, ts int64) bool {
	return cfg.RemoteWriteInterval > 0 && bucketTimestampMS(ts, cfg.RemoteWriteInterval) == ts
}

func postHogMatcherCanSkip(matcher *labels.Matcher) bool {
	switch matcher.Type {
	case labels.MatchEqual, labels.MatchRegexp:
		return matcher.Matches("")
	default:
		return false
	}
}

// postHogMatcherCondition builds a condition against the series table, where
// every label is available.
func postHogMatcherCondition(matcher *labels.Matcher) (string, bool) {
	return postHogMatcherConditionWith(matcher, postHogSeriesLabelExpr)
}

// postHogSampleMatcherCondition builds a condition against the samples table,
// which only carries metric_name and service_name.
func postHogSampleMatcherCondition(matcher *labels.Matcher) (string, bool) {
	return postHogMatcherConditionWith(matcher, postHogSampleLabelExpr)
}

func postHogMatcherConditionWith(matcher *labels.Matcher, labelExpr func(string) (string, bool)) (string, bool) {
	if matcher.Name == labels.MetricName {
		return metricMatcherCondition(matcher)
	}
	expr, ok := labelExpr(matcher.Name)
	if !ok {
		return "", false
	}
	return stringColumnMatcherCondition(expr, matcher)
}

func postHogSampleColumnLabel(name string) bool {
	return name == labels.MetricName || name == "service_name"
}

func postHogSampleLabelExpr(name string) (string, bool) {
	switch name {
	case labels.MetricName:
		return "metric_name", true
	case "service_name":
		return "service_name", true
	default:
		return "", false
	}
}

func postHogSeriesLabelExpr(name string) (string, bool) {
	switch name {
	case labels.MetricName:
		return "metric_name", true
	case "service_name":
		return "service_name", true
	case "":
		return "", false
	default:
		return postHogLabelValueExpr(name), true
	}
}

func postHogLabelValueExpr(name string) string {
	key := sqlString(name)
	return "if(mapContains(attributes, " + key + "), attributes[" + key + "], resource_attributes[" + key + "])"
}

func postHogMatchersPushdownSafe(matchers []*labels.Matcher) bool {
	if len(matchers) == 0 {
		return false
	}
	for _, matcher := range matchers {
		if matcherIsNoop(matcher) {
			continue
		}
		if _, ok := postHogMatcherCondition(matcher); !ok {
			return false
		}
		switch matcher.Type {
		case labels.MatchEqual, labels.MatchRegexp:
			if matcher.Name != labels.MetricName && matcher.Matches("") {
				return false
			}
		case labels.MatchNotEqual, labels.MatchNotRegexp:
			if matcher.Name != labels.MetricName && !matcher.Matches("") {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// postHogNeedsSeriesLookup reports whether a query must consult the series
// table: it does when a matcher or grouping label is not a samples column.
func postHogNeedsSeriesLookup(matchers []*labels.Matcher, grouping []string) bool {
	for _, matcher := range matchers {
		if matcherIsNoop(matcher) || postHogMatcherCanSkip(matcher) {
			continue
		}
		if !postHogSampleColumnLabel(matcher.Name) {
			return true
		}
	}
	for _, name := range grouping {
		if !postHogSampleColumnLabel(name) {
			return true
		}
	}
	return false
}

// postHogQueryPlan decides where a PostHog metrics query reads labels from.
// When useSeries is set, a selected_series CTE over the series table carries
// the matched fingerprints and any grouping labels, the samples scan is
// restricted to those fingerprints, and per-series rows join the CTE. When it
// is not set, every label the query needs is a samples column and the series
// table is not read.
type postHogQueryPlan struct {
	cfg        Config
	matchers   []*labels.Matcher
	grouping   []string
	mint, maxt int64
	useSeries  bool
	seriesSQL  string
}

func newPostHogQueryPlan(cfg Config, matchers []*labels.Matcher, grouping []string, mint, maxt int64, needLabels bool) *postHogQueryPlan {
	plan := &postHogQueryPlan{cfg: cfg, matchers: matchers, grouping: grouping, mint: mint, maxt: maxt}
	plan.useSeries = needLabels || postHogNeedsSeriesLookup(matchers, grouping)
	if plan.useSeries {
		plan.seriesSQL = postHogSelectedSeriesSQL(cfg, matchers, mint, maxt, cfg.MaxSeries, plan.seriesGroupSelects())
	}
	return plan
}

func (p *postHogQueryPlan) seriesGroupSelects() []string {
	selects := make([]string, 0, len(p.grouping))
	for i, name := range p.grouping {
		expr, _ := postHogSeriesLabelExpr(name)
		selects = append(selects, expr+" AS "+quoteIdent(groupAlias(i)))
	}
	return selects
}

func (p *postHogQueryPlan) withSelectedSeries(sql string) string {
	if !p.useSeries {
		return sql
	}
	return "WITH selected_series AS (" + p.seriesSQL + ") " + sql
}

// sampleWhere filters the samples scan: team, time, samples-column matchers
// and, with a series lookup, membership in the selected fingerprints. The
// metric_name membership keeps the primary key usable when the matchers do
// not pin the metric name.
func (p *postHogQueryPlan) sampleWhere() []string {
	where := postHogSampleFilters(p.cfg, p.matchers, p.mint, p.maxt)
	if p.useSeries {
		where = append(where, "series_fingerprint IN (SELECT series_id FROM selected_series)")
		if exactMetricName(p.matchers) == "" {
			where = append(where, "metric_name IN (SELECT metric_name FROM selected_series)")
		}
	}
	return where
}

// perSeriesGroupSelects returns the grouping labels a per-series samples
// aggregate must carry when no series lookup runs. They are samples columns,
// constant per fingerprint.
func (p *postHogQueryPlan) perSeriesGroupSelects() []string {
	if p.useSeries {
		return nil
	}
	selects := make([]string, 0, len(p.grouping))
	for i, name := range p.grouping {
		expr, _ := postHogSampleLabelExpr(name)
		selects = append(selects, "any("+expr+") AS "+quoteIdent(groupAlias(i)))
	}
	return selects
}

// rowGroupSelects returns the grouping labels a raw samples projection must
// carry when no series lookup runs.
func (p *postHogQueryPlan) rowGroupSelects() []string {
	if p.useSeries {
		return nil
	}
	selects := make([]string, 0, len(p.grouping))
	for i, name := range p.grouping {
		expr, _ := postHogSampleLabelExpr(name)
		selects = append(selects, expr+" AS "+quoteIdent(groupAlias(i)))
	}
	return selects
}

func (p *postHogQueryPlan) groupAliases() []string {
	aliases := make([]string, 0, len(p.grouping))
	for i := range p.grouping {
		aliases = append(aliases, quoteIdent(groupAlias(i)))
	}
	return aliases
}

// joinSeries wraps a per-series subquery (which selects series_id) and joins
// the selected series labels onto it when the plan reads the series table.
func (p *postHogQueryPlan) joinSeries(inner string) string {
	if !p.useSeries {
		return "(" + inner + ")"
	}
	return "(" + inner + ") AS samples INNER JOIN selected_series USING series_id"
}

func postHogLabelMap(metricName, serviceName string, resourceAttrs, attrs map[string]string) map[string]string {
	out := make(map[string]string, len(resourceAttrs)+len(attrs)+2)
	for key, value := range resourceAttrs {
		if key == "" {
			continue
		}
		out[key] = value
	}
	if serviceName != "" {
		out["service_name"] = serviceName
	}
	for key, value := range attrs {
		if key == "" || key == labels.MetricName {
			continue
		}
		out[key] = value
	}
	out[labels.MetricName] = metricName
	return out
}

func (q *CHQuerier) postHogLabelNames(ctx context.Context, limit int, matchers ...*labels.Matcher) ([]string, error) {
	if len(matchers) > 0 {
		series, err := q.selectPostHogSeries(ctx, q.mint, q.maxt, matchers...)
		if err != nil {
			return nil, err
		}
		names := map[string]struct{}{labels.MetricName: {}}
		for _, s := range series {
			for name := range s.labelMap {
				names[name] = struct{}{}
			}
		}
		return sortedLimited(names, limit), nil
	}
	names := map[string]struct{}{labels.MetricName: {}, "service_name": {}}
	var sql string
	if attrTable := q.queryable.cfg.AttributeTable; attrTable != "" {
		sql = fmt.Sprintf(
			"SELECT DISTINCT attribute_key FROM %s WHERE %s AND %s ORDER BY attribute_key%s",
			tableName(q.queryable.cfg.CHDatabase, attrTable),
			teamFilter(q.queryable.cfg),
			strings.Join(postHogAttributeTimeFilters(q.mint, q.maxt), " AND "),
			sqlLimit(limit),
		)
	} else {
		sql = fmt.Sprintf(
			"SELECT DISTINCT arrayJoin(arrayConcat(mapKeys(attributes), mapKeys(resource_attributes))) AS attribute_key FROM %s WHERE %s ORDER BY attribute_key%s",
			postHogSeriesTable(q.queryable.cfg),
			strings.Join(postHogSeriesFilters(q.queryable.cfg, nil, q.mint, q.maxt), " AND "),
			sqlLimit(limit),
		)
	}
	if err := q.addStringRows(ctx, names, sql); err != nil {
		return nil, err
	}
	return sortedLimited(names, limit), nil
}

func (q *CHQuerier) postHogLabelValues(ctx context.Context, name string, limit int, matchers ...*labels.Matcher) ([]string, error) {
	if len(matchers) > 0 {
		series, err := q.selectPostHogSeries(ctx, q.mint, q.maxt, matchers...)
		if err != nil {
			return nil, err
		}
		values := make(map[string]struct{})
		for _, s := range series {
			if value, ok := s.labelMap[name]; ok {
				values[value] = struct{}{}
			}
		}
		return sortedLimited(values, limit), nil
	}
	values := make(map[string]struct{})
	var sql string
	switch {
	case postHogSampleColumnLabel(name):
		column, _ := postHogSeriesLabelExpr(name)
		sql = fmt.Sprintf(
			"SELECT DISTINCT %s AS label_value FROM %s WHERE %s ORDER BY label_value%s",
			column,
			postHogSeriesTable(q.queryable.cfg),
			strings.Join(postHogSeriesFilters(q.queryable.cfg, nil, q.mint, q.maxt), " AND "),
			sqlLimit(limit),
		)
	case q.queryable.cfg.AttributeTable != "":
		sql = fmt.Sprintf(
			"SELECT DISTINCT attribute_value AS label_value FROM %s WHERE %s AND attribute_key = %s AND %s ORDER BY label_value%s",
			tableName(q.queryable.cfg.CHDatabase, q.queryable.cfg.AttributeTable),
			teamFilter(q.queryable.cfg),
			sqlString(name),
			strings.Join(postHogAttributeTimeFilters(q.mint, q.maxt), " AND "),
			sqlLimit(limit),
		)
	default:
		sql = fmt.Sprintf(
			"SELECT DISTINCT %s AS label_value FROM %s WHERE %s AND label_value != '' ORDER BY label_value%s",
			postHogLabelValueExpr(name),
			postHogSeriesTable(q.queryable.cfg),
			strings.Join(postHogSeriesFilters(q.queryable.cfg, nil, q.mint, q.maxt), " AND "),
			sqlLimit(limit),
		)
	}
	if err := q.addStringRows(ctx, values, sql); err != nil {
		return nil, err
	}
	return sortedLimited(values, limit), nil
}

// postHogAttributeTimeFilters bounds the attribute rollup, which buckets by
// hour.
func postHogAttributeTimeFilters(mint, maxt int64) []string {
	return []string{
		"time_bucket >= toStartOfHour(" + chTimeMillis(mint) + ")",
		"time_bucket <= toStartOfHour(" + chTimeMillis(maxt) + ")",
	}
}

// postHogSelectedSeriesWhereSQL is postHogSelectedSeriesSQL with the series
// table filters supplied by the caller.
func postHogSelectedSeriesWhereSQL(cfg Config, where []string, limit int, extraSelects []string) string {
	selects := []string{"series_fingerprint AS series_id", postHogSeriesLabelColumns}
	selects = append(selects, extraSelects...)
	return fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s LIMIT 1 BY series_id%s",
		strings.Join(selects, ", "),
		postHogSeriesTable(cfg),
		strings.Join(where, " AND "),
		sqlLimit(limit),
	)
}

// postHogSampleRowsSQL projects raw sample rows with their fingerprint as
// series_id, the requested columns, and grouping labels when the plan reads
// them from the samples table.
func postHogSampleRowsSQL(cfg Config, plan *postHogQueryPlan, columns []string, where []string) string {
	selects := []string{"series_fingerprint AS series_id"}
	selects = append(selects, columns...)
	selects = append(selects, plan.rowGroupSelects()...)
	return fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s",
		strings.Join(selects, ", "),
		postHogSamplesTable(cfg),
		strings.Join(where, " AND "),
	)
}

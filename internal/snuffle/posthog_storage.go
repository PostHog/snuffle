package snuffle

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/prometheus/prometheus/model/labels"
)

// PostHog metrics live in two tables: metric_series3 holds the labels of each
// series (keyed by series_fingerprint, one row per expiry day) and metrics2
// holds the samples keyed by the same fingerprint. Queries select series from
// the series table, then read samples by fingerprint and join the labels back
// in. Two hourly rollups answer discovery without a series scan:
// metric_attributes3 has one row per metric, service, attribute pair and hour,
// and metric_names3 has one row per metric name and hour.

const postHogSeriesLabelColumns = "metric_name, service_name, resource_attributes, attributes"

// postHogLabelWindowMillis is the longest gap between two labelled rows of a
// series. Ingestion sends the labels of a series on one row per hour at most,
// and only those rows reach the series table and the hourly rollups, so
// last_seen and time_bucket can trail the newest sample by up to an hour.
const postHogLabelWindowMillis = int64(time.Hour / time.Millisecond)

func (q *CHQuerier) selectPostHogSeries(ctx context.Context, mint, maxt int64, matchers ...*labels.Matcher) ([]*seriesMeta, error) {
	if alias, ok := postHogExactHistogramAlias(matchers); ok {
		return q.selectPostHogExactHistogramSeries(ctx, mint, maxt, alias, matchers, false, false)
	}
	series, err := q.selectPostHogFloatSeries(ctx, mint, maxt, matchers...)
	if err != nil {
		return nil, err
	}
	return q.appendPostHogHistogramSeries(ctx, series, mint, maxt, matchers, false)
}

// selectPostHogSeriesSamples reads the series table once for labels, then
// reads samples by fingerprint. Joining labels onto the samples scan shipped
// both label maps on every sample row, and decoding those maps in Go
// dominated range queries over many series.
func (q *CHQuerier) selectPostHogSeriesSamples(ctx context.Context, mint, maxt int64, latestOnly bool, matchers ...*labels.Matcher) ([]*seriesMeta, error) {
	if alias, ok := postHogExactHistogramAlias(matchers); ok {
		return q.selectPostHogExactHistogramSeries(ctx, mint, maxt, alias, matchers, true, latestOnly)
	}
	series, err := q.selectPostHogFloatSeries(ctx, mint, maxt, matchers...)
	if err != nil {
		return nil, err
	}
	if err := q.loadPostHogSamples(ctx, series, mint, maxt, latestOnly, matchers); err != nil {
		return nil, err
	}
	// A series can be selected by last_seen and still have no sample in the
	// window (a query in the past, or a stale latest sample). Drop it, as the
	// former join did.
	series = slices.DeleteFunc(series, func(meta *seriesMeta) bool { return len(meta.samples) == 0 })
	return q.appendPostHogHistogramSeries(ctx, series, mint, maxt, matchers, true)
}

// selectPostHogFloatSeries returns the float series matching the matchers,
// with labels but without samples.
func (q *CHQuerier) selectPostHogFloatSeries(ctx context.Context, mint, maxt int64, matchers ...*labels.Matcher) ([]*seriesMeta, error) {
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
	return queryPostHogSeriesIDs(ctx, q.queryable.client, q.queryable.cfg.IDChunkSize, ids, func(batch []uint64, idCondition string) string {
		metricNames := make(map[string]struct{}, 8)
		for _, id := range batch {
			if s := byID[id]; s != nil {
				metricNames[s.metricName] = struct{}{}
			}
		}
		return postHogLoadSamplesWhereSQL(q.queryable.cfg, idCondition, sortedLimited(metricNames, 0), matchers, mint, maxt, latestOnly)
	}, postHogSampleRowHandler(byID, latestOnly))
}

// postHogSampleRowHandler decodes each row as one series.
// It uses scalar columns when the query keeps one sample for each series.
func postHogSampleRowHandler(byID map[uint64]*seriesMeta, latestOnly bool) func(clickHouseRow) error {
	if latestOnly {
		return sampleRowHandler(byID)
	}
	return func(row clickHouseRow) error {
		var id uint64
		var firstTS int64
		var deltas []int64
		var values []float64
		if err := row.Scan(&id, &firstTS, &deltas, &values); err != nil {
			return err
		}
		s := byID[id]
		if s == nil {
			return nil
		}
		samples, err := decodeDeltaSamples(firstTS, deltas, values, s.samples)
		if err != nil {
			return fmt.Errorf("series %d: %w", id, err)
		}
		s.samples = samples
		sortSamples(s.samples)
		return nil
	}
}

// decodeDeltaSamples appends the samples of one series to the input slice.
// The timestamps travel as the first timestamp plus the difference to each predecessor.
// The first difference is zero.
func decodeDeltaSamples(firstTS int64, deltas []int64, values []float64, samples []samplePoint) ([]samplePoint, error) {
	if len(deltas) != len(values) {
		return nil, fmt.Errorf("delta samples have %d timestamp deltas and %d values", len(deltas), len(values))
	}
	samples = slices.Grow(samples, len(values))
	ts := firstTS
	for index, value := range values {
		if index > 0 {
			ts += deltas[index]
		}
		samples = append(samples, samplePoint{t: ts, v: value})
	}
	return samples, nil
}

const postHogSeriesIDsTable = "series_ids"

// queryPostHogSeriesIDs runs the SQL from sqlFor over the fingerprints in ids.
// Small sets go as literal IN lists in chunks. A literal list of more than
// chunkSize fingerprints can exceed the ClickHouse max_query_size, so larger
// sets are sent once as an external table that the SQL reads with a subquery.
func queryPostHogSeriesIDs(ctx context.Context, client *ClickHouseClient, chunkSize int, ids []uint64, sqlFor func(batch []uint64, idCondition string) string, handle func(clickHouseRow) error) error {
	if len(ids) > chunkSize {
		sql := sqlFor(ids, "series_fingerprint IN (SELECT id FROM "+postHogSeriesIDsTable+")")
		return client.QueryRowsWithExternalUInt64s(ctx, postHogSeriesIDsTable, "id", ids, sql, handle)
	}
	for _, batch := range idBatches(ids, chunkSize) {
		if err := client.QueryRows(ctx, sqlFor(batch, "series_fingerprint IN ("+joinUint64(batch)+")"), handle); err != nil {
			return err
		}
	}
	return nil
}

func postHogLoadSamplesSQL(cfg Config, ids []uint64, metricNames []string, matchers []*labels.Matcher, mint, maxt int64, latestOnly bool) string {
	return postHogLoadSamplesWhereSQL(cfg, "series_fingerprint IN ("+joinUint64(ids)+")", metricNames, matchers, mint, maxt, latestOnly)
}

func postHogLoadSamplesWhereSQL(cfg Config, idCondition string, metricNames []string, matchers []*labels.Matcher, mint, maxt int64, latestOnly bool) string {
	where := postHogSampleFilters(cfg, matchers, mint, maxt)
	if exactMetricName(matchers) == "" {
		if condition := metricNamesCondition(metricNames); condition != "" {
			where = append(where, condition)
		}
	}
	where = append(where, idCondition)
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
	// ClickHouse returns one row for each series.
	// ClickHouse sorts the samples of a series into arrays.
	// Go decodes typed arrays instead of three values for each sample.
	// Timestamps travel as deltas: typed columns of small integers compress
	// several times better than an opaque RowBinary string.
	return withMaxThreads(fmt.Sprintf(
		"SELECT series_id, points[1].1 AS first_ts, arrayDifference(arrayMap(p -> p.1, points)) AS ts_deltas, arrayMap(p -> p.2, points) AS vals "+
			"FROM (SELECT series_id, arraySort(groupArray((toUnixTimestamp64Milli(timestamp), value))) AS points FROM (%s) GROUP BY series_id)",
		source,
	), cfg.RangeQueryThreads)
}

// postHogSelectedSeriesSQL returns one row per series matching the matchers,
// with the label columns and any extra select expressions. The series table
// is a ReplacingMergeTree keyed by fingerprint and partitioned by expiry day,
// so unmerged duplicates and rows from other expiry days are collapsed with
// LIMIT 1 BY.
func postHogSelectedSeriesSQL(cfg Config, matchers []*labels.Matcher, mint, maxt int64, limit int, extraSelects []string) string {
	return postHogSelectedSeriesWhereSQL(cfg, postHogSeriesFilters(cfg, matchers, mint, maxt), limit, extraSelects)
}

func postHogSeriesTable(cfg Config) string {
	return tableName(cfg.CHDatabase, cfg.SeriesTable)
}

func postHogSamplesTable(cfg Config) string {
	return tableName(cfg.CHDatabase, cfg.SamplesTable)
}

func postHogAttributeTable(cfg Config) string {
	return tableName(cfg.CHDatabase, cfg.AttributeTable)
}

func postHogMetricNamesTable(cfg Config) string {
	return tableName(cfg.CHDatabase, cfg.MetricNamesTable)
}

// postHogSeriesFilters filters the series table. last_seen is the time of the
// newest labelled row of a series, which can be up to a label window before
// its newest sample, so a series last seen a window before mint has no
// samples in the window.
func postHogSeriesFilters(cfg Config, matchers []*labels.Matcher, mint, _ int64) []string {
	filters := []string{teamFilter(cfg), "last_seen >= " + chTimeMillis(mint-postHogLabelWindowMillis)}
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

// postHogLabelValueExpr reads a map label with the same precedence as
// postHogLabelMap: a resource attribute wins over a metric attribute.
func postHogLabelValueExpr(name string) string {
	key := sqlString(name)
	return "if(mapContains(resource_attributes, " + key + "), resource_attributes[" + key + "], attributes[" + key + "])"
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
	for key, value := range attrs {
		if key == "" || key == labels.MetricName {
			continue
		}
		out[key] = value
	}
	for key, value := range resourceAttrs {
		if key == "" || key == labels.MetricName {
			continue
		}
		out[key] = value
	}
	if serviceName != "" {
		out["service_name"] = serviceName
	}
	out[labels.MetricName] = metricName
	return out
}

func (q *CHQuerier) postHogLabelNames(ctx context.Context, limit int, matchers ...*labels.Matcher) ([]string, error) {
	sql, ok := postHogLabelNamesSQL(q.queryable.cfg, q.mint, q.maxt, limit, matchers)
	if !ok {
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
	if err := q.addStringRows(ctx, names, sql); err != nil {
		return nil, err
	}
	virtual, err := q.postHogHistogramLabelValues(ctx, "", matchers)
	if err != nil {
		return nil, err
	}
	for _, name := range virtual {
		names[name] = struct{}{}
	}
	return sortedLimited(names, limit), nil
}

// postHogLabelNamesSQL lists attribute keys from the attribute rollup. It
// reports false when the matchers need a series scan.
func postHogLabelNamesSQL(cfg Config, mint, maxt int64, limit int, matchers []*labels.Matcher) (string, bool) {
	if cfg.AttributeTable == "" {
		if len(matchers) > 0 {
			return "", false
		}
		return fmt.Sprintf(
			"SELECT DISTINCT arrayJoin(arrayConcat(mapKeys(attributes), mapKeys(resource_attributes))) AS attribute_key FROM %s WHERE %s ORDER BY attribute_key%s",
			postHogSeriesTable(cfg),
			strings.Join(postHogSeriesFilters(cfg, nil, mint, maxt), " AND "),
			sqlLimit(limit),
		), true
	}
	where, ok := postHogAttributeFilters(cfg, mint, maxt, matchers)
	if !ok {
		return "", false
	}
	return fmt.Sprintf(
		"SELECT DISTINCT attribute_key FROM %s WHERE %s ORDER BY attribute_key%s",
		postHogAttributeTable(cfg),
		strings.Join(where, " AND "),
		sqlLimit(limit),
	), true
}

func (q *CHQuerier) postHogLabelValues(ctx context.Context, name string, limit int, matchers ...*labels.Matcher) ([]string, error) {
	sql, ok := postHogLabelValuesSQL(q.queryable.cfg, name, q.mint, q.maxt, limit, matchers)
	if !ok {
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
	if err := q.addStringRows(ctx, values, sql); err != nil {
		return nil, err
	}
	virtual, err := q.postHogHistogramLabelValues(ctx, name, matchers)
	if err != nil {
		return nil, err
	}
	for _, value := range virtual {
		values[value] = struct{}{}
	}
	return sortedLimited(values, limit), nil
}

// postHogLabelValuesSQL lists the values of one label. Metric names come from
// the metric name rollup, other map labels from the attribute rollup, and the
// rest from the series table. It reports false when the matchers need a
// series scan.
func postHogLabelValuesSQL(cfg Config, name string, mint, maxt int64, limit int, matchers []*labels.Matcher) (string, bool) {
	switch {
	case name == labels.MetricName && cfg.MetricNamesTable != "":
		where, ok := postHogMetricNameFilters(cfg, mint, maxt, matchers)
		if !ok {
			return "", false
		}
		return fmt.Sprintf(
			"SELECT DISTINCT metric_name AS label_value FROM %s WHERE %s ORDER BY label_value%s",
			postHogMetricNamesTable(cfg),
			strings.Join(where, " AND "),
			sqlLimit(limit),
		), true
	case postHogSampleColumnLabel(name):
		if len(matchers) > 0 {
			return "", false
		}
		column, _ := postHogSeriesLabelExpr(name)
		return fmt.Sprintf(
			"SELECT DISTINCT %s AS label_value FROM %s WHERE %s ORDER BY label_value%s",
			column,
			postHogSeriesTable(cfg),
			strings.Join(postHogSeriesFilters(cfg, nil, mint, maxt), " AND "),
			sqlLimit(limit),
		), true
	case cfg.AttributeTable != "":
		where, ok := postHogAttributeFilters(cfg, mint, maxt, matchers)
		if !ok {
			return "", false
		}
		where = append(where, "attribute_key = "+sqlString(name))
		return fmt.Sprintf(
			"SELECT DISTINCT attribute_value AS label_value FROM %s WHERE %s ORDER BY label_value%s",
			postHogAttributeTable(cfg),
			strings.Join(where, " AND "),
			sqlLimit(limit),
		), true
	default:
		if len(matchers) > 0 {
			return "", false
		}
		return fmt.Sprintf(
			"SELECT DISTINCT %s AS label_value FROM %s WHERE %s AND label_value != '' ORDER BY label_value%s",
			postHogLabelValueExpr(name),
			postHogSeriesTable(cfg),
			strings.Join(postHogSeriesFilters(cfg, nil, mint, maxt), " AND "),
			sqlLimit(limit),
		), true
	}
}

// postHogMetricNameFilters filters the metric name rollup by team, time and
// the matchers it can answer: any matcher on __name__, including the regex
// that a metric search sends. A matcher on another label needs the series
// table and reports false.
func postHogMetricNameFilters(cfg Config, mint, maxt int64, matchers []*labels.Matcher) ([]string, bool) {
	filters := []string{teamFilter(cfg)}
	filters = append(filters, postHogAttributeTimeFilters(mint, maxt)...)
	for _, matcher := range matchers {
		if matcherIsNoop(matcher) || postHogMatcherCanSkip(matcher) {
			continue
		}
		if matcher.Name != labels.MetricName {
			return nil, false
		}
		condition, ok := metricMatcherCondition(matcher)
		if !ok {
			return nil, false
		}
		filters = append(filters, condition)
	}
	return filters, true
}

// postHogAttributeFilters filters the attribute rollup by team, time and the
// matchers it can answer. The rollup keys on metric_name and service_name, so
// only exact matchers on those labels are accepted; any other matcher reports
// false and needs the series table. A metric_name filter also needs a table
// that has the column.
func postHogAttributeFilters(cfg Config, mint, maxt int64, matchers []*labels.Matcher) ([]string, bool) {
	filters := []string{teamFilter(cfg)}
	filters = append(filters, postHogAttributeTimeFilters(mint, maxt)...)
	for _, matcher := range matchers {
		if matcherIsNoop(matcher) {
			continue
		}
		if matcher.Type != labels.MatchEqual || matcher.Value == "" {
			return nil, false
		}
		if matcher.Name == labels.MetricName && !cfg.AttributeTableHasMetricName {
			return nil, false
		}
		column, ok := postHogSampleLabelExpr(matcher.Name)
		if !ok {
			return nil, false
		}
		filters = append(filters, column+" = "+sqlString(matcher.Value))
	}
	return filters, true
}

// postHogAttributeTimeFilters bounds the attribute and metric name rollups,
// which bucket by hour and only have a row in the hour of a labelled row, so
// the window widens by a label window on each side.
func postHogAttributeTimeFilters(mint, maxt int64) []string {
	return []string{
		"time_bucket >= toStartOfHour(" + chTimeMillis(mint-postHogLabelWindowMillis) + ")",
		"time_bucket <= toStartOfHour(" + chTimeMillis(maxt+postHogLabelWindowMillis) + ")",
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

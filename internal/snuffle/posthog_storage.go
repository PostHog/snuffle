package snuffle

import (
	"context"
	"fmt"
	"strings"

	"github.com/prometheus/prometheus/model/labels"
)

func (q *CHQuerier) selectPostHogSeries(ctx context.Context, mint, maxt int64, matchers ...*labels.Matcher) ([]*seriesMeta, error) {
	sql := postHogSelectedSeriesSQL(q.queryable.cfg, matchers, mint, maxt, q.queryable.cfg.MaxSeries)
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
		sql := postHogLoadSamplesSQL(q.queryable.cfg, batch, matchers, mint, maxt, latestOnly)
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

func postHogLoadSamplesSQL(cfg Config, ids []uint64, matchers []*labels.Matcher, mint, maxt int64, latestOnly bool) string {
	where := postHogSampleFilters(cfg, matchers, mint, maxt)
	where = append(where, postHogSeriesIDExpr()+" IN ("+joinUint64(ids)+")")
	source := fmt.Sprintf(
		"SELECT %s AS series_id, timestamp, value FROM %s WHERE %s",
		postHogSeriesIDExpr(),
		tableName(cfg.CHDatabase, cfg.SamplesTable),
		strings.Join(where, " AND "),
	)
	if latestOnly {
		return fmt.Sprintf(
			"SELECT series_id, toUnixTimestamp64Milli(max(timestamp)) AS ts, argMax(value, timestamp) AS value FROM (%s) GROUP BY series_id ORDER BY series_id",
			source,
		)
	}
	return fmt.Sprintf(
		"SELECT series_id, toUnixTimestamp64Milli(timestamp) AS ts, value FROM (%s) ORDER BY series_id, timestamp",
		source,
	)
}

func postHogSelectedSeriesSQL(cfg Config, matchers []*labels.Matcher, mint, maxt int64, limit int) string {
	source := postHogSeriesLabelsSourceSQL(cfg, matchers, mint, maxt)
	return fmt.Sprintf(
		"SELECT xxHash64(metric_name, service_name, resource_fingerprint, sorted_attributes_map_str) AS series_id, metric_name, service_name, any(resource_attributes) AS resource_attributes, sorted_attributes_map_str AS attributes_map_str FROM (%s) GROUP BY metric_name, service_name, resource_fingerprint, sorted_attributes_map_str LIMIT %d",
		source,
		limit,
	)
}

func postHogSeriesSamplesSQL(cfg Config, matchers []*labels.Matcher, mint, maxt int64, latestOnly bool) string {
	if latestOnly && postHogExactSampleTimestamp(cfg, maxt) {
		source := postHogSeriesSourceSQL(cfg, matchers, maxt, maxt, true)
		return fmt.Sprintf(
			"SELECT series_id, any(metric_name) AS metric_name, any(service_name) AS service_name, any(resource_attributes) AS resource_attributes, any(attributes_map_str) AS attributes_map_str, toUnixTimestamp64Milli(max(timestamp)) AS ts, argMax(value, timestamp) AS value FROM (%s) GROUP BY series_id ORDER BY series_id LIMIT %d",
			source,
			cfg.MaxSeries,
		)
	}
	source := postHogSeriesSourceSQL(cfg, matchers, mint, maxt, true)
	if latestOnly {
		return fmt.Sprintf(
			"SELECT series_id, any(metric_name) AS metric_name, any(service_name) AS service_name, any(resource_attributes) AS resource_attributes, any(attributes_map_str) AS attributes_map_str, toUnixTimestamp64Milli(max(timestamp)) AS ts, argMax(value, timestamp) AS value FROM (%s) GROUP BY series_id ORDER BY series_id LIMIT %d",
			source,
			cfg.MaxSeries,
		)
	}
	selectedSeries := fmt.Sprintf(
		"SELECT xxHash64(metric_name, service_name, resource_fingerprint, sorted_attributes_map_str) AS series_id, metric_name, service_name, any(resource_attributes) AS resource_attributes, sorted_attributes_map_str AS attributes_map_str FROM (%s) GROUP BY metric_name, service_name, resource_fingerprint, sorted_attributes_map_str LIMIT %d",
		postHogSeriesLabelsSourceSQL(cfg, matchers, mint, maxt),
		cfg.MaxSeries,
	)
	samples := postHogSeriesSourceSQL(cfg, matchers, mint, maxt, false)
	return fmt.Sprintf(
		"WITH selected_series AS (%s) SELECT selected_series.series_id, selected_series.metric_name, selected_series.service_name, selected_series.resource_attributes, selected_series.attributes_map_str, toUnixTimestamp64Milli(samples.timestamp) AS ts, samples.value AS value FROM (%s) AS samples INNER JOIN selected_series USING series_id ORDER BY selected_series.series_id, samples.timestamp",
		selectedSeries,
		samples,
	)
}

func postHogSeriesLabelsSourceSQL(cfg Config, matchers []*labels.Matcher, mint, maxt int64) string {
	where := postHogSampleFilters(cfg, matchers, mint, maxt)
	return fmt.Sprintf(
		"SELECT metric_name, service_name, resource_fingerprint, resource_attributes, mapSort(attributes_map_str) AS sorted_attributes_map_str FROM %s WHERE %s",
		tableName(cfg.CHDatabase, cfg.SamplesTable),
		strings.Join(where, " AND "),
	)
}

func postHogSeriesSourceSQL(cfg Config, matchers []*labels.Matcher, mint, maxt int64, includeLabels bool) string {
	selects := []string{
		postHogSeriesIDExpr() + " AS series_id",
		"timestamp",
		"value",
	}
	if includeLabels {
		selects = append(selects,
			"metric_name",
			"service_name",
			"resource_attributes",
			"attributes_map_str",
		)
	}
	where := postHogSampleFilters(cfg, matchers, mint, maxt)
	return fmt.Sprintf(
		"SELECT %s FROM %s WHERE %s",
		strings.Join(selects, ", "),
		tableName(cfg.CHDatabase, cfg.SamplesTable),
		strings.Join(where, " AND "),
	)
}

func postHogSeriesIDExpr() string {
	return "xxHash64(metric_name, service_name, resource_fingerprint, mapSort(attributes_map_str))"
}

func postHogExactSampleTimestamp(cfg Config, ts int64) bool {
	return cfg.RemoteWriteInterval > 0 && bucketTimestampMS(ts, cfg.RemoteWriteInterval) == ts
}

func postHogSampleFilters(cfg Config, matchers []*labels.Matcher, mint, maxt int64) []string {
	filters := []string{teamFilter(cfg)}
	filters = append(filters, sampleTimeFilters(cfg, mint, maxt)...)
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

func postHogMatcherCanSkip(matcher *labels.Matcher) bool {
	switch matcher.Type {
	case labels.MatchEqual, labels.MatchRegexp:
		return matcher.Matches("")
	default:
		return false
	}
}

func postHogMatcherCondition(matcher *labels.Matcher) (string, bool) {
	switch matcher.Name {
	case labels.MetricName:
		return metricMatcherCondition(matcher)
	case "service_name":
		return stringColumnMatcherCondition("service_name", matcher)
	case "":
		return "", false
	default:
		return stringColumnMatcherCondition(postHogLabelValueExpr(matcher.Name), matcher)
	}
}

func postHogLabelValueExpr(name string) string {
	metricKey := sqlString(name + "__str")
	resourceKey := sqlString(name)
	metricColumn := "attributes_map_str[" + metricKey + "]"
	resourceColumn := "resource_attributes[" + resourceKey + "]"
	return "if(mapContains(attributes_map_str, " + metricKey + "), " + metricColumn + ", " + resourceColumn + ")"
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
		if key == "" {
			continue
		}
		key = strings.TrimSuffix(key, "__str")
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
	attrTable := q.queryable.cfg.AttributeTable
	if attrTable != "" {
		sql := fmt.Sprintf(
			"SELECT DISTINCT attribute_key FROM %s WHERE %s AND time_bucket >= toStartOfInterval(%s, toIntervalMinute(10)) AND time_bucket <= toStartOfInterval(%s, toIntervalMinute(10)) ORDER BY attribute_key%s",
			tableName(q.queryable.cfg.CHDatabase, attrTable),
			teamFilter(q.queryable.cfg),
			chTimeMillis(q.mint),
			chTimeMillis(q.maxt),
			sqlLimit(limit),
		)
		if err := q.addStringRows(ctx, names, sql); err != nil {
			return nil, err
		}
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
	switch name {
	case labels.MetricName:
		sql = fmt.Sprintf(
			"SELECT DISTINCT metric_name AS label_value FROM %s WHERE %s AND timestamp >= %s AND timestamp <= %s ORDER BY label_value%s",
			tableName(q.queryable.cfg.CHDatabase, q.queryable.cfg.SamplesTable),
			teamFilter(q.queryable.cfg),
			chTimeMillis(q.mint),
			chTimeMillis(q.maxt),
			sqlLimit(limit),
		)
	case "service_name":
		sql = fmt.Sprintf(
			"SELECT DISTINCT service_name AS label_value FROM %s WHERE %s AND timestamp >= %s AND timestamp <= %s ORDER BY label_value%s",
			tableName(q.queryable.cfg.CHDatabase, q.queryable.cfg.SamplesTable),
			teamFilter(q.queryable.cfg),
			chTimeMillis(q.mint),
			chTimeMillis(q.maxt),
			sqlLimit(limit),
		)
	default:
		attrTable := q.queryable.cfg.AttributeTable
		if attrTable == "" {
			return nil, nil
		}
		sql = fmt.Sprintf(
			"SELECT DISTINCT attribute_value AS label_value FROM %s WHERE %s AND attribute_key = %s AND time_bucket >= toStartOfInterval(%s, toIntervalMinute(10)) AND time_bucket <= toStartOfInterval(%s, toIntervalMinute(10)) ORDER BY label_value%s",
			tableName(q.queryable.cfg.CHDatabase, attrTable),
			teamFilter(q.queryable.cfg),
			sqlString(name),
			chTimeMillis(q.mint),
			chTimeMillis(q.maxt),
			sqlLimit(limit),
		)
	}
	if err := q.addStringRows(ctx, values, sql); err != nil {
		return nil, err
	}
	return sortedLimited(values, limit), nil
}

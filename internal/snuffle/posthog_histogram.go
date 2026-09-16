package snuffle

import (
	"context"
	"fmt"
	"maps"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/prometheus/prometheus/model/labels"
	promvalue "github.com/prometheus/prometheus/model/value"
)

var postHogHistogramSuffixes = []string{"_bucket", "_count", "_sum"}

type postHogHistogramAlias struct {
	baseName string
	suffix   string
}

func postHogExactHistogramAlias(matchers []*labels.Matcher) (postHogHistogramAlias, bool) {
	name := exactMetricName(matchers)
	for _, suffix := range postHogHistogramSuffixes {
		if strings.HasSuffix(name, suffix) && len(name) > len(suffix) {
			return postHogHistogramAlias{baseName: strings.TrimSuffix(name, suffix), suffix: suffix}, true
		}
	}
	return postHogHistogramAlias{}, false
}

func postHogExactHistogramMetadataSQL(cfg Config, mint, maxt int64, alias postHogHistogramAlias, matchers []*labels.Matcher) string {
	name := alias.baseName + alias.suffix
	realWhere := postHogSeriesFilters(cfg, matchers, mint, maxt)
	virtualWhere := postHogSeriesFilters(cfg, postHogHistogramBaseMatchers(matchers), mint, maxt)
	virtualWhere = append(virtualWhere, "metric_name = "+sqlString(alias.baseName))
	if alias.suffix == "_bucket" {
		virtualWhere = append(virtualWhere, "metric_type = 'histogram'")
	} else {
		virtualWhere = append(virtualWhere, postHogHistogramTypeFilter)
	}
	virtualWhere = append(virtualWhere, fmt.Sprintf("%s NOT IN (SELECT metric_name FROM %s WHERE %s AND metric_name = %s)", sqlString(name), postHogSeriesTable(cfg), teamFilter(cfg), sqlString(name)))
	where := []string{
		teamFilter(cfg),
		"metric_name IN (" + sqlString(name) + ", " + sqlString(alias.baseName) + ")",
		"((" + strings.Join(realWhere, " AND ") + ") OR (" + strings.Join(virtualWhere, " AND ") + "))",
	}
	return postHogSelectedSeriesWhereSQL(cfg, where, cfg.MaxSeries, nil)
}

func (q *CHQuerier) selectPostHogExactHistogramMetadata(ctx context.Context, mint, maxt int64, alias postHogHistogramAlias, matchers []*labels.Matcher) ([]*seriesMeta, map[uint64]postHogHistogramSource, []uint64, error) {
	for _, matcher := range matchers {
		if matcher.Name == labels.MetricName && !matcher.Matches(alias.baseName+alias.suffix) {
			return nil, nil, nil, nil
		}
	}
	series := make([]*seriesMeta, 0, 64)
	sources := make(map[uint64]postHogHistogramSource, 64)
	ids := make([]uint64, 0, 64)
	err := q.queryable.client.QueryRows(ctx, postHogExactHistogramMetadataSQL(q.queryable.cfg, mint, maxt, alias, matchers), func(row clickHouseRow) error {
		var id uint64
		var source postHogHistogramSource
		if err := row.Scan(&id, &source.metricName, &source.serviceName, &source.resource, &source.attributes); err != nil {
			return err
		}
		if source.metricName == alias.baseName {
			sources[id] = source
			ids = append(ids, id)
			return nil
		}
		labelMap := postHogLabelMap(source.metricName, source.serviceName, source.resource, source.attributes)
		if matchesAll(labelMap, matchers) {
			series = append(series, &seriesMeta{id: id, metricName: source.metricName, labelMap: labelMap, labels: labels.FromMap(labelMap)})
		}
		return nil
	})
	if err != nil {
		return nil, nil, nil, err
	}
	if len(ids) >= q.queryable.cfg.MaxSeries {
		return nil, nil, nil, fmt.Errorf("histogram series limit exceeded (%d); tighten matchers or increase CH_MAX_SERIES", q.queryable.cfg.MaxSeries)
	}
	if len(series) >= q.queryable.cfg.MaxSeries {
		return nil, nil, nil, fmt.Errorf("series limit exceeded (%d); tighten matchers or increase CH_MAX_SERIES", q.queryable.cfg.MaxSeries)
	}
	return series, sources, ids, nil
}

func (q *CHQuerier) selectPostHogExactHistogramSeries(ctx context.Context, mint, maxt int64, alias postHogHistogramAlias, matchers []*labels.Matcher, withSamples, latestOnly bool) ([]*seriesMeta, error) {
	series, sources, ids, err := q.selectPostHogExactHistogramMetadata(ctx, mint, maxt, alias, matchers)
	if err != nil {
		return nil, err
	}
	if len(series) == 0 && len(ids) == 0 {
		return nil, nil
	}
	if withSamples {
		if err := q.loadPostHogSamples(ctx, series, mint, maxt, latestOnly, matchers); err != nil {
			return nil, err
		}
		series = slices.DeleteFunc(series, func(meta *seriesMeta) bool { return len(meta.samples) == 0 })
	}
	virtual, err := q.readPostHogHistogramSamples(ctx, mint, maxt, []postHogHistogramAlias{alias}, matchers, sources, ids, withSamples)
	if err != nil {
		return nil, err
	}
	if len(series)+len(virtual) > q.queryable.cfg.MaxSeries {
		return nil, fmt.Errorf("series limit exceeded (%d); tighten matchers or increase CH_MAX_SERIES", q.queryable.cfg.MaxSeries)
	}
	return append(series, virtual...), nil
}

func postHogMaySelectHistogram(matchers []*labels.Matcher) bool {
	if name := exactMetricName(matchers); name != "" {
		for _, suffix := range postHogHistogramSuffixes {
			if strings.HasSuffix(name, suffix) && len(name) > len(suffix) {
				return true
			}
		}
		return false
	}
	return true
}

func postHogHistogramBaseMatchers(matchers []*labels.Matcher) []*labels.Matcher {
	base := make([]*labels.Matcher, 0, len(matchers))
	for _, matcher := range matchers {
		if matcher.Name != labels.MetricName && matcher.Name != "le" {
			base = append(base, matcher)
		}
	}
	return base
}

func postHogHistogramAliasesSQL(cfg Config, mint, maxt int64, matchers []*labels.Matcher) string {
	where := postHogSeriesFilters(cfg, postHogHistogramBaseMatchers(matchers), mint, maxt)
	where = append(where, "metric_type IN ('histogram', 'exponential_histogram')")
	where = append(where, "(metric_type != 'exponential_histogram' OR suffix != '_bucket')")
	realWhere := []string{teamFilter(cfg)}
	for _, matcher := range matchers {
		if matcher.Name != labels.MetricName {
			continue
		}
		condition, _ := stringColumnMatcherCondition("concat(metric_name, suffix)", matcher)
		where = append(where, condition)
		condition, _ = metricMatcherCondition(matcher)
		realWhere = append(realWhere, condition)
	}
	if name := exactMetricName(matchers); name != "" {
		for _, suffix := range postHogHistogramSuffixes {
			if strings.HasSuffix(name, suffix) {
				where = append(where, "metric_name = "+sqlString(strings.TrimSuffix(name, suffix)), "suffix = "+sqlString(suffix))
				break
			}
		}
	}
	where = append(where, fmt.Sprintf("concat(metric_name, suffix) NOT IN (SELECT metric_name FROM %s WHERE %s)", postHogSeriesTable(cfg), strings.Join(realWhere, " AND ")))
	return fmt.Sprintf("SELECT DISTINCT metric_name AS base_name, suffix FROM %s ARRAY JOIN ['_bucket', '_count', '_sum'] AS suffix WHERE %s ORDER BY base_name, suffix%s", postHogSeriesTable(cfg), strings.Join(where, " AND "), sqlLimit(cfg.MaxSeries+1))
}

func (q *CHQuerier) postHogHistogramAliases(ctx context.Context, mint, maxt int64, matchers []*labels.Matcher) ([]postHogHistogramAlias, error) {
	if !postHogMaySelectHistogram(matchers) {
		return nil, nil
	}
	var aliases []postHogHistogramAlias
	err := q.queryable.client.QueryRows(ctx, postHogHistogramAliasesSQL(q.queryable.cfg, mint, maxt, matchers), func(row clickHouseRow) error {
		var alias postHogHistogramAlias
		if err := row.Scan(&alias.baseName, &alias.suffix); err != nil {
			return err
		}
		aliases = append(aliases, alias)
		if len(aliases) > q.queryable.cfg.MaxSeries {
			return fmt.Errorf("histogram alias limit exceeded (%d); tighten metric name matchers", q.queryable.cfg.MaxSeries)
		}
		return nil
	})
	return aliases, err
}

const postHogHistogramTypeFilter = "metric_type IN ('histogram', 'exponential_histogram')"

// postHogHistogramSampleMatchers replaces the virtual metric name and le
// matchers with a matcher on the stored base metric names.
func postHogHistogramSampleMatchers(aliases []postHogHistogramAlias, matchers []*labels.Matcher) []*labels.Matcher {
	names := make(map[string]struct{}, len(aliases))
	for _, alias := range aliases {
		names[alias.baseName] = struct{}{}
	}
	patterns := sortedLimited(names, 0)
	for index, name := range patterns {
		patterns[index] = regexp.QuoteMeta(name)
	}
	baseMatchers := postHogHistogramBaseMatchers(matchers)
	return append(baseMatchers, labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, strings.Join(patterns, "|")))
}

// postHogHistogramSeriesSQL selects the stored histogram series behind the
// aliases, with their labels, from the series table.
func postHogHistogramSeriesSQL(cfg Config, mint, maxt int64, aliases []postHogHistogramAlias, matchers []*labels.Matcher) string {
	where := postHogSeriesFilters(cfg, postHogHistogramSampleMatchers(aliases, matchers), mint, maxt)
	where = append(where, postHogHistogramTypeFilter)
	return postHogSelectedSeriesWhereSQL(cfg, where, cfg.MaxSeries, nil)
}

// postHogHistogramSamplesSQL reads histogram samples for the selected
// fingerprints. Labels come from postHogHistogramSeriesSQL, once per series,
// not once per sample row.
func postHogHistogramSamplesSQL(cfg Config, mint, maxt int64, ids []uint64, aliases []postHogHistogramAlias, matchers []*labels.Matcher) string {
	where := postHogSampleFilters(cfg, postHogHistogramSampleMatchers(aliases, matchers), mint, maxt)
	where = append(where, postHogHistogramTypeFilter, "series_fingerprint IN ("+joinUint64(ids)+")")
	return fmt.Sprintf("SELECT series_fingerprint AS series_id, toUnixTimestamp64Milli(timestamp) AS ts, value, count, formatRow('RowBinary', histogram_bounds, histogram_counts) AS histogram_arrays, aggregation_temporality FROM %s WHERE %s ORDER BY series_id, timestamp", postHogSamplesTable(cfg), strings.Join(where, " AND "))
}

// postHogHistogramSource is the stored series a virtual histogram series
// derives from.
type postHogHistogramSource struct {
	metricName  string
	serviceName string
	resource    map[string]string
	attributes  map[string]string
}

type postHogHistogramSample struct {
	id          uint64
	metricName  string
	serviceName string
	resource    map[string]string
	attributes  map[string]string
	timestamp   int64
	sum         float64
	count       uint64
	bounds      []float64
	counts      []uint64
	temporality string
}

type postHogHistogramSeriesBuilder struct {
	cfg            Config
	matchers       []*labels.Matcher
	withSamples    bool
	series         []*seriesMeta
	byLabels       map[string]*seriesMeta
	bySource       map[uint64]*postHogHistogramSourceSeries
	previousBucket map[uint64]map[string]*seriesMeta
	samples        int
}

type postHogHistogramSeriesKey struct {
	suffix string
	bound  string
}

type postHogHistogramSourceSeries struct {
	labels map[string]string
	series map[postHogHistogramSeriesKey]*seriesMeta
}

func newPostHogHistogramSeriesBuilder(cfg Config, matchers []*labels.Matcher, withSamples bool) *postHogHistogramSeriesBuilder {
	return &postHogHistogramSeriesBuilder{cfg: cfg, matchers: matchers, withSamples: withSamples, byLabels: make(map[string]*seriesMeta), bySource: make(map[uint64]*postHogHistogramSourceSeries), previousBucket: make(map[uint64]map[string]*seriesMeta)}
}

func postHogHistogramBuckets(sample postHogHistogramSample) (map[string]float64, error) {
	if len(sample.counts) != len(sample.bounds)+1 {
		return nil, fmt.Errorf("histogram %q has %d bounds but %d bucket counts", sample.metricName, len(sample.bounds), len(sample.counts))
	}
	buckets := make(map[string]float64, len(sample.counts))
	var cumulative uint64
	for index, count := range sample.counts {
		if count > math.MaxUint64-cumulative {
			return nil, fmt.Errorf("histogram %q bucket count overflows uint64", sample.metricName)
		}
		cumulative += count
		bound := "+Inf"
		if index < len(sample.bounds) {
			upper := sample.bounds[index]
			if math.IsNaN(upper) || math.IsInf(upper, 0) || (index > 0 && upper <= sample.bounds[index-1]) {
				return nil, fmt.Errorf("histogram %q bounds must be finite and strictly increasing", sample.metricName)
			}
			bound = strconv.FormatFloat(upper, 'g', -1, 64)
		}
		buckets[bound] = float64(cumulative)
	}
	if cumulative != sample.count {
		return nil, fmt.Errorf("histogram %q bucket counts do not match its observation count", sample.metricName)
	}
	return buckets, nil
}

func (builder *postHogHistogramSeriesBuilder) add(sample postHogHistogramSample, suffixes []string) error {
	source := builder.bySource[sample.id]
	if source == nil {
		source = &postHogHistogramSourceSeries{
			labels: postHogLabelMap(sample.metricName, sample.serviceName, sample.resource, sample.attributes),
			series: make(map[postHogHistogramSeriesKey]*seriesMeta),
		}
		builder.bySource[sample.id] = source
	}
	for _, suffix := range suffixes {
		switch suffix {
		case "_bucket":
			buckets, err := postHogHistogramBuckets(sample)
			if err != nil {
				return err
			}
			current := make(map[string]*seriesMeta, len(buckets))
			for bound, count := range buckets {
				meta, err := builder.addPoint(source, sample, postHogHistogramSeriesKey{suffix: suffix, bound: bound}, count)
				if err != nil {
					return err
				}
				if meta != nil {
					current[bound] = meta
				}
			}
			if builder.withSamples {
				for bound, previous := range builder.previousBucket[sample.id] {
					if _, exists := current[bound]; !exists {
						if err := builder.appendSample(previous, sample.timestamp, math.Float64frombits(promvalue.StaleNaN)); err != nil {
							return err
						}
					}
				}
				builder.previousBucket[sample.id] = current
			}
		case "_count":
			if _, err := builder.addPoint(source, sample, postHogHistogramSeriesKey{suffix: suffix}, float64(sample.count)); err != nil {
				return err
			}
		case "_sum":
			if _, err := builder.addPoint(source, sample, postHogHistogramSeriesKey{suffix: suffix}, sample.sum); err != nil {
				return err
			}
		}
	}
	return nil
}

func (builder *postHogHistogramSeriesBuilder) addPoint(source *postHogHistogramSourceSeries, sample postHogHistogramSample, seriesKey postHogHistogramSeriesKey, value float64) (*seriesMeta, error) {
	meta := source.series[seriesKey]
	var labelMap map[string]string
	if meta == nil {
		labelMap = maps.Clone(source.labels)
		labelMap[labels.MetricName] = sample.metricName + seriesKey.suffix
		if seriesKey.suffix == "_bucket" {
			labelMap["le"] = seriesKey.bound
		}
		if !matchesAll(labelMap, builder.matchers) {
			return nil, nil
		}
	}
	if builder.withSamples && sample.temporality != "cumulative" {
		return nil, fmt.Errorf("histogram %q has %q aggregation temporality; virtual Prometheus counters require cumulative histograms; convert delta histograms to cumulative before ingestion", sample.metricName, sample.temporality)
	}
	if meta == nil {
		labelSet := labels.FromMap(labelMap)
		key := labelSet.String()
		meta = builder.byLabels[key]
		if meta == nil {
			if len(builder.series) >= builder.cfg.MaxSeries {
				return nil, fmt.Errorf("histogram series limit exceeded (%d); tighten matchers or increase CH_MAX_SERIES", builder.cfg.MaxSeries)
			}
			meta = &seriesMeta{id: labelSet.Hash(), metricName: labelMap[labels.MetricName], labelMap: labelMap, labels: labelSet}
			builder.byLabels[key] = meta
			builder.series = append(builder.series, meta)
		}
		source.series[seriesKey] = meta
	}
	if builder.withSamples {
		if err := builder.appendSample(meta, sample.timestamp, value); err != nil {
			return nil, err
		}
	}
	return meta, nil
}

func (builder *postHogHistogramSeriesBuilder) appendSample(meta *seriesMeta, timestamp int64, value float64) error {
	if builder.cfg.MaxSamples > 0 && builder.samples >= builder.cfg.MaxSamples {
		return fmt.Errorf("histogram sample limit exceeded (%d); shorten the time range or tighten matchers", builder.cfg.MaxSamples)
	}
	meta.samples = append(meta.samples, samplePoint{t: timestamp, v: value})
	builder.samples++
	return nil
}

func (q *CHQuerier) selectPostHogHistogramSources(ctx context.Context, mint, maxt int64, aliases []postHogHistogramAlias, matchers []*labels.Matcher) (map[uint64]postHogHistogramSource, []uint64, error) {
	sources := make(map[uint64]postHogHistogramSource, 64)
	ids := make([]uint64, 0, 64)
	err := q.queryable.client.QueryRows(ctx, postHogHistogramSeriesSQL(q.queryable.cfg, mint, maxt, aliases, matchers), func(row clickHouseRow) error {
		var id uint64
		var source postHogHistogramSource
		if err := row.Scan(&id, &source.metricName, &source.serviceName, &source.resource, &source.attributes); err != nil {
			return err
		}
		sources[id] = source
		ids = append(ids, id)
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	if len(ids) >= q.queryable.cfg.MaxSeries {
		return nil, nil, fmt.Errorf("histogram series limit exceeded (%d); tighten matchers or increase CH_MAX_SERIES", q.queryable.cfg.MaxSeries)
	}
	return sources, ids, nil
}

func (q *CHQuerier) readPostHogHistogramSeries(ctx context.Context, mint, maxt int64, aliases []postHogHistogramAlias, matchers []*labels.Matcher, withSamples bool) ([]*seriesMeta, error) {
	if len(aliases) == 0 {
		return nil, nil
	}
	sources, ids, err := q.selectPostHogHistogramSources(ctx, mint, maxt, aliases, matchers)
	if err != nil {
		return nil, err
	}
	return q.readPostHogHistogramSamples(ctx, mint, maxt, aliases, matchers, sources, ids, withSamples)
}

func (q *CHQuerier) readPostHogHistogramSamples(ctx context.Context, mint, maxt int64, aliases []postHogHistogramAlias, matchers []*labels.Matcher, sources map[uint64]postHogHistogramSource, ids []uint64, withSamples bool) ([]*seriesMeta, error) {
	suffixes := make(map[string][]string, len(aliases))
	for _, alias := range aliases {
		suffixes[alias.baseName] = append(suffixes[alias.baseName], alias.suffix)
	}
	builder := newPostHogHistogramSeriesBuilder(q.queryable.cfg, matchers, withSamples)
	var sample postHogHistogramSample
	for _, batch := range idBatches(ids, q.queryable.cfg.IDChunkSize) {
		err := q.queryable.client.QueryRows(ctx, postHogHistogramSamplesSQL(q.queryable.cfg, mint, maxt, batch, aliases, matchers), func(row clickHouseRow) error {
			if err := sample.scanPacked(row); err != nil {
				return err
			}
			source, ok := sources[sample.id]
			if !ok {
				return nil
			}
			sample.metricName = source.metricName
			sample.serviceName = source.serviceName
			sample.resource = source.resource
			sample.attributes = source.attributes
			return builder.add(sample, suffixes[sample.metricName])
		})
		if err != nil {
			return nil, err
		}
	}
	return builder.series, nil
}

func (q *CHQuerier) appendPostHogHistogramSeries(ctx context.Context, series []*seriesMeta, mint, maxt int64, matchers []*labels.Matcher, withSamples bool) ([]*seriesMeta, error) {
	aliases, err := q.postHogHistogramAliases(ctx, mint, maxt, matchers)
	if err != nil {
		return nil, err
	}
	virtual, err := q.readPostHogHistogramSeries(ctx, mint, maxt, aliases, matchers, withSamples)
	if err != nil {
		return nil, err
	}
	if len(series)+len(virtual) > q.queryable.cfg.MaxSeries {
		return nil, fmt.Errorf("series limit exceeded (%d); tighten matchers or increase CH_MAX_SERIES", q.queryable.cfg.MaxSeries)
	}
	return append(series, virtual...), nil
}

func (q *CHQuerier) postHogHistogramLabelValues(ctx context.Context, name string, matchers []*labels.Matcher) ([]string, error) {
	if len(matchers) == 0 && name != "" && name != labels.MetricName && name != "le" {
		return nil, nil
	}
	aliases, err := q.postHogHistogramAliases(ctx, q.mint, q.maxt, matchers)
	if err != nil || len(aliases) == 0 {
		return nil, err
	}
	if name == "" && len(matchers) == 0 {
		for _, alias := range aliases {
			if alias.suffix == "_bucket" {
				return []string{"le"}, nil
			}
		}
		return nil, nil
	}
	if name == labels.MetricName {
		if _, onlyNames := postHogMetricNameFilters(q.queryable.cfg, q.mint, q.maxt, matchers); onlyNames {
			values := make([]string, 0, len(aliases))
			for _, alias := range aliases {
				values = append(values, alias.baseName+alias.suffix)
			}
			return values, nil
		}
	}
	series, err := q.readPostHogHistogramSeries(ctx, q.mint, q.maxt, aliases, matchers, false)
	if err != nil {
		return nil, err
	}
	values := make(map[string]struct{})
	for _, meta := range series {
		if name == "" {
			for label := range meta.labelMap {
				values[label] = struct{}{}
			}
		} else if value, exists := meta.labelMap[name]; exists {
			values[value] = struct{}{}
		}
	}
	return sortedLimited(values, 0), nil
}

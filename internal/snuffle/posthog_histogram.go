package snuffle

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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
	return mergePostHogHistogramSeries(series, virtual), nil
}

// mergePostHogHistogramSeries joins real component series and virtual
// histogram series that share a label set into one series. A histogram whose
// scrape arrived split across requests is stored as a native row for the
// complete part and as plain rows for the rest, so both forms carry the same
// labels. Samples of a shared series merge by timestamp. Where both forms have
// a sample at one timestamp, the real sample wins, which removes the stale
// marker the virtual builder writes for a bucket missing from a native row.
func mergePostHogHistogramSeries(real, virtual []*seriesMeta) []*seriesMeta {
	if len(real) == 0 {
		return virtual
	}
	if len(virtual) == 0 {
		return real
	}
	byLabels := make(map[string]*seriesMeta, len(virtual))
	for _, meta := range virtual {
		byLabels[meta.labels.String()] = meta
	}
	merged := make([]*seriesMeta, 0, len(real)+len(virtual))
	for _, meta := range real {
		target, ok := byLabels[meta.labels.String()]
		if !ok {
			merged = append(merged, meta)
			continue
		}
		target.samples = mergeSamplesPreferFirst(meta.samples, target.samples)
	}
	return append(merged, virtual...)
}

// mergeSamplesPreferFirst returns the union of two sorted sample lists. At a
// shared timestamp the sample from the first list is kept.
func mergeSamplesPreferFirst(preferred, other []samplePoint) []samplePoint {
	if len(other) == 0 {
		return preferred
	}
	if len(preferred) == 0 {
		return other
	}
	sortSamples(preferred)
	sortSamples(other)
	out := make([]samplePoint, 0, len(preferred)+len(other))
	i, j := 0, 0
	for i < len(preferred) || j < len(other) {
		switch {
		case j >= len(other) || (i < len(preferred) && preferred[i].t < other[j].t):
			out = append(out, preferred[i])
			i++
		case i >= len(preferred) || other[j].t < preferred[i].t:
			out = append(out, other[j])
			j++
		default:
			out = append(out, preferred[i])
			i++
			j++
		}
	}
	return out
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

// postHogHistogramSourceExistsSQL finds one stored histogram series that can
// back virtual series for the alias inside the query window. The series table
// is keyed by team, metric name, and hour, so this is a primary key lookup.
func postHogHistogramSourceExistsSQL(cfg Config, mint, maxt int64, alias postHogHistogramAlias) string {
	typeFilter := postHogHistogramTypeFilter
	if alias.suffix == "_bucket" {
		typeFilter = "metric_type = 'histogram'"
	}
	return fmt.Sprintf("SELECT 1 FROM %s WHERE %s AND metric_name = %s AND %s LIMIT 1", postHogSeriesTable(cfg), strings.Join(postHogSeriesFilters(cfg, nil, mint, maxt), " AND "), sqlString(alias.baseName), typeFilter)
}

// postHogSelectsHistogram reports whether the selector can return virtual
// histogram series inside the query window. Without an exact metric name, any
// virtual name can match. An exact name with a histogram suffix, such as a
// classic histogram's _bucket series, only does when the team stores a
// histogram with the base name in the window.
func (s *Server) postHogSelectsHistogram(ctx context.Context, mint, maxt int64, matchers []*labels.Matcher) (bool, error) {
	if !postHogMaySelectHistogram(matchers) {
		return false, nil
	}
	alias, ok := postHogExactHistogramAlias(matchers)
	if !ok {
		return true, nil
	}
	found := false
	err := s.client.QueryRows(ctx, postHogHistogramSourceExistsSQL(s.cfg, mint, maxt, alias), func(clickHouseRow) error {
		found = true
		return nil
	})
	return found, err
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
	for _, matcher := range matchers {
		if matcher.Name != labels.MetricName {
			continue
		}
		if condition, ok := stringColumnMatcherCondition("concat(metric_name, suffix)", matcher); ok {
			where = append(where, condition)
		}
	}
	if name := exactMetricName(matchers); name != "" {
		for _, suffix := range postHogHistogramSuffixes {
			if strings.HasSuffix(name, suffix) {
				where = append(where, "metric_name = "+sqlString(strings.TrimSuffix(name, suffix)), "suffix = "+sqlString(suffix))
				break
			}
		}
	}
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
	return postHogHistogramSamplesWhereSQL(cfg, mint, maxt, "series_fingerprint IN ("+joinUint64(ids)+")", aliases, matchers)
}

func postHogHistogramSamplesWhereSQL(cfg Config, mint, maxt int64, idCondition string, aliases []postHogHistogramAlias, matchers []*labels.Matcher) string {
	where := postHogSampleFilters(cfg, postHogHistogramSampleMatchers(aliases, matchers), mint, maxt)
	where = append(where, postHogHistogramTypeFilter, idCondition)
	return fmt.Sprintf("SELECT series_fingerprint AS series_id, toUnixTimestamp64Milli(timestamp) AS ts, value, count, formatRow('RowBinary', histogram_bounds, histogram_counts) AS histogram_arrays, aggregation_temporality FROM %s ORDER BY series_id, timestamp", postHogSampleRowsFrom(cfg, where, true))
}

// postHogHistogramBoundsSQL reads the distinct bound sets of the selected
// fingerprints for discovery. It returns rows in the shape of
// postHogHistogramSamplesWhereSQL with zero counts, so the series builder
// creates the same virtual series without reading every sample row.
func postHogHistogramBoundsSQL(cfg Config, mint, maxt int64, idCondition string, aliases []postHogHistogramAlias, matchers []*labels.Matcher) string {
	where := postHogSampleRowFilters(cfg, postHogHistogramSampleMatchers(aliases, matchers), mint, maxt)
	where = append(where, postHogSampleRowHasPointFilter(mint, maxt))
	where = append(where, postHogHistogramTypeFilter, idCondition)
	return fmt.Sprintf("SELECT DISTINCT series_fingerprint AS series_id, toInt64(0) AS ts, toFloat64(0) AS value, toUInt64(0) AS count, formatRow('RowBinary', histogram_bounds, arrayWithConstant(length(histogram_bounds) + 1, toUInt64(0))) AS histogram_arrays, aggregation_temporality FROM %s WHERE %s ORDER BY series_id", postHogSamplesTable(cfg), strings.Join(where, " AND "))
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
	strict         bool
	series         []*seriesMeta
	byLabels       map[string]*seriesMeta
	bySource       map[uint64]*postHogHistogramSourceSeries
	previousBucket map[uint64]map[string]*seriesMeta
	skipped        map[uint64]struct{}
	samples        int
}

// invalidPostHogHistogramError reports a stored histogram sample that cannot
// become Prometheus series. Limit errors are not of this type.
type invalidPostHogHistogramError struct {
	message string
}

func (err invalidPostHogHistogramError) Error() string {
	return err.message
}

func invalidPostHogHistogram(format string, args ...any) error {
	return invalidPostHogHistogramError{message: fmt.Sprintf(format, args...)}
}

type postHogHistogramSeriesKey struct {
	suffix string
	bound  string
}

type postHogHistogramSourceSeries struct {
	labels map[string]string
	series map[postHogHistogramSeriesKey]*seriesMeta
}

// newPostHogHistogramSeriesBuilder builds virtual series from stored
// histogram samples. A selector that names the metric exactly is strict: an
// invalid sample fails the query. A broader selector reaches every histogram
// of the team, so one invalid histogram must not fail it; the builder skips
// that source and logs it once.
func newPostHogHistogramSeriesBuilder(cfg Config, matchers []*labels.Matcher, withSamples bool) *postHogHistogramSeriesBuilder {
	return &postHogHistogramSeriesBuilder{cfg: cfg, matchers: matchers, withSamples: withSamples, strict: exactMetricName(matchers) != "", byLabels: make(map[string]*seriesMeta), bySource: make(map[uint64]*postHogHistogramSourceSeries), previousBucket: make(map[uint64]map[string]*seriesMeta), skipped: make(map[uint64]struct{})}
}

func postHogHistogramBuckets(sample postHogHistogramSample) (map[string]float64, error) {
	if len(sample.counts) != len(sample.bounds)+1 {
		return nil, invalidPostHogHistogram("histogram %q has %d bounds but %d bucket counts", sample.metricName, len(sample.bounds), len(sample.counts))
	}
	buckets := make(map[string]float64, len(sample.counts))
	var cumulative uint64
	for index, count := range sample.counts {
		if count > math.MaxUint64-cumulative {
			return nil, invalidPostHogHistogram("histogram %q bucket count overflows uint64", sample.metricName)
		}
		cumulative += count
		bound := "+Inf"
		if index < len(sample.bounds) {
			upper := sample.bounds[index]
			if math.IsNaN(upper) || math.IsInf(upper, 0) || (index > 0 && upper <= sample.bounds[index-1]) {
				return nil, invalidPostHogHistogram("histogram %q bounds must be finite and strictly increasing", sample.metricName)
			}
			bound = strconv.FormatFloat(upper, 'g', -1, 64)
		}
		buckets[bound] = float64(cumulative)
	}
	if cumulative != sample.count {
		return nil, invalidPostHogHistogram("histogram %q bucket counts do not match its observation count", sample.metricName)
	}
	return buckets, nil
}

func (builder *postHogHistogramSeriesBuilder) add(sample postHogHistogramSample, suffixes []string) error {
	err := builder.addSample(sample, suffixes)
	var invalid invalidPostHogHistogramError
	if err == nil || builder.strict || !errors.As(err, &invalid) {
		return err
	}
	if _, seen := builder.skipped[sample.id]; !seen {
		builder.skipped[sample.id] = struct{}{}
		slog.Warn("skipping invalid histogram series", "metric", sample.metricName, "service_name", sample.serviceName, "error", err)
	}
	return nil
}

func (builder *postHogHistogramSeriesBuilder) addSample(sample postHogHistogramSample, suffixes []string) error {
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
		return nil, invalidPostHogHistogram("histogram %q has %q aggregation temporality; virtual Prometheus counters require cumulative histograms; convert delta histograms to cumulative before ingestion", sample.metricName, sample.temporality)
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
	sqlFor := func(_ []uint64, idCondition string) string {
		if withSamples {
			return postHogHistogramSamplesWhereSQL(q.queryable.cfg, mint, maxt, idCondition, aliases, matchers)
		}
		return postHogHistogramBoundsSQL(q.queryable.cfg, mint, maxt, idCondition, aliases, matchers)
	}
	var sample postHogHistogramSample
	err := queryPostHogSeriesIDs(ctx, q.queryable.client, q.queryable.cfg.IDChunkSize, ids, sqlFor, func(row clickHouseRow) error {
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
	return mergePostHogHistogramSeries(series, virtual), nil
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

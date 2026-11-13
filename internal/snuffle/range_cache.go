package snuffle

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"sort"
	"sync"
	"time"

	"github.com/prometheus/prometheus/model/labels"
)

// The range result cache keeps the settled steps of range query results.
// A dashboard repeats one query over a window that moves forward. Most steps of
// the new window are also steps of the previous window, so the cache returns
// those steps and Snuffle evaluates only the steps after them.
//
// A step is settled when it is older than the cache offset plus one step.
// Samples can arrive late, so newer steps are evaluated on every request.
// An entry expires after the cache TTL, which limits how long a result
// calculated before a late backfill stays in use.
//
// The design follows the rollup result cache of VictoriaMetrics.

// Clients see these values in the X-Snuffle-Range-Cache header, in the
// range_cache field of the request log and in the status label of the cache metrics.
const (
	rangeCacheHit = "hit"
	// The cache held the first steps of the request and the rest were evaluated.
	rangeCachePartial = "partial"
	rangeCacheMiss    = "miss"
	// The query or its result cannot use the cache, so nothing was read from it or stored in it.
	rangeCacheBypass = "bypass"
)

const rangeCacheHeader = "X-Snuffle-Range-Cache"

// rangeCacheBackend is the query backend of a request for which no step was evaluated.
const rangeCacheBackend = "range_cache"

// The Prometheus engine refuses a request for more points in one series, so
// an entry with more steps could never be read in full.
const rangeCacheMaxSteps = maxRangePushdownPoints

// One entry can use one part in rangeCacheMaxEntryFraction of the size limit,
// so one large result cannot remove most of the other entries.
const rangeCacheMaxEntryFraction = 4

// rangeCacheKey identifies the results that one entry can hold. The tenant is
// outside the hash, so a hash collision cannot give one tenant the result of another.
type rangeCacheKey struct {
	teamID uint64
	hash   [sha256.Size]byte
}

// rangeCacheKeyParts are the request properties, other than the tenant, that
// two requests must share to use one entry.
type rangeCacheKeyParts struct {
	// The password is not a part. Instead, query uses or stores cached steps
	// only after ClickHouse accepted a query with the request credentials.
	username string
	// query is the prepared query, because it holds the durations that the
	// preparation resolves from the step and the lookback.
	query    string
	step     int64
	lookback int64
	// phase is the start time modulo the step. Steps of requests with the same
	// phase are on the same grid.
	phase int64
}

// key hashes the parts, so an entry does not keep the query text.
func (p rangeCacheKeyParts) key(teamID uint64) rangeCacheKey {
	hash := sha256.New()
	var buf [8]byte
	writeUint := func(value uint64) {
		binary.BigEndian.PutUint64(buf[:], value)
		hash.Write(buf[:])
	}
	// Each string has a length prefix, so two different part lists cannot hash the same bytes.
	writeString := func(value string) {
		writeUint(uint64(len(value)))
		hash.Write([]byte(value))
	}
	writeString(p.username)
	writeString(p.query)
	writeUint(uint64(p.step))
	writeUint(uint64(p.lookback))
	writeUint(uint64(p.phase))
	key := rangeCacheKey{teamID: teamID}
	hash.Sum(key.hash[:0])
	return key
}

// The cache never changes the points or the metric of a series after it
// stores them, because responses share them.
type rangeCacheSeries struct {
	labels labels.Labels
	// id is equal for two series only when their label sets are equal.
	id     string
	metric map[string]string
	points []samplePoint
}

// rangeCacheEntry holds the settled steps from start to end, both included.
type rangeCacheEntry struct {
	start   int64
	end     int64
	created time.Time
	series  []rangeCacheSeries
	bytes   int64
}

type rangeCacheItem struct {
	key   rangeCacheKey
	entry *rangeCacheEntry
}

type rangeResultCache struct {
	maxBytes int64
	offset   time.Duration
	ttl      time.Duration
	metrics  *bridgeMetrics
	// now is a field so tests can set the time.
	now func() time.Time

	mu      sync.Mutex
	bytes   int64
	entries map[rangeCacheKey]*list.Element
	// order lists the entries from the most recently used to the least recently used.
	order *list.List
}

// newRangeResultCache returns nil when the configuration disables the cache.
func newRangeResultCache(cfg Config, metrics *bridgeMetrics) *rangeResultCache {
	if cfg.RangeCacheMaxBytes <= 0 {
		return nil
	}
	return &rangeResultCache{
		maxBytes: int64(cfg.RangeCacheMaxBytes),
		offset:   cfg.RangeCacheOffset,
		ttl:      cfg.RangeCacheTTL,
		metrics:  metrics,
		now:      time.Now,
		entries:  make(map[rangeCacheKey]*list.Element),
		order:    list.New(),
	}
}

// get returns nil for an expired entry. The entry stays in the cache until a
// put replaces it or the size limit removes it.
func (c *rangeResultCache) get(key rangeCacheKey) *rangeCacheEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	element, ok := c.entries[key]
	if !ok {
		return nil
	}
	entry := element.Value.(*rangeCacheItem).entry
	if c.ttl > 0 && c.now().Sub(entry.created) >= c.ttl {
		return nil
	}
	c.order.MoveToFront(element)
	return entry
}

// put stores entry for key. base is the entry whose steps the new entry
// extends, or nil when every step of the new entry was evaluated. An extension
// is stored only while its base is still the entry of the key. Without this
// check, a slow request could bring back steps that a newer evaluation replaced.
func (c *rangeResultCache) put(key rangeCacheKey, entry, base *rangeCacheEntry) {
	if entry.bytes > c.maxBytes/rangeCacheMaxEntryFraction {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if element, ok := c.entries[key]; ok {
		item := element.Value.(*rangeCacheItem)
		if base != nil && item.entry != base {
			return
		}
		c.bytes += entry.bytes - item.entry.bytes
		item.entry = entry
		c.order.MoveToFront(element)
	} else {
		if base != nil {
			return
		}
		c.entries[key] = c.order.PushFront(&rangeCacheItem{key: key, entry: entry})
		c.bytes += entry.bytes
	}
	for c.bytes > c.maxBytes {
		oldest := c.order.Back()
		item := oldest.Value.(*rangeCacheItem)
		c.order.Remove(oldest)
		delete(c.entries, item.key)
		c.bytes -= item.entry.bytes
		c.metrics.observeRangeCacheEviction()
	}
	c.metrics.setRangeCacheSize(len(c.entries), c.bytes)
}

// remove deletes entry only while it is still the entry of key, so it cannot
// delete an entry that another request stored in the meantime.
func (c *rangeResultCache) remove(key rangeCacheKey, entry *rangeCacheEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	element, ok := c.entries[key]
	if !ok || element.Value.(*rangeCacheItem).entry != entry {
		return
	}
	c.order.Remove(element)
	delete(c.entries, key)
	c.bytes -= entry.bytes
	c.metrics.setRangeCacheSize(len(c.entries), c.bytes)
}

type rangeEvalFunc func(ctx context.Context, start, end time.Time) (rangeQueryResult, *rangeQueryError)

// query returns the result for the steps start, start+step, ... up to end.
// It evaluates only the steps that the cache does not hold. All times are in milliseconds.
//
// The key holds the user but not the password, so cached steps are used or
// stored only for a request that ClickHouse confirmed. An evaluation that read
// ClickHouse is such a confirmation. For every other request, verify asks
// ClickHouse. A request without a confirmation gets the result of a complete
// evaluation, and the entry of the key stays as it is.
// noCache evaluates every step and replaces the cached entry.
func (c *rangeResultCache) query(ctx context.Context, key rangeCacheKey, start, end, step int64, noCache bool, verify func(context.Context) error, eval rangeEvalFunc) (rangeQueryResult, string, *rangeQueryError) {
	lastStep := start + (end-start)/step*step
	evalAll := func() (rangeQueryResult, string, *rangeQueryError) {
		result, err := eval(ctx, time.UnixMilli(start).UTC(), time.UnixMilli(end).UTC())
		return result, rangeCacheBypass, err
	}
	entry := c.get(key)
	usable := entry
	// The entry must hold the first step. A gap before the cached steps would
	// need a second evaluation, so such a request evaluates every step.
	if noCache || entry == nil || start < entry.start || start > entry.end {
		usable = nil
	}

	if usable != nil && usable.end >= lastStep {
		if verify(ctx) != nil {
			return evalAll()
		}
		groups := rangeCacheGroups(usable.series, nil, start, lastStep+step, lastStep)
		return rangeQueryResult{backend: rangeCacheBackend, floats: true, groups: groups}, rangeCacheHit, nil
	}

	evalStart, status := start, rangeCacheMiss
	if usable != nil {
		evalStart, status = usable.end+step, rangeCachePartial
	}
	fresh, queryErr := eval(ctx, time.UnixMilli(evalStart).UTC(), time.UnixMilli(end).UTC())
	if queryErr != nil {
		return fresh, status, queryErr
	}
	if !fresh.clickHouseAccepted && verify(ctx) != nil {
		if usable == nil {
			return fresh, rangeCacheBypass, nil
		}
		return evalAll()
	}
	freshSeries, ok := rangeCacheSeriesFromGroups(fresh.groups)
	if !fresh.floats || fresh.rangeDependent || !ok {
		if usable == nil {
			return fresh, rangeCacheBypass, nil
		}
		// The new steps cannot join the cached steps. Evaluate every step, and
		// remove the entry so the next request does not evaluate twice.
		c.remove(key, usable)
		return evalAll()
	}

	var cached []rangeCacheSeries
	if usable != nil {
		cached = usable.series
	}
	result := rangeQueryResult{backend: fresh.backend, floats: true, groups: rangeCacheGroups(cached, freshSeries, start, evalStart, lastStep)}

	// Keep the steps that no late sample can change.
	cutoff := c.now().UnixMilli() - c.offset.Milliseconds() - step
	if cutoff < start {
		return result, status, nil
	}
	settledEnd := start + (min(cutoff, lastStep)-start)/step*step
	stored := &rangeCacheEntry{start: start, end: settledEnd, created: c.now()}
	if usable != nil {
		if settledEnd <= usable.end {
			return result, status, nil
		}
		stored.start, stored.created = usable.start, usable.created
	}
	if (stored.end-stored.start)/step+1 > rangeCacheMaxSteps {
		stored.start = stored.end - (rangeCacheMaxSteps-1)*step
	}
	stored.series = rangeCacheMergeSeries(cached, freshSeries, stored.start, evalStart, stored.end)
	// An empty result can mean that the samples are not in ClickHouse yet.
	if len(stored.series) == 0 {
		return result, status, nil
	}
	stored.bytes = rangeCacheEntryBytes(stored.series)
	c.put(key, stored, usable)
	return result, status, nil
}

// rangeCacheSeriesFromGroups returns the series sorted by labels. It returns
// false when two series have the same labels, because the cache joins the
// steps of a series by its labels.
func rangeCacheSeriesFromGroups(groups []rangeAggregateGroup) ([]rangeCacheSeries, bool) {
	series := make([]rangeCacheSeries, 0, len(groups))
	for _, group := range groups {
		set := labels.FromMap(group.metric)
		series = append(series, rangeCacheSeries{labels: set, id: string(set.Bytes(nil)), metric: group.metric, points: group.points})
	}
	sort.Slice(series, func(i, j int) bool { return labels.Compare(series[i].labels, series[j].labels) < 0 })
	for i := 1; i < len(series); i++ {
		if series[i].id == series[i-1].id {
			return nil, false
		}
	}
	return series, true
}

// rangeCacheMergeSeries joins cached and fresh series, sorted by labels.
// Each series gets its cached points from start to before split, then its
// fresh points from split to end. A series without points is left out.
func rangeCacheMergeSeries(cached, fresh []rangeCacheSeries, start, split, end int64) []rangeCacheSeries {
	freshByID := make(map[string]*rangeCacheSeries, len(fresh))
	for i := range fresh {
		freshByID[fresh[i].id] = &fresh[i]
	}
	merged := make([]rangeCacheSeries, 0, max(len(cached), len(fresh)))
	add := func(series rangeCacheSeries, head, tail []samplePoint) {
		head = rangeCachePointsBetween(head, start, min(split-1, end))
		tail = rangeCachePointsBetween(tail, max(split, start), end)
		if len(head)+len(tail) == 0 {
			return
		}
		// The copy keeps this series separate from the slices it came from.
		series.points = append(append(make([]samplePoint, 0, len(head)+len(tail)), head...), tail...)
		merged = append(merged, series)
	}
	for _, series := range cached {
		var tail []samplePoint
		if match, ok := freshByID[series.id]; ok {
			tail = match.points
			delete(freshByID, series.id)
		}
		add(series, series.points, tail)
	}
	for i := range fresh {
		if _, ok := freshByID[fresh[i].id]; ok {
			add(fresh[i], nil, fresh[i].points)
		}
	}
	sort.Slice(merged, func(i, j int) bool { return labels.Compare(merged[i].labels, merged[j].labels) < 0 })
	return merged
}

func rangeCacheGroups(cached, fresh []rangeCacheSeries, start, split, end int64) []rangeAggregateGroup {
	merged := rangeCacheMergeSeries(cached, fresh, start, split, end)
	groups := make([]rangeAggregateGroup, 0, len(merged))
	for _, series := range merged {
		groups = append(groups, rangeAggregateGroup{metric: series.metric, points: series.points})
	}
	return groups
}

// rangeCachePointsBetween includes the points at start and at end.
func rangeCachePointsBetween(points []samplePoint, start, end int64) []samplePoint {
	first := sort.Search(len(points), func(i int) bool { return points[i].t >= start })
	last := sort.Search(len(points), func(i int) bool { return points[i].t > end })
	if first >= last {
		return nil
	}
	return points[first:last]
}

// rangeCacheEntryBytes estimates the memory of an entry. The constants are
// sized so that the estimate is not below the measured heap use of entries
// with 3 to 25 labels, which keeps the cache in its size limit.
func rangeCacheEntryBytes(series []rangeCacheSeries) int64 {
	const entryOverhead, seriesOverhead, labelOverhead, pointBytes, labelCopies = 128, 320, 80, 16, 3
	bytes := int64(entryOverhead)
	for _, s := range series {
		// The labels, the id and the metric map each hold the label text.
		bytes += seriesOverhead + int64(len(s.metric))*labelOverhead + int64(len(s.id))*labelCopies + int64(len(s.points))*pointBytes
	}
	return bytes
}

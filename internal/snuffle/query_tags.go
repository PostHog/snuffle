package snuffle

import (
	"context"
	"encoding/json"
	"strings"
)

// maxQueryTagsHeaderBytes bounds the caller tags copied into every query's
// log_comment, so one header cannot inflate system.query_log rows.
const maxQueryTagsHeaderBytes = 4 << 10

type queryTagType int

const (
	queryTagString queryTagType = iota
	queryTagInt
	queryTagBool
)

// callerQueryTagTypes lists the log_comment keys a caller may set. The names
// and types follow PostHog's query_log_archive log_comment tuple, which parses
// log_comment with a typed schema.
var callerQueryTagTypes = map[string]queryTagType{
	"access_method":   queryTagString,
	"api_key_label":   queryTagString,
	"api_key_mask":    queryTagString,
	"client_query_id": queryTagString,
	"dashboard_id":    queryTagInt,
	"feature":         queryTagString,
	"id":              queryTagString,
	"insight_id":      queryTagInt,
	"is_impersonated": queryTagBool,
	"kind":            queryTagString,
	"name":            queryTagString,
	"org_id":          queryTagString,
	"product":         queryTagString,
	"request_name":    queryTagString,
	"route_id":        queryTagString,
	"session_id":      queryTagString,
	"user_id":         queryTagInt,
	"workload":        queryTagString,
}

// Snuffle owns these keys. A caller value for one of them is dropped.
var reservedQueryTags = map[string]struct{}{
	"service_name": {},
	"team_id":      {},
	"query_type":   {},
}

// routeQueryTypes names each ClickHouse-backed endpoint in log_comment.
// The names are part of the attribution contract, so keep them stable.
var routeQueryTypes = map[string]string{
	"/api/v1/query":                   "query",
	"/api/v1/query_range":             "query_range",
	"/api/v1/write":                   "remote_write",
	"/api/v1/read":                    "remote_read",
	"/api/v1/labels":                  "labels",
	"/api/v1/label/:name/values":      "label_values",
	"/api/v1/series":                  "series",
	"/api/v1/metadata":                "metadata",
	"/api/v1/query_exemplars":         "query_exemplars",
	"/loki/api/v1/push":               "loki_push",
	"/loki/api/v1/query":              "loki_query",
	"/loki/api/v1/query_range":        "loki_query_range",
	"/loki/api/v1/labels":             "loki_labels",
	"/loki/api/v1/label/:name/values": "loki_label_values",
	"/loki/api/v1/series":             "loki_series",
	"/loki/api/v1/index/stats":        "loki_index_stats",
}

const selfScrapeQueryType = "self_scrape"

type queryTags struct {
	teamID    uint64
	product   string
	queryType string
	caller    map[string]any
}

type logCommentKey struct{}

const defaultLogComment = `{"service_name":"snuffle"}`

func routeQueryTags(endpoint string, teamID uint64) queryTags {
	product := "metrics"
	if strings.HasPrefix(endpoint, "/loki/") {
		product = "logs"
	}
	queryType, ok := routeQueryTypes[endpoint]
	if !ok {
		queryType = "other"
	}
	return queryTags{teamID: teamID, product: product, queryType: queryType}
}

// logComment encodes the tags as the JSON object ClickHouse stores in
// system.query_log.log_comment. Snuffle's own keys are written last, so
// caller tags cannot replace them.
func (t queryTags) logComment() string {
	comment := make(map[string]any, len(t.caller)+4)
	for key, value := range t.caller {
		comment[key] = value
	}
	if _, ok := comment["product"]; !ok && t.product != "" {
		comment["product"] = t.product
	}
	comment["service_name"] = "snuffle"
	comment["team_id"] = t.teamID
	comment["query_type"] = t.queryType
	payload, err := json.Marshal(comment)
	if err != nil {
		return defaultLogComment
	}
	return string(payload)
}

func withQueryTags(ctx context.Context, tags queryTags) context.Context {
	return context.WithValue(ctx, logCommentKey{}, tags.logComment())
}

func logCommentFromContext(ctx context.Context) string {
	if comment, ok := ctx.Value(logCommentKey{}).(string); ok {
		return comment
	}
	return defaultLogComment
}

// parseCallerQueryTags keeps the allowlisted, correctly typed keys of a JSON
// object header. It never fails the request: each problem is returned as a
// reason for the rejection metric, and the offending key or header is dropped.
func parseCallerQueryTags(header string) (map[string]any, []string) {
	if header == "" {
		return nil, nil
	}
	if len(header) > maxQueryTagsHeaderBytes {
		return nil, []string{"too_large"}
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(header), &raw); err != nil {
		return nil, []string{"invalid_json"}
	}
	var tags map[string]any
	var rejected []string
	for key, value := range raw {
		if _, ok := reservedQueryTags[key]; ok {
			rejected = append(rejected, "reserved_key")
			continue
		}
		typ, ok := callerQueryTagTypes[key]
		if !ok {
			rejected = append(rejected, "unknown_key")
			continue
		}
		if string(value) == "null" {
			continue
		}
		parsed, ok := parseQueryTagValue(typ, value)
		if !ok {
			rejected = append(rejected, "invalid_type")
			continue
		}
		if tags == nil {
			tags = make(map[string]any, len(raw))
		}
		tags[key] = parsed
	}
	return tags, rejected
}

func parseQueryTagValue(typ queryTagType, value json.RawMessage) (any, bool) {
	var parsed any
	var err error
	switch typ {
	case queryTagString:
		var s string
		err = json.Unmarshal(value, &s)
		parsed = s
	case queryTagInt:
		var n int64
		err = json.Unmarshal(value, &n)
		parsed = n
	case queryTagBool:
		var b bool
		err = json.Unmarshal(value, &b)
		parsed = b
	default:
		return nil, false
	}
	return parsed, err == nil
}

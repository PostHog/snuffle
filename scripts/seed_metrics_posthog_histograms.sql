-- Histograms for the posthog_metrics histogram_quantile scenarios (ENG-010, ENG-031).
-- Classic histograms: 2,000 series x 15 buckets scraped every 15 s for 24 h 10 m.
INSERT INTO metrics4_input (team_id, metric_name, series_fingerprint, timestamp, observed_timestamp, original_expiry_timestamp, service_name, metric_type, aggregation_temporality, value, count, histogram_bounds, histogram_counts, has_labels, resource_attributes, attributes)
SELECT
    {tenant:Int32},
    'django_http_requests_latency_seconds_by_view_method',
    cityHash64('perf-cumulative', s),
    fromUnixTimestamp64Milli(toUnixTimestamp64Milli(parseDateTime64BestEffort({start:String}, 3, 'UTC')) + toInt64(m) * 15000, 'UTC'),
    now64(6),
    now64(6) + INTERVAL 1 DAY,
    'django',
    'histogram',
    'cumulative',
    arraySum(counts) * 0.1,
    arraySum(counts),
    [0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10, 25],
    arrayMap(i -> toUInt64(c * (16 - i) * (1 + (s + i) % 3) + intDiv(c * c, 4000) * (i % 4)), range(15)) AS counts,
    m % 240 = 0,
    map('region', 'us'),
    map('view', concat('view-', toString(s % 150)), 'method', ['GET', 'POST'][1 + s % 2], 'instance', toString(s))
FROM (SELECT intDiv(number, 5800) AS s, number % 5800 AS m, if(s % 50 = 0, m % 3000, m) AS c FROM numbers(11600000));

-- Delta histograms: one point per event, 1.9M a day for 7 days across 2,000 series.
INSERT INTO metrics4_input (team_id, metric_name, series_fingerprint, timestamp, observed_timestamp, original_expiry_timestamp, service_name, metric_type, aggregation_temporality, value, count, histogram_bounds, histogram_counts, has_labels, resource_attributes, attributes)
SELECT
    {tenant:Int32},
    'llm_call_duration_seconds',
    cityHash64('perf-delta', number % 2000),
    fromUnixTimestamp64Milli(toUnixTimestamp64Milli(parseDateTime64BestEffort({start:String}, 3, 'UTC')) + intDiv(toInt64(number) * 604800000, 13300000), 'UTC'),
    now64(6),
    now64(6) + INTERVAL 1 DAY,
    'llm-gateway',
    'histogram',
    'delta',
    0.3,
    1,
    [0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10, 25],
    arrayMap(i -> toUInt64(i = 14 - toUInt8(floor(sqrt(cityHash64(number) % 225)))), range(15)),
    intDiv(number, 2000) % 1000 = 0,
    map('region', 'us'),
    map('model', concat('model-', toString(number % 2000)))
FROM numbers(13300000);

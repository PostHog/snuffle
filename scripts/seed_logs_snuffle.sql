INSERT INTO log_streams (team_id, stream_id, labels, resource_attributes, updated_at)
WITH
    number AS n,
    concat('svc-', toString(n % 64)) AS service,
    concat('host-', toString(n % 1024)) AS host,
    arrayElement(['us-east', 'us-west', 'eu-central', 'ap-south'], (n % 4) + 1) AS region,
    arrayElement(['prod', 'staging', 'dev'], (n % 3) + 1) AS env,
    if(n % 2 = 0, 'logfmt', 'json') AS fmt,
    if(n % 97 = 0, 'error', if(n % 19 = 0, 'warn', if(n % 7 = 0, 'debug', 'info'))) AS sev,
    xxHash64(concat('app=snuffle-bench|env=', env, '|format=', fmt, '|host=', host, '|level=', sev, '|region=', region, '|service_name=', service, '|resource.host.hostname=', host, '|resource.region=', region, '|resource.service.name=', service)) AS stream_id,
    map('app', 'snuffle-bench', 'env', env, 'format', fmt, 'host', host, 'level', sev, 'severity_text', sev, 'detected_level', sev, 'region', region, 'service_name', service, 'service.name', service) AS labels,
    map('service.name', service, 'host.hostname', host, 'region', region) AS resource_attributes
SELECT DISTINCT
    {tenant:Int32},
    stream_id,
    labels,
    resource_attributes,
    now64(6)
FROM numbers({rows:UInt64});

INSERT INTO log_stream_stats (team_id, bucket, stream_id, log_count, byte_count)
WITH
    number AS n,
    toDateTime64({bench_start:String}, 6, 'UTC') + toIntervalSecond(n % 86400) + toIntervalMicrosecond(intDiv(n, 86400) % 1000000) AS ts,
    concat('svc-', toString(n % 64)) AS service,
    concat('pod-', toString(n % 256)) AS pod,
    concat('host-', toString(n % 1024)) AS host,
    arrayElement(['us-east', 'us-west', 'eu-central', 'ap-south'], (n % 4) + 1) AS region,
    arrayElement(['prod', 'staging', 'dev'], (n % 3) + 1) AS env,
    if(n % 2 = 0, 'logfmt', 'json') AS fmt,
    if(n % 97 = 0, 'error', if(n % 19 = 0, 'warn', if(n % 7 = 0, 'debug', 'info'))) AS sev,
    arrayElement(['/checkout', '/login', '/search', '/api/items', '/health'], (n % 5) + 1) AS route,
    if(n % 97 = 0, '500', if(n % 23 = 0, '404', '200')) AS status,
    toString(5 + (n % 2000)) AS duration_ms,
    toString(128 + (n % 8192)) AS size_b,
    xxHash64(concat('app=snuffle-bench|env=', env, '|format=', fmt, '|host=', host, '|level=', sev, '|region=', region, '|service_name=', service, '|resource.host.hostname=', host, '|resource.region=', region, '|resource.service.name=', service)) AS stream_id,
    if(
        fmt = 'logfmt',
        concat('level=', sev, ' service=', service, ' env=', env, ' region=', region, ' pod=', pod, ' route=', route, ' status=', status, ' duration=', duration_ms, 'ms size=', size_b, 'B request_id=req-', toString(n), if(n % 97 = 0, ' error=true message=checkout_failed', ' message=ok')),
        concat('{"level":"', sev, '","service":"', service, '","env":"', env, '","region":"', region, '","pod":"', pod, '","route":"', route, '","status":', status, ',"duration":', duration_ms, ',"size":', size_b, ',"message":"', if(n % 97 = 0, 'checkout error', 'ok'), '"}')
    ) AS line
SELECT
    {tenant:Int32},
    toStartOfMinute(ts) AS bucket,
    stream_id,
    count(),
    sum(length(line))
FROM numbers({rows:UInt64})
GROUP BY
    bucket,
    stream_id;

INSERT INTO logs (
    team_id,
    timestamp,
    expires_at,
    stream_id,
    observed_ns,
    body,
    fields
)
WITH
    number AS n,
    toDateTime64({bench_start:String}, 6, 'UTC') + toIntervalSecond(n % 86400) + toIntervalMicrosecond(intDiv(n, 86400) % 1000000) AS ts,
    concat('svc-', toString(n % 64)) AS service,
    concat('pod-', toString(n % 256)) AS pod,
    concat('host-', toString(n % 1024)) AS host,
    arrayElement(['us-east', 'us-west', 'eu-central', 'ap-south'], (n % 4) + 1) AS region,
    arrayElement(['prod', 'staging', 'dev'], (n % 3) + 1) AS env,
    if(n % 2 = 0, 'logfmt', 'json') AS fmt,
    if(n % 97 = 0, 'error', if(n % 19 = 0, 'warn', if(n % 7 = 0, 'debug', 'info'))) AS sev,
    arrayElement(['/checkout', '/login', '/search', '/api/items', '/health'], (n % 5) + 1) AS route,
    if(n % 97 = 0, '500', if(n % 23 = 0, '404', '200')) AS status,
    toString(5 + (n % 2000)) AS duration_ms,
    toString(128 + (n % 8192)) AS size_b,
    xxHash64(concat('app=snuffle-bench|env=', env, '|format=', fmt, '|host=', host, '|level=', sev, '|region=', region, '|service_name=', service, '|resource.host.hostname=', host, '|resource.region=', region, '|resource.service.name=', service)) AS stream_id,
    if(
        fmt = 'logfmt',
        concat('level=', sev, ' service=', service, ' env=', env, ' region=', region, ' pod=', pod, ' route=', route, ' status=', status, ' duration=', duration_ms, 'ms size=', size_b, 'B request_id=req-', toString(n), if(n % 97 = 0, ' error=true message=checkout_failed', ' message=ok')),
        concat('{"level":"', sev, '","service":"', service, '","env":"', env, '","region":"', region, '","pod":"', pod, '","route":"', route, '","status":', status, ',"duration":', duration_ms, ',"size":', size_b, ',"message":"', if(n % 97 = 0, 'checkout error', 'ok'), '"}')
    ) AS line
SELECT
    {tenant:Int32},
    ts,
    ts + toIntervalDay(30),
    stream_id,
    toInt64(toUnixTimestamp64Nano(ts)),
    line,
    CAST(map(), 'Map(LowCardinality(String), String)')
FROM numbers({rows:UInt64})
SETTINGS max_insert_threads = 4;

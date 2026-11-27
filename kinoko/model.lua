-- Loki ingest durability: every push Snuffle acks (2xx) must be readable back through
-- LogQL, exactly once, whatever happens to ClickHouse or Snuffle. Reference: https://kinoko.sh/docs

-- settle_s: a killed ClickHouse takes ~10 s to come back; judge `eventually` after that.
config { ready_deadline_s = 180, max_fault_s = 15, settle_s = 30 }

local SNUFFLE = "http://snuffle:9091"

setup("snuffle and clickhouse up", "client", [=[
until curl -sf -o /dev/null -H 'Content-Type: application/json' \
  -d '{"streams":[{"stream":{"job":"warmup"},"values":[["'"$(date +%s)"'000000000","warmup"]]}]}' \
  ]=] .. SNUFFLE .. [=[/loki/api/v1/push; do sleep 0.5; done
]=])

-- One command so the acked list is read *before* the query: anything acked is then
-- already stored, which makes "missing" checkable at every beat, not just once settled.
-- Prints "<missing> <duplicated>", or "down" when Snuffle could not answer.
resource("ingest", {
  bash = "client",
  command = [=[
acked=$(sort -u /tmp/acked 2>/dev/null)
now=$(date +%s)
out=$(curl -sf -m 3 -G ]=] .. SNUFFLE .. [=[/loki/api/v1/query_range \
  --data-urlencode 'query={job="kinoko"}' --data-urlencode limit=100000 \
  --data-urlencode start=$((now - 3600)) --data-urlencode end=$((now + 60))) || { echo down; exit 0; }
echo "$out" | grep -o 'kinoko-[0-9]*' | cut -d- -f2 | sort > /tmp/stored.$$
missing=$(echo "$acked" | grep . | grep -vxF -f /tmp/stored.$$ -c)
dups=$(uniq -d /tmp/stored.$$ | wc -l); rm -f /tmp/stored.$$
echo "$missing $dups"
]=],
  read = function(out)
    local missing, dups = out.stdout:match("^(%d+) (%d+)")
    if not missing then return { ok = false } end
    return { ok = true, missing = tonumber(missing) > 0, duplicated = tonumber(dups) > 0 }
  end,
  steer = true,
})

-- A Promtail/Alloy-like client: unique line id, retry on 5xx or no answer, record an ack.
-- Prints the last HTTP status (000 = no answer).
action("push", { bash = "client" }, function(draw, r)
  return { command = [=[
id=$(flock /tmp/lock sh -c 'n=$(( $(cat /tmp/n 2>/dev/null || echo 0) + 1 )); echo $n > /tmp/n; echo $n')
body='{"streams":[{"stream":{"job":"kinoko"},"values":[["'"$(date +%s)"'000000000","kinoko-'$id'"]]}]}'
for try in 1 2 3; do
  code=$(curl -s -o /dev/null -w '%{http_code}' -m 3 -H 'Content-Type: application/json' -d "$body" ]=] .. SNUFFLE .. [=[/loki/api/v1/push)
  case $code in 2*) echo $id >> /tmp/acked; break ;; 4*) break ;; esac
done
echo $code
]=] }
end)

-- bash, not http: Snuffle wants an explicit start (no Loki-style `since` default) and
-- the model has no clock. Prints the HTTP status (000 = no answer).
action("query", { bash = "client" }, function(draw, r)
  return { command = [=[
now=$(date +%s)
curl -s -o /dev/null -w '%{http_code}' -m 3 -G ]=] .. SNUFFLE .. [=[/loki/api/v1/query_range \
  --data-urlencode 'query={job="kinoko"}' --data-urlencode limit=1000 \
  --data-urlencode start=$((now - 3600)) --data-urlencode end=$now
]=] }
end)

always("every acked push is queryable", function(r)
  return not r.ingest.ok or not r.ingest.missing
end)
always("no pushed line is returned twice", function(r)
  return not r.ingest.ok or not r.ingest.duplicated
end)
eventually("snuffle answers queries once faults end", function(r) return r.ingest.ok end)

-- A valid push is never refused as the client's fault, whatever the backend is doing.
reply("push", "faults never turn into 4xx", function(asked, answer)
  if answer == nil or answer.err ~= nil then return true end
  return not answer.result.stdout:match("^4")
end)
reply("query", "query answers 200, 5xx or nothing", function(asked, answer)
  if answer == nil or answer.err ~= nil then return true end
  local s = answer.result.stdout
  return s:match("^200") or s:match("^5") or s:match("^000")
end)

fault { down = "clickhouse" }
fault { pause = "clickhouse" }
fault { down = "snuffle" }
fault { pause = "snuffle" }
fault { cut = { "snuffle", "clickhouse" } }
fault { slow = "snuffle", to = "clickhouse", ms = 500 }

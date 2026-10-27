#!/usr/bin/env bash
# End-to-end check of the Docker deployment:
#   1. bring the stack up, wait for 3 healthy workers
#   2. send traffic through the gateway
#   3. docker kill one worker container (an external SIGKILL)
#   4. assert the controller opened an incident, restarted the container, and closed it
set -euo pipefail

PYBIN=$(command -v python3 || command -v python)
API=${API:-http://localhost:8090}
GW=${GW:-http://localhost:8080}

echo "== up"
docker compose up -d
for i in $(seq 1 60); do
  healthy=$(curl -fs "$API/api/state" 2>/dev/null | "$PYBIN" -c 'import sys,json; print(json.load(sys.stdin)["fleet"]["healthy"])' 2>/dev/null || echo 0)
  [ "$healthy" = "3" ] && break
  sleep 1
done
[ "$healthy" = "3" ] || { echo "fleet never became healthy"; curl -s "$API/api/state"; exit 1; }
echo "== 3/3 healthy"

echo "== traffic"
for i in $(seq 1 30); do
  code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$GW/infer" -H 'content-type: application/json' -d '{"tokens":16}')
  [ "$code" = "200" ] || { echo "request $i failed with $code"; exit 1; }
done

echo "== kill sentinel-w2"
t0=$(date +%s.%N)
docker kill sentinel-w2 >/dev/null
for i in $(seq 1 60); do
  status=$(curl -fs "$API/api/incidents" | "$PYBIN" -c 'import sys,json; i=[x for x in json.load(sys.stdin)["incidents"] if x["worker"]=="w2"]; print(i[0]["status"] if i else "none")')
  [ "$status" = "recovered" ] && break
  sleep 0.5
done
t1=$(date +%s.%N)
[ "$status" = "recovered" ] || { echo "w2 incident status: $status"; curl -s "$API/api/incidents"; exit 1; }
echo "== w2 recovered in $("$PYBIN" -c "print(round($t1-$t0,2))") s (wall, coarse)"
curl -fs "$API/api/incidents" | "$PYBIN" -c 'import sys,json; i=json.load(sys.stdin)["incidents"][0]; print("incident:", i["reason"], "attempts", i["attempts"], "rung", i["escalation"], "detect->healthy ms", i["detect_to_recover_ms"])'

echo "== traffic after recovery"
for i in $(seq 1 30); do
  code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$GW/infer" -H 'content-type: application/json' -d '{"tokens":16}')
  [ "$code" = "200" ] || { echo "request $i failed with $code"; exit 1; }
done
echo "== OK"

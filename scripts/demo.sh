#!/usr/bin/env bash
# Demo against the docker-compose stack: creates the example rule, sends signals, shows the resulting notifications.
set -euo pipefail
API=${API:-http://localhost:8080/api/v1/tenants/acme}
KEY=${KEY:-cust-$RANDOM}
NOW=$(date -u +%Y-%m-%dT%H:%M:%SZ)
DUE=$(date -u -d '+3 days' +%Y-%m-%d 2>/dev/null || date -u -v+3d +%Y-%m-%d)

echo "== create rule r1 (example from the spec, ignore 409 if it already exists)"
curl -s -X POST "$API/rules" -H 'content-type: application/json' --data-binary @"$(dirname "$0")/example-rule.json" | head -c 400; echo

produce() {
  docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic signals.in \
    --property parse.key=true --property key.separator='|'
}

echo "== send signals for $KEY (due_date=$DUE)"
{
  echo "acme:$KEY|{\"signal_id\":\"$KEY-email\",\"tenant_id\":\"acme\",\"correlation_key\":\"$KEY\",\"signal\":\"customer_email\",\"kind\":\"KPI\",\"value\":\"someone@example.com\",\"occurred_at\":\"$NOW\"}"
  echo "acme:$KEY|{\"signal_id\":\"$KEY-due\",\"tenant_id\":\"acme\",\"correlation_key\":\"$KEY\",\"signal\":\"due_date\",\"kind\":\"DATE\",\"value\":\"$DUE\",\"occurred_at\":\"$NOW\"}"
  echo "acme:$KEY|{\"signal_id\":\"$KEY-kpi2\",\"tenant_id\":\"acme\",\"correlation_key\":\"$KEY\",\"signal\":\"kpi2\",\"kind\":\"KPI\",\"value\":true,\"occurred_at\":\"$NOW\"}"
} | produce

sleep 3
echo "== notifications for $KEY"
curl -s "$API/notifications?correlationKey=$KEY" | python3 -m json.tool | grep -E '"(id|templateId|status|action|dueAt)"' || true
echo "== dry-run with simulated NOW on the 1st of next month"
FIRST=$(date -u -d "$(date -u +%Y-%m-01) +1 month" +%Y-%m-01T00:20:00+01:00 2>/dev/null || echo "2027-01-01T00:20:00+01:00")
curl -s -X POST "$API/rules/dry-run" -H 'content-type: application/json' \
  -d "{\"ruleId\":\"r1\",\"correlationKey\":\"$KEY\",\"now\":\"$(python3 -c "import datetime;print(datetime.datetime.fromisoformat('$FIRST').astimezone(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'))")\",\"signals\":{\"kpi1\":{\"value\":7},\"customer_email\":{\"value\":\"x@y.z\"},\"due_date\":{\"kind\":\"DATE\",\"value\":\"$DUE\"}}}" \
  | python3 -m json.tool | head -40

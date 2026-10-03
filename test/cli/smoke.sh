#!/bin/sh
# Runs inside the redis:7-alpine container (see scripts/redis-cli-smoke.ps1).
# One redis-cli invocation per line of commands.txt so the transcript can show
# each command next to its reply, then a pub/sub round trip.
HOST=${EMBER_HOST:-ember-smoke}
PORT=${EMBER_PORT:-6380}

echo "# $(redis-cli --version)"
echo "# server: $HOST:$PORT"
while IFS= read -r line; do
  printf '127.0.0.1:%s> %s\n' "$PORT" "$line"
  echo "$line" | redis-cli --no-raw -h "$HOST" -p "$PORT"
done < /cli/commands.txt

echo
echo "# Pub/sub: a pattern subscriber on one connection, a publisher on another"
(timeout 3 redis-cli -h "$HOST" -p "$PORT" psubscribe 'ne*' > /tmp/sub.out 2>&1 &)
sleep 1
printf '127.0.0.1:%s> PUBLISH news "breaking"\n' "$PORT"
redis-cli --no-raw -h "$HOST" -p "$PORT" publish news breaking
sleep 3
echo "--- subscriber saw:"
cat /tmp/sub.out

#!/bin/sh
# Differential test, run inside the redis:7-alpine client container. Sends the same
# commands to a real Redis and to Ember and prints a diff of the replies.
# usage: compat.sh REDIS_HOST REDIS_PORT EMBER_HOST EMBER_PORT
set -u
RH=$1 RP=$2 EH=$3 EP=$4

cat > /tmp/cmds.txt <<'EOF'
SET str hello
RPUSH lst a b c
GET lst
LPUSH str x
RPUSH str x
LPOP str
LLEN str
LRANGE str 0 -1
APPEND lst x
STRLEN lst
INCR str
INCRBY str abc
DECRBY str abc
SET big 9223372036854775807
INCR big
SET small -9223372036854775808
DECR small
DECRBY n -9223372036854775808
GET
GET a b
SET a
SET a b NOPE
SET a b EX
SET a b EX 0
SET a b EX -1
SET a b EX abc
SET a b PX 9223372036854775807
SET a b NX XX
SET a b EX 10 PX 10
MSET a
MSET a 1 b
EXPIRE str abc
EXPIRE nokey 10
EXPIRE str 9223372036854775807
PEXPIRE str abc
TTL nokey
PTTL nokey
TTL str
TYPE nokey
LRANGE lst a b
LPOP lst -1
LPOP lst abc
LPOP lst 1 2
LPOP nokey
LPOP nokey 2
RPOP nokey
DEL
EXISTS
FROBNICATE
FROBNICATE x y z w v
KEYS
DBSIZE extra
ECHO
PING a b
SELECT x
GETDEL lst
GETDEL nokey
STRLEN nokey
LLEN nokey
LRANGE nokey 0 -1
SET k v GET
SET k v NX GET
EOF

run() { # host port
  while IFS= read -r line; do
    printf '> %s\n' "$line"
    echo "$line" | redis-cli -h "$1" -p "$2"
  done < /tmp/cmds.txt
}

redis-cli -h "$RH" -p "$RP" flushall >/dev/null
redis-cli -h "$EH" -p "$EP" flushall >/dev/null
run "$RH" "$RP" > /tmp/redis.out 2>&1
run "$EH" "$EP" > /tmp/ember.out 2>&1
echo "commands compared: $(grep -c '^> ' /tmp/redis.out)"
if diff -u /tmp/redis.out /tmp/ember.out > /tmp/diff.out; then
  echo "IDENTICAL: Redis and Ember replied the same to every command"
else
  echo "DIFFERENCES (--- real Redis, +++ Ember):"
  cat /tmp/diff.out
  exit 1
fi

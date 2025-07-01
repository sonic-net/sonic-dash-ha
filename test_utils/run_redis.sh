#!/bin/bash
# Check if both arguments are provided
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <database_config.json> <redis_data.cmd>"
    exit 1
fi

# Access the arguments
db_cfg="$1"
db_data="$2"
mkdir -p /var/run/redis/sonic-db
cp $db_cfg /var/run/redis/sonic-db/
redis-server --appendonly no --save --notify-keyspace-events AKE --port 6379 --unixsocket /var/run/redis/redis.sock&
sleep 3
redis-cli < $db_data
This folder contains some utilities, configurations, instructions on running manual test in development environment.

# Run hamgrd, swbusd and swbus-cli
- Start redis db
    cd test_utils
    ./run_redis.sh hamgrd/database_config.json hamgrd/redis_data_set.cmd 

- Start swbusd
    target/debug/swbusd -s 0

- Start hamgrd
    target/debug/hamgrd -s 0

- Run swbus-cli
    DEV=dpu0 target/debug/swbus-cli show hamgrd actor /hamgrd/0/dpu/switch1_dpu0
# SONiC - DASH HA (High Availability)

## Getting Started

### Prerequisites

1. Install Rust using [rustup](https://rustup.rs/).

2. Install Protobuf compiler (`protoc`)

    ```sh
    # Ubuntu
    sudo apt-get install protobuf-compiler
    
    # Windows
    winget install -e --id Google.Protobuf
    ```

3. Install [libswsscommon](https://github.com/sonic-net/sonic-swss-common) and its dependencies (optional, for swss-common/hamgrd, Linux only) 

    1. Clone the swss-common repo

        ```sh
        git clone https://github.com/sonic-net/sonic-swss-common
        ```

    2. Modify `loadLoadScript` from `sonic-swss-common/common/redisapi.h` to change hard-coded paths.

        You can do this by hand, or apply the following patch.
        (Modify the part in `<>` to point to the directory where you cloned sonic-swss-common)
        ```sh
        diff --git a/common/redisapi.h b/common/redisapi.h
        index bdb32b5..cb44c38 100644
        --- a/common/redisapi.h
        +++ b/common/redisapi.h
        @@ -63,7 +63,7 @@ static inline std::string loadLuaScript(const std::string& path)
         {
             SWSS_LOG_ENTER();

        -    return readTextFile("/usr/share/swss/" + path);
        +    return readTextFile("<path to sonic-swss-common>/common/" + path);
         }

         static inline std::set<std::string> runRedisScript(RedisContext &ctx, const std::string& sha,
        ```

    3. Build the repo using the instructions from their README.

        (Their README is a bit out of date, and is missing some dependencies. Sorry about that.)

        ```sh
        cd sonic-swss-common
        ./autogen.sh
        ./configure
        make -j `nproc`
        ```

    4. Set `SWSS_COMMON_REPO` and `LD_LIBRARY_PATH` to the local paths of sonic-swss-common.

        ```sh
        export SWSS_COMMON_REPO="<path to sonic-swss-common>"
        export LD_LIBRARY_PATH="$SWSS_COMMON_REPO/common/.libs"
        ```

4. Install GNU Make (optional)

    ```sh
    # Ubuntu
    sudo apt-get install make

    # Windows
    winget install -e --id GnuWin32.Make
    ```

## Contribution guide

See the [contributors guide](https://github.com/sonic-net/SONiC/wiki/Becoming-a-contributor) for information about how to contribute.

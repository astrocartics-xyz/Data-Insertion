cmake_minimum_required(VERSION 3.16)
project(kill_update)

set(CMAKE_CXX_STANDARD 17)

include(FetchContent)

FetchContent_Declare(
    nlohmann_json
    GIT_REPOSITORY https://github.com/nlohmann/json.git
    GIT_TAG v3.11.2
)
FetchContent_MakeAvailable(nlohmann_json)

FetchContent_Declare(
    libpqxx
    GIT_REPOSITORY https://github.com/jtv/libpqxx.git
    GIT_TAG 7.7.5
)
FetchContent_MakeAvailable(libpqxx)

find_package(CURL REQUIRED)
find_package(PostgreSQL REQUIRED)

add_executable(kill_update kill_update.cpp)

target_link_libraries(kill_update
    PRIVATE
        nlohmann_json::nlohmann_json
        pqxx
        pq
        CURL::libcurl
        PostgreSQL::PostgreSQL
)

target_include_directories(kill_update PRIVATE
    ${CURL_INCLUDE_DIRS}
)

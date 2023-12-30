cmake_minimum_required(VERSION 3.10.2 FATAL_ERROR)
project(ROLEX_dev)

#Compiler options
set(CMAKE_C_FLAGS "-Wall -Wno-deprecated-declarations -Wsign-compare -g")   # -DNDEBUG
# set(CMAKE_C_FLAGS "-Wall -Wno-deprecated-declarations -Wno-unused-variable -Wno-unused-but-set-variable -Wsign-compare -O3")  # -O3
set(CMAKE_CXX_FLAGS "${CMAKE_C_FLAGS} -std=c++17")

#Link Options
set(LINKS_FLAGS "-lnuma -lcityhash -lboost_system -lboost_coroutine -lboost_context -lpthread -libverbs -lmemcached -ltbb")

#Env Options
option (STATIC_MN_IP "Use static MNs according the IPs of the nodes" OFF)
option (ENABLE_CORO "Turn on the coroutine" ON)
option (LONG_TEST_EPOCH "Use big epoch num and long epoch duration" OFF)
option (SHORT_TEST_EPOCH "Use small epoch num and short epoch duration" OFF)
option (MIDDLE_TEST_EPOCH "Use middle epoch num and short epoch duration" OFF)

if(STATIC_MN_IP)
    add_definitions(-DSTATIC_ID_FROM_IP)
else()
    remove_definitions(-DSTATIC_ID_FROM_IP)
endif()

if(ENABLE_CORO)
    add_definitions(-DUSE_CORO)
else()
    remove_definitions(-DUSE_CORO)
endif()

if(LONG_TEST_EPOCH)
    add_definitions(-DLONG_TEST_EPOCH)
else()
    remove_definitions(-DLONG_TEST_EPOCH)
endif()

if(SHORT_TEST_EPOCH)
    add_definitions(-DSHORT_TEST_EPOCH)
else()
    remove_definitions(-DSHORT_TEST_EPOCH)
endif()

if(MIDDLE_TEST_EPOCH)
    add_definitions(-DMIDDLE_TEST_EPOCH)
else()
    remove_definitions(-DMIDDLE_TEST_EPOCH)
endif()


#Include files
set(INCLUDE_BASE ${PROJECT_SOURCE_DIR}/include)
include_directories(${INCLUDE_BASE})
include_directories(${INCLUDE_BASE}/rolex_libs)
include_directories(${INCLUDE_BASE}/rolex_libs/rolex)
include_directories(${INCLUDE_BASE}/rolex_libs/deps)

#Source file define
set(COMMON_SRC ${PROJECT_SOURCE_DIR}/src)

#Used by both server and clients
file(GLOB_RECURSE COMMON_FILE ${COMMON_SRC}/*.cpp)
add_library(ROLEX_dev STATIC ${COMMON_FILE} ${INCLUDE_BASE}/rolex_libs/deps/r2/src/logging.cc)
link_libraries(ROLEX_dev)

#Test codes
file(GLOB TEST_SRC ${PROJECT_SOURCE_DIR}/test/*.cpp)
foreach (TEST ${TEST_SRC})
    get_filename_component(TEST_NAME ${TEST} NAME_WE)
    add_executable(${TEST_NAME} ${TEST})
    target_link_libraries(${TEST_NAME} ${LINKS_FLAGS})
endforeach()

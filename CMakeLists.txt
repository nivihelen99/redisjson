cmake_minimum_required(VERSION 3.16)
project(RedisJSONPlusPlus VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# Add source files
file(GLOB_RECURSE LIB_SOURCES "src/*.cpp")

# Add library
add_library(redisjson++ ${LIB_SOURCES})
target_include_directories(redisjson++ PUBLIC include)

# nlohmann_json is now vendored in thirdparty/
# Add thirdparty to include directories for redisjson++ target
target_include_directories(redisjson++ PUBLIC thirdparty)
# For consumers of redisjson++, if they need to see nlohmann/json.hpp via redisjson++'s headers:
target_include_directories(redisjson++ INTERFACE thirdparty)


# --- Use system-installed hiredis via pkg-config ---
# Remove FetchContent for hiredis
# include(FetchContent)
# FetchContent_Declare(
#   hiredis
#   GIT_REPOSITORY https://github.com/redis/hiredis.git
#   GIT_TAG v1.2.0 # Or any recent stable version
# )
# set(BUILD_TESTING OFF CACHE BOOL "Disable building tests for dependencies" FORCE) # Not needed if not using FetchContent for deps
# set(HIREDIS_TESTS OFF CACHE BOOL "Disable Hiredis specific tests" FORCE) # Not needed
# FetchContent_MakeAvailable(hiredis)

# Find hiredis using pkg-config
find_package(PkgConfig REQUIRED)
pkg_check_modules(HIREDIS REQUIRED hiredis)

if(HIREDIS_FOUND)
    message(STATUS "Found system hiredis:")
    message(STATUS "  HIREDIS_INCLUDE_DIRS: ${HIREDIS_INCLUDE_DIRS}")
    message(STATUS "  HIREDIS_LIBRARIES: ${HIREDIS_LIBRARIES}")
    message(STATUS "  HIREDIS_CFLAGS: ${HIREDIS_CFLAGS}") # Other flags like definitions
    target_include_directories(redisjson++ PUBLIC ${HIREDIS_INCLUDE_DIRS})
    target_link_libraries(redisjson++ PUBLIC ${HIREDIS_LIBRARIES})
    # If HIREDIS_CFLAGS contains definitions (e.g., -D flags), they might need to be added:
    # string(REGEX MATCHALL "-D[^ ]+" HIREDIS_DEFINITIONS "${HIREDIS_CFLAGS}")
    # target_compile_definitions(redisjson++ PUBLIC ${HIREDIS_DEFINITIONS})
    # Or more simply, if HIREDIS_CFLAGS are suitable for target_compile_options:
    # target_compile_options(redisjson++ PUBLIC ${HIREDIS_CFLAGS}) # Be careful with this, might add non-portable flags
else()
    message(FATAL_ERROR "System hiredis not found via pkg-config. Please install hiredis (e.g., libhiredis-dev) or ensure it's in PKG_CONFIG_PATH.")
endif()
# --- End of hiredis changes ---


# Enable testing with GoogleTest
enable_testing()
include(FetchContent)
FetchContent_Declare(
  googletest
  GIT_REPOSITORY https://github.com/google/googletest.git
  GIT_TAG release-1.12.1 # Or a more recent stable tag
)
# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

# Test files
file(GLOB_RECURSE TEST_SOURCES "tests/*.cpp")

add_executable(unit_tests ${TEST_SOURCES})
target_link_libraries(unit_tests PRIVATE redisjson++ GTest::gtest_main GTest::gmock) # Modern GTest targets

# Add test to CTest
include(GoogleTest)
gtest_discover_tests(unit_tests)

# --- Examples ---
# Non-SWSS Sample Program (formerly redisjson_sample_program)
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/examples/sample.cpp")
    add_executable(redisjson_sample_non_swss examples/sample.cpp)
    target_link_libraries(redisjson_sample_non_swss PRIVATE redisjson++)
    message(STATUS "Added example program target: redisjson_sample_non_swss (Non-SWSS mode)")
else()
    message(STATUS "examples/sample.cpp not found, redisjson_sample_non_swss target not created.")
endif()

# SWSS Sample Program
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/examples/sample_swss.cpp")
    add_executable(redisjson_sample_swss examples/sample_swss.cpp)
    target_link_libraries(redisjson_sample_swss PRIVATE redisjson++)
    message(STATUS "Added example program target: redisjson_sample_swss (SWSS mode)")
else()
    message(STATUS "examples/sample_swss.cpp not found, redisjson_sample_swss target not created.")
endif()

# Installation (optional)
# install(TARGETS redisjson++ DESTINATION lib)
# install(DIRECTORY include/ DESTINATION include)
# For system dependencies like hiredis, you typically wouldn't reinstall them with your project.
# Consumers of your installed redisjson++ would need to find hiredis on their system too.
# This is often handled by exporting your project's dependencies in a CMake config file.
# (Advanced topic, not covered by this simple change)

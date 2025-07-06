cmake_minimum_required(VERSION 3.16)
project(RedisJSONPlusPlus VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# Add include directory
# include_directories(include) # target_include_directories is preferred

# Add source files
file(GLOB_RECURSE LIB_SOURCES "src/*.cpp")

# Add library
add_library(redisjson++ ${LIB_SOURCES})
target_include_directories(redisjson++ PUBLIC include)

# Required dependencies
# For hiredis, you'd typically find_package and link.
# Using FetchContent for hiredis for self-contained example, if system packages are not assumed.

include(FetchContent)

# nlohmann_json is now vendored in thirdparty/
# Add thirdparty to include directories for redisjson++ target
target_include_directories(redisjson++ PUBLIC thirdparty)
# For consumers of redisjson++, if they need to see nlohmann/json.hpp via redisjson++'s headers:
target_include_directories(redisjson++ INTERFACE thirdparty)


# hiredis
FetchContent_Declare(
  hiredis
  GIT_REPOSITORY https://github.com/redis/hiredis.git
  GIT_TAG v1.2.0 # Or any recent stable version
)

# Disable tests for hiredis when using FetchContent
set(BUILD_TESTING OFF CACHE BOOL "Disable building tests for dependencies" FORCE)
set(HIREDIS_TESTS OFF CACHE BOOL "Disable Hiredis specific tests" FORCE) # Attempt Hiredis specific flag too

FetchContent_MakeAvailable(hiredis)

# Restore BUILD_TESTING if it was previously set by the user for the main project
# This is a bit tricky as we forced it. A better way is to use fetchcontent options if available.
# For now, assume the main project testing is enabled by enable_testing() later.
# A cleaner way if FetchContent_MakeAvailable supported it directly:
# FetchContent_MakeAvailable(hiredis BUILD_TESTING=OFF)

# hiredis builds a library, typically hiredis::hiredis or libhiredis.a/so
# It might need CMAKE_POSITION_INDEPENDENT_CODE for shared libs if redisjson++ is shared.
# Check if hiredis creates an imported target or if we need to add_library manually from its sources.
# Often, FetchContent_MakeAvailable will run hiredis's CMakeLists.txt which should create targets.
if(TARGET hiredis::hiredis)
    target_link_libraries(redisjson++ PRIVATE hiredis::hiredis)
else()
    # Fallback if hiredis::hiredis target isn't created (older hiredis or CMake setup)
    # This assumes hiredis source is in ${hiredis_SOURCE_DIR} and builds hiredis_static or hiredis_shared
    # This part might need adjustment based on how hiredis's CMake actually works.
    # For simplicity, assuming hiredis::hiredis target is available.
    message(WARNING "hiredis::hiredis target not found after FetchContent. Linking might fail.")
    # As a last resort, try to link directly if lib name is known (e.g. hiredis)
    # target_link_libraries(redisjson++ PRIVATE hiredis)
endif()


# Enable testing with GoogleTest
enable_testing()
find_package(GTest CONFIG REQUIRED) # Modern CMake way to find GTest

# Test files
file(GLOB_RECURSE TEST_SOURCES "tests/*.cpp")

add_executable(unit_tests ${TEST_SOURCES})
target_link_libraries(unit_tests PRIVATE redisjson++ GTest::gtest_main GTest::gmock)
# For older GTest, might be GTest::GTest GTest::Main

# Add test to CTest
include(GoogleTest)
gtest_discover_tests(unit_tests)

# --- Examples ---
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/examples/sample.cpp")
    add_executable(redisjson_sample_program examples/sample.cpp)
    target_link_libraries(redisjson_sample_program PRIVATE redisjson++)
    # Dependencies like hiredis and nlohmann_json should be linked transitively by redisjson++
    # if redisjson++ itself is properly configured to export its dependencies or link them publicly.
    # If redisjson++ links hiredis and nlohmann_json as PRIVATE, then the example program
    # might also need to link to them directly if it uses their headers directly,
    # or if the compiler/linker needs them explicitly for redisjson++'s public headers.
    # Assuming redisjson++ handles its dependencies' include directories for its public headers.
    # If explicit linking for dependencies is needed for the example:
    # target_link_libraries(redisjson_sample_program PRIVATE hiredis::hiredis)
    # if(TARGET nlohmann_json::nlohmann_json) # nlohmann is often header-only
    #    target_link_libraries(redisjson_sample_program PRIVATE nlohmann_json::nlohmann_json)
    # endif()
    message(STATUS "Added example program target: redisjson_sample_program")
else()
    message(STATUS "examples/sample.cpp not found, redisjson_sample_program target not created.")
endif()


# Installation (optional)
# install(TARGETS redisjson++ DESTINATION lib)
# install(DIRECTORY include/ DESTINATION include)
# Installation for FetchContent dependencies might also be needed if they are not header-only
# and not built as static part of redisjson++
# install(TARGETS nlohmann_json::nlohmann_json DESTINATION lib) # If it's a target
# install(TARGETS hiredis::hiredis DESTINATION lib)
# install(DIRECTORY include/ DESTINATION include)

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
# Assuming nlohmann_json is header-only and included via target_include_directories
# For hiredis, you'd typically find_package and link.
# Using FetchContent for both for self-contained example, if system packages are not assumed.

include(FetchContent)

# nlohmann_json (header only, but FetchContent can still be used to get it)
FetchContent_Declare(
  nlohmann_json
  GIT_REPOSITORY https://github.com/nlohmann/json.git
  GIT_TAG v3.11.3 # Or any recent stable version
)
FetchContent_MakeAvailable(nlohmann_json)
# For header-only, often just adding its include dir is enough if it's in a known path
# If nlohmann_json provides a target, use it. Otherwise, target_include_directories.
# Assuming nlohmann_json source dir is added to include path by FetchContent_MakeAvailable
# or it creates a target like nlohmann_json::nlohmann_json.
# For nlohmann_json, it's often used as just #include <nlohmann/json.hpp>
# target_include_directories(redisjson++ PRIVATE ${nlohmann_json_SOURCE_DIR}/include)
# If it exports a target:
# target_link_libraries(redisjson++ PRIVATE nlohmann_json::nlohmann_json) # if available
# For simplicity, ensure nlohmann/json.hpp is findable. Often put in 'vendor' or system include.
# For this example, we'll assume it's available in the include path for the library.
# The target_include_directories for redisjson++ itself should make its own headers available.
# If nlohmann_json is fetched, its include directory needs to be added to redisjson++
if(nlohmann_json_SOURCE_DIR)
    target_include_directories(redisjson++ PUBLIC ${nlohmann_json_SOURCE_DIR}/single_include) # For single header version
    target_include_directories(redisjson++ INTERFACE ${nlohmann_json_SOURCE_DIR}/single_include) # For consumers of redisjson++
else()
    # Fallback if FetchContent didn't define it, try find_package
    find_package(nlohmann_json 3.11.0) # QUIETLY?
    if(TARGET nlohmann_json::nlohmann_json)
        target_link_libraries(redisjson++ PRIVATE nlohmann_json::nlohmann_json)
    else()
        # Manual include or error
        message(WARNING "nlohmann_json not found via FetchContent or find_package. Ensure it's in include path.")
    endif()
endif()


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

# Installation (optional)
# install(TARGETS redisjson++ DESTINATION lib)
# install(DIRECTORY include/ DESTINATION include)
# Installation for FetchContent dependencies might also be needed if they are not header-only
# and not built as static part of redisjson++
# install(TARGETS nlohmann_json::nlohmann_json DESTINATION lib) # If it's a target
# install(TARGETS hiredis::hiredis DESTINATION lib)
# install(DIRECTORY include/ DESTINATION include)

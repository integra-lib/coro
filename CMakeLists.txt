cmake_minimum_required(VERSION 3.21)

project(
  hwlib_coro
  VERSION 0.1.0
  LANGUAGES CXX)

add_library(hwlib_coro INTERFACE)
add_library(Hwlib::coro ALIAS hwlib_coro)

target_include_directories(hwlib_coro
                           INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_compile_features(hwlib_coro INTERFACE cxx_std_20)

# Dependants read the version from this property rather than from a variable: a
# variable set here is invisible in a sibling directory, and a stale one left in
# the parent scope would be read instead — a version check that silently passes.
set_target_properties(hwlib_coro PROPERTIES HWLIB_VERSION ${PROJECT_VERSION})

if(PROJECT_IS_TOP_LEVEL)
  include(FetchContent)
  FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.15.2)
  FetchContent_MakeAvailable(googletest)

  include(GoogleTest)
  enable_testing()

  find_package(Threads REQUIRED)
  add_executable(
    coro-test
    tests/task_test.cpp
    tests/task_second_unit.cpp
    tests/sync_wait_test.cpp
    tests/when_all_ready_test.cpp
    tests/async_scope_test.cpp
    tests/async_generator_test.cpp
    tests/queues_test.cpp
    tests/io_service_test.cpp
    tests/readme_examples_test.cpp
    tests/thread_id.cpp)
  target_link_libraries(coro-test PRIVATE Hwlib::coro GTest::gtest_main
                                          Threads::Threads)
  gtest_discover_tests(coro-test)
endif()

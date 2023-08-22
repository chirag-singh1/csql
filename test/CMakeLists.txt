find_library (LIBPG_QUERY libpg_query.so)
find_library (LIBPG_QUERY libpg_query.so)

file(GLOB_RECURSE files
     "./*.h"
     "./*.cpp"
     "../lib/*.h"
     "../lib/*.cpp"
     "../src/*.h"
     "../src/*.cpp"
)
list(FILTER files EXCLUDE REGEX ".*/main.cpp")

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
add_executable (test ${files})
target_link_libraries(test ${LIBPG_QUERY})
target_link_libraries(test pthread)
target_link_libraries(test gtest gtest_main)
add_test( test test )
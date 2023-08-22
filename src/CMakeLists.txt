find_library (LIBPG_QUERY libpg_query.so)
file(GLOB_RECURSE files
     "./*.h"
     "./*.cpp"
     "../lib/*.h"
     "../lib/*.cpp"
)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
add_executable (csql ${files})
target_link_libraries(csql ${LIBPG_QUERY})
target_link_libraries(csql pthread)
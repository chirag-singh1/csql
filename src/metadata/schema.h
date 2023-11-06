#pragma once

#include <map>
#include <vector>

typedef int DataType;
# define NUM_SUPPORTED_DATATYPES 3
# define DATATYPES "int4", "string", "bool"

# define TYPE_COL_REF -1
# define TYPE_INT 0
# define TYPE_STRING 1
# define TYPE_BOOL 2

# define DT_INT4 "int4"
# define DT_STRING "string"
# define DT_BOOL "bool"

struct Schema {
    std::map<std::string, int> column_indices;
    std::vector<std::pair<std::string, DataType>> columns;
};

inline bool col_in_schema(std::string name, Schema* schema) {
    return schema->column_indices.find(name) != schema->column_indices.end();
};
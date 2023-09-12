#pragma once

#include "../util/log.h"

#include <string>
#include <unordered_map>
#include <map>
#include <vector>

typedef int DataType;
# define NUM_SUPPORTED_DATATYPES 3
# define DATATYPES "int4", "string", "bool"

# define TYPE_INT 0
# define TYPE_STRING 1
# define TYPE_BOOL 2

# define DT_INT4 "int4"
# define DT_STRING "string"
# define DT_BOOL "bool"

struct Schema {
    std::map<std::string, DataType> columns;
};

class Table {
    public:
        Table(std::string name, Schema* schema, int num_columns);
        ~Table();
        void print_schema();

    private:
        std::string name;
        Schema* schema;
        int num_columns;
};

class Database {
    public:
        Database(std::string name);
        ~Database();
        bool table_exists(std::string name);
        bool create_table(std::string name, std::vector<std::pair<std::string, std::string>> cols);

    private:
        std::string name;
        std::unordered_map<std::string, Table*> tables;
};

inline bool col_in_schema(std::string name, Schema* schema) {
    return schema->columns.find(name) != schema->columns.end();
}

inline Table* create_table_obj(std::string name, std::vector<std::pair<std::string, std::string>> cols) {
    Schema* s = new Schema;
    std::unordered_map<std::string, int> datatype_lookup;
    char* datatypes[] = { DATATYPES };
    for (int i = 0; i < NUM_SUPPORTED_DATATYPES; i++) {
        datatype_lookup[datatypes[i]] = i;
    }

    int i = 0;
    for (auto itr = cols.begin(); itr != cols.end(); itr++) {
        std::string col_name = itr->first;

        // Check for duplicate column names.
        if (col_in_schema(col_name, s)) {
            LOG_DEBUG("Duplicate column in schema", col_name);
            delete s;
            return nullptr;
        }

        // Resolve datatypes.
        if (datatype_lookup.find(itr->second) == datatype_lookup.end()) {
            LOG_DEBUG("Invalid datatype", itr->second);
            delete s;
            return nullptr;
        }
        DataType d = datatype_lookup.find(itr->second)->second;
        s->columns[col_name] = d;
        i++;
    }

    return new Table(name, s, i);
}
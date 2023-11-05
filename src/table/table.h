#pragma once

#include "../metadata/schema.h"
#include "../dataframe/in_memory_df.h"
#include "../util/log.h"

#include <string>
#include <unordered_map>
#include <vector>

class Schema;
class MetadataStore;

class Table {
    public:
        Table(std::string name, Schema* schema, int num_columns, MetadataStore* m);
        ~Table();
        void print_schema();
        Schema* get_schema();
        bool insert_data(InMemoryDF* data);
        bool load_from_disk();
        bool flush_to_disk();

        InMemoryDF* project_all();
        InMemoryDF* project_cols(std::vector<std::string> cols);

    private:
        void cleanup_data();

        MetadataStore* m;
        std::string name;
        Schema* schema;
        int num_columns;
        InMemoryDF* data;
        bool is_stale;
};

inline Table* create_table_obj(MetadataStore* m, std::string name, std::vector<std::pair<std::string, std::string>> cols) {
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
        s->columns.push_back(std::make_pair(col_name, d));
        s->column_indices[col_name] = s->columns.size();
        i++;
    }

    return new Table(name, s, i, m);
}
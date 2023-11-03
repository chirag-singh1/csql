#include "table.h"
#include "../metadata/metadata.h"

Table::Table(std::string name, Schema* schema, int num_columns, MetadataStore* m)
: name(name), schema(schema), num_columns(num_columns), is_stale(true), data(nullptr), m(m) {}

void Table::cleanup_data() {
    if (data != nullptr) {
        delete data;
    }
}

Table::~Table() {
    delete schema;
    cleanup_data();
}

void Table::print_schema() {
    char* datatypes[] = { DATATYPES };
    std::string output = "\n";
    for (auto itr = schema->columns.begin(); itr != schema->columns.end(); itr++) {
        output += "\t  | ";
        output += itr->first;
        output += "(";
        output += datatypes[itr->second];
        output += ")\n";
    }
    LOG_DEBUG("Table", name);
    LOG_DEBUG("Schema", output);
}

Schema* Table::get_schema() {
    return schema;
}

bool Table::insert_data(InMemoryDF* new_data) {
    // Update InMemoryDF.
    if (is_stale) {
        if (!load_from_disk()) {
            LOG_DEBUG_RAW("Load from disk failed");
        }
    }

    // Special case: first insertion, just copy data.
    if (data->get_num_records() == 0) {
        LOG_DEBUG_RAW("Previous DF empty, creating new DF");
        cleanup_data();
        data = new InMemoryDF(new_data);
        data->print();
        return flush_to_disk();
    }

    bool merge_success = data->merge_df(new_data);
    if (merge_success) {
        return flush_to_disk();
    }
    return false;
}

// Returns updated copy of current data.
InMemoryDF* Table::project_all() {
    if (is_stale) {
        load_from_disk();
    }
    return new InMemoryDF(data);
}

bool Table::load_from_disk() {
    data = new InMemoryDF(schema);
    return data->from_disk(m, name);
}
bool Table::flush_to_disk() {
    return data->to_disk(m, name);
}
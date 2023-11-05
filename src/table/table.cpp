#include "table.h"
#include "../util/file.h"
#include "../metadata/metadata.h"
#include "../metadata/metadata_store.h"

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

InMemoryDF* Table::project_cols(std::vector<std::string> cols) {
    if (is_stale) {
        load_from_disk();
    }

    std::vector<int> inds;
    LOG_DEBUG_VEC("Projecting columns", cols);
    for (int i = 0; i < cols.size(); i++) {
        inds.push_back(schema->column_indices[cols[i]]);
    }

    return new InMemoryDF(data, inds);
}

bool Table::load_from_disk() {
    data = new InMemoryDF(schema);
    return data->from_disk(m, name);
}
bool Table::flush_to_disk() {
    return data->to_disk(m, name);
}

bool Table::delete_data() {
    LOG_DEBUG("Deleting data at", m->get_table_filepath(name));
    DELETE_TABLE_DATA(m->get_table_filepath(name));
    LOG_DEBUG_RAW("Data deleted");
}
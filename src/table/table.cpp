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
        cleanup_data();
        data = new InMemoryDF(new_data);
        data->print();
        flush_to_disk();
    }

    // TODO: normal case.
}
bool Table::load_from_disk() {
    data = new InMemoryDF(schema);
    return true;
}
bool Table::flush_to_disk() {
    return data->to_disk(m, name);
}
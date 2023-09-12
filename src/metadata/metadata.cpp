#include "metadata.h"
#include "../util/log.h"

#include <string>
#include <unordered_map>

Table::Table(std::string name, Schema* schema, int num_columns)
: name(name), schema(schema), num_columns(num_columns) {}

Table::~Table() {
    delete schema;
}

void Table::print_schema() {
    char* datatypes[] = { DATATYPES };
    std::string output = "\n";
    for (auto itr = schema->columns.begin(); itr != schema->columns.end(); itr++) {
        output += "\t  | ";
        output += itr->first;
        output += "(";
        LOG_DEBUG("dtype", itr->second);
        output += datatypes[itr->second];
        output += ")\n";
    }
    LOG_DEBUG("Table", name);
    LOG_DEBUG("Schema", output);
}

Database::Database(std::string name): name(name) {}

Database::~Database() {
    for (auto itr = tables.begin(); itr != tables.end(); itr++) {
        delete itr->second;
    }
}

bool Database::table_exists(std::string name) {
    return tables.find(name) != tables.end();
}

bool Database::create_table(std::string name, std::vector<std::pair<std::string, std::string>> cols) {
    if (table_exists(name)) {
        LOG_DEBUG("Table exists", name);
        return false;
    }

    // Will fail on invalid schema (ex. bad datatype, duplicate cols, etc.).
    Table* new_table = create_table_obj(name, cols);
    if (new_table != nullptr) {
        LOG_DEBUG("Table created", name);
        new_table->print_schema();
    }
    return new_table != nullptr;
}
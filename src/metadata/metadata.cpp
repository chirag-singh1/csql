#include "metadata.h"
#include "../util/log.h"
#include "../../lib/rapidjson/document.h"
#include "../table/table.h"

#include <string>
#include <unordered_map>

Database::Database(std::string name): name(name) {}

Database::~Database() {
    for (auto itr = tables.begin(); itr != tables.end(); itr++) {
        delete itr->second;
    }
}

bool Database::table_exists(std::string name) {
    return tables.find(name) != tables.end();
}

void Database::attach_table(std::string name, Table* t) {
    tables[name] = t;
}

bool Database::create_table(MetadataStore* m, std::string name, std::vector<std::pair<std::string, std::string>> cols) {
    if (table_exists(name)) {
        LOG_DEBUG("Table exists", name);
        return false;
    }

    // Will fail on invalid schema (ex. bad datatype, duplicate cols, etc.).
    Table* new_table = create_table_obj(m, name, cols);
    if (new_table != nullptr) {
        LOG_DEBUG("Table created", name);
        new_table->print_schema();
        tables[name] = new_table;
    }
    return new_table != nullptr;
}

bool Database::drop_table(MetadataStore* m, std::string name) {
    if (!table_exists(name)) {
        LOG_DEBUG("No table", name);
        return false;
    }
    delete tables[name];
    tables.erase(tables.find(name));
    return true;
}

Table* Database::get_table(std::string name) {
    if (table_exists(name)) {
        return tables[name];
    }
    return nullptr;
}

std::unordered_map<std::string, Table*>* Database::get_tables() {
    return &tables;
}
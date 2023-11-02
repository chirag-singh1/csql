#include "metadata_store.h"
#include "../constants.h"
#include "../util/file.h"
#include "../util/log.h"
#include "../../lib/rapidjson/document.h"
#include "../analyzer/operations.h"
#include "../table/table.h"

#include <string>
#include <unordered_map>

MetadataStore::MetadataStore() {
    skip_persist_metadata = true;
    read_json_from_file(metastore_filepath());
    const rapidjson::Value& metadata = _out;

    active_db = DEFAULT_DB_NAME;
    LOG_DEBUG_RAW("Loading metadata");
    for (auto& db : _out.GetObject()) {
        LOG_DEBUG_RAW("Loading new db");
        std::string db_name = std::string(db.name.GetString());
        databases[db_name] = new Database(db_name);
        for (auto& tbl : db.value.GetObject()) {
            std::string tbl_name = std::string(tbl.name.GetString());
            Schema* s = new Schema;
            for (auto& col : tbl.value.GetArray()) {
                std::string col_name = col.FindMember(OPT_COL_NAMES)->value.GetString();
                s->columns.push_back(std::make_pair(col_name,
                    col.FindMember(OPT_COL_TYPES)->value.GetInt()));
                s->column_indices[col_name] = s->columns.size();
            }
            Table* t = new Table(tbl_name, s, tbl.value.Size(), this);
            databases.find(db_name)->second->attach_table(tbl_name, t);
        }
    }
    skip_persist_metadata = false;
}

MetadataStore::~MetadataStore() {
    for (auto itr = databases.begin(); itr != databases.end(); itr++) {
        delete itr->second;
    }
}

bool MetadataStore::create_db(std::string name) {
    LOG_DEBUG("Creating database", name);
    if (db_exists(name)) {
        LOG_DEBUG("Found database", name);
        return false;
    }

    databases[name] = new Database(name);
    LOG_DEBUG("Database created", name);
    persist_metadata();
    return true;
}

bool MetadataStore::drop_db(std::string name, bool missing_ok) {
    LOG_DEBUG("Dropping database", name);
    // Can't drop default DB.
    if (name == DEFAULT_DB_NAME) {
        LOG_DEBUG("Can't delete default DB", name);
        return false;
    }

    if (!db_exists(name)) {
        LOG_DEBUG("No database", name);
        return !missing_ok;
    }

    databases.erase(databases.find(name));
    persist_metadata();
    LOG_DEBUG("Database dropped", name);

    // Reset active DB name if active DB dropped.
    if (active_db == name) {
        active_db = DEFAULT_DB_NAME;
    }

    return true;
}

bool MetadataStore::use_db(std::string name) {
    LOG_DEBUG("Using database", name);
    if(!db_exists(name)) {
        LOG_DEBUG("No database", name);
        return false;
    }

    active_db = name;
    LOG_DEBUG("Using database", name);
    return true;
}

bool MetadataStore::db_exists(std::string name) {
    return databases.find(name) != databases.end();
}

bool MetadataStore::create_table(std::string name, std::vector<std::pair<std::string, std::string>> cols) {
    LOG_DEBUG("Creating table", name);
    Database* curr_db = databases.find(active_db)->second;
    bool res = curr_db->create_table(this, name, cols);
    if (res) {
        persist_metadata();
    }
    return res;
}

Table* MetadataStore::get_table(std::string name) {
    Database* curr_db = databases.find(active_db)->second;
    return curr_db->get_table(name);
}

std::string MetadataStore::get_table_filepath(std::string name) {
    if (databases.find(active_db)->second->table_exists(name)) {
        return DEFAULT_FILEPATH + std::string("/") + active_db + std::string("/") + name;
    }
    return "";
}

void MetadataStore::persist_metadata() {
    if (skip_persist_metadata) {
        LOG_DEBUG_RAW("Skipping metadata persist");
    }
    LOG_DEBUG_RAW("Saving metadata");
    rapidjson::Document d;
    d.SetObject();
    for (auto itr = databases.begin(); itr != databases.end(); itr++) {
        rapidjson::Value db;
        db.SetObject();
        std::unordered_map<std::string, Table*>* tables = itr->second->get_tables();
        for (auto tbl_itr = tables->begin(); tbl_itr != tables->end(); tbl_itr++) {
            rapidjson::Value t;
            t.SetArray();
            Schema* schema = tbl_itr->second->get_schema();
            for (auto s_itr = schema->columns.begin(); s_itr != schema->columns.end(); s_itr++) {
                rapidjson::Value col;
                col.SetObject();
                rapidjson::Value col_name(s_itr->first.c_str(), strlen(s_itr->first.c_str()), d.GetAllocator());
                col.AddMember(OPT_COL_NAMES, col_name, d.GetAllocator());
                col.AddMember(OPT_COL_TYPES, s_itr->second, d.GetAllocator());
                t.PushBack(col, d.GetAllocator());
            }

            rapidjson::Value table_name(tbl_itr->first.c_str(), strlen(tbl_itr->first.c_str()), d.GetAllocator());
            db.AddMember(table_name, t, d.GetAllocator());
        }

        rapidjson::Value db_name(itr->first.c_str(), strlen(itr->first.c_str()), d.GetAllocator());
        d.AddMember(db_name, db, d.GetAllocator());
    }

    JSON_LOG_DEBUG("Metadata", &d);
    write_json_to_file(&d, metastore_filepath());
    LOG_DEBUG_RAW("Metadata saved");
}
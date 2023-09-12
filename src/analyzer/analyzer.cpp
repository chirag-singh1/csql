#include "analyzer.h"
#include "../util/log.h"

#include <iostream>
#include <vector>

using std::string;
using std::vector;

Analyzer::Analyzer() {
    char* op_names[] = { OPERATIONS };
    for (int i = 0; i < NUM_SUPPORTED_OPERATIONS; i++) {
        operation_lookup[op_names[i]] = i;
    }
}

OperationNode* Analyzer::query_to_node(char* parsed_query) {
    LOG_DEBUG("Analyzing query", parsed_query);
    // Parse query as JSON.
    rapidjson::Document d;
    d.Parse(parsed_query);
    const rapidjson::Value& statements = d["stmts"];

    OperationNode* output_node =
        new OperationNode(OP_NONE, statements.Size(), "Root");
    int i = 0;
    for (rapidjson::Value::ConstValueIterator itr = statements.Begin(); itr != statements.End(); ++itr) {
        const rapidjson::Value& statement = *itr;
        output_node->set_child(query_to_node_internal(&statement), i);
        i++;
    }
    output_node->print(0);

    return output_node;
}

OperationNode* Analyzer::query_to_node_internal(const rapidjson::Value* query) {
    JSON_LOG_DEBUG("Analyzing statement", query);

    // If not a recursive call, extract the statement.
    const rapidjson::Value& stmt_obj =
        query->FindMember("stmt")->value.IsObject() ? query->FindMember("stmt")->value.GetObject() : query->GetObject();
    const string op_name = string(stmt_obj.MemberBegin()->name.GetString());
    const rapidjson::Value& options = stmt_obj.MemberBegin()->value;
    if (operation_lookup.find(op_name) != operation_lookup.end()) {
        OperationNode* o
            = new OperationNode(operation_lookup.at(op_name), 0, op_name);

        // Set operation options.
        if (operation_lookup.at(op_name) == OP_CREATE_DB) { // CREATE DATABASE
            // Set database name.
            o->set_string_option(OPT_DB_NAME, options.FindMember(OPT_DB_NAME)->value.GetString());
        }
        else if (operation_lookup.at(op_name) == OP_DROP_DB) { // DROP DATABASE
            // Set database name, and IF EXISTS.
            o->set_string_option(OPT_DB_NAME, options.FindMember(OPT_DB_NAME)->value.GetString());
            o->set_bool_option(OPT_DB_MISSING_OK, options.FindMember(OPT_DB_MISSING_OK)->value.GetBool());
        }
        else if (operation_lookup.at(op_name) == OP_CREATE) { // CREATE TABLE
            // Set table name and number of columns;
            o->set_string_option(OPT_TABLE_NAME, options.FindMember(OPT_RELATION)->value.FindMember(OPT_TABLE_NAME)->value.GetString());
            o->set_int_option(OPT_NUM_COLUMNS, options.FindMember(OPT_TABLE_DESC)->value.Size());
            const rapidjson::Value& table_info = options.FindMember(OPT_TABLE_DESC)->value;

            // Get table schema.
            int i = 0;
            for (rapidjson::Value::ConstValueIterator itr = table_info.Begin(); itr != table_info.End(); ++itr) {
                const rapidjson::Value& column = (*itr).FindMember(OPT_COL_DEF)->value;
                // Set column name.
                o->set_string_option(OPT_COL_NAME(i), column.FindMember(OPT_COL_NAMES)->value.GetString());

                // Set column type.
                const rapidjson::Value& typeArr = column.FindMember(OPT_COL_TYPES)->value.FindMember(OPT_TYPE_NAMES)->value.GetArray();
                for (rapidjson::Value::ConstValueIterator names = typeArr.Begin(); names != typeArr.End(); ++names) {
                    std::string name = names->FindMember(OPT_STRING)->value.FindMember("sval")->value.GetString();
                    // Check coltype isn't pg_catalog.
                    if (name != OPT_TYPE_NAME_INVALID) {
                        o->set_string_option(OPT_COL_TYPE(i), name);
                    }
                }
                i++;
            }
        }
        else if (operation_lookup.at(op_name) == OP_INSERT) {
            o->set_string_option(OPT_TABLE_NAME, options.FindMember(OPT_RELATION)->value.FindMember(OPT_TABLE_NAME)->value.GetString());
            o->set_num_children(1);
            o->set_child(query_to_node_internal(&options.FindMember(OPT_SELECT_0)->value), 0);
        }
        else if (operation_lookup.at(op_name) == OP_SELECT) {
            const rapidjson::Value& sel_op = options.FindMember(OPT_SELECT_1)->value;
            // Select statement comes from list of values.
            if (sel_op.HasMember(OPT_VALUE_LIST)) {
                // Currently, only gets the first list from valuesList.
                assert(sel_op.FindMember(OPT_VALUE_LIST)->value.GetArray().Size() == 1);
                const rapidjson::Value& vals =
                    (*sel_op.FindMember(OPT_VALUE_LIST)->value.GetArray().Begin()).FindMember(OPT_LIST)->value.FindMember(OPT_ITEMS)->value;
                int i = 0;
                for (auto itr = vals.Begin(); itr != vals.End(); ++itr) {
                    const rapidjson::Value& val = itr->FindMember(OPT_CONST)->value;
                    if (val.HasMember(OPT_INT_VAL)) {
                       o->set_string_option(OPT_CONST_TYPE(i), OPT_INT_VAL);
                       o->set_int_option(OPT_CONST_VAL(i), val.FindMember(OPT_INT_VAL)->value.FindMember(OPT_INT_VAL)->value.GetInt());
                    }
                    else if (val.HasMember(OPT_STR_VAL)) {
                        o->set_string_option(OPT_CONST_TYPE(i), OPT_STR_VAL);
                        o->set_string_option(OPT_CONST_VAL(i), val.FindMember(OPT_STR_VAL)->value.FindMember(OPT_STR_VAL)->value.GetString());
                    }
                    else if (val.HasMember(OPT_BOOL_VAL)) {
                        o->set_string_option(OPT_CONST_TYPE(i), OPT_BOOL_VAL);
                        o->set_bool_option(OPT_CONST_VAL(i), val.FindMember(OPT_BOOL_VAL)->value.FindMember(OPT_BOOL_VAL)->value.GetBool());
                    }
                    else {
                        JSON_LOG_DEBUG("Unsupported type found in", &val);
                        assert(false); // TODO: better error handling for unsupported type.
                    }
                    i++;
                }
            }
        }

        return o;
    }
    else {
        return new OperationNode(OP_UNSUPPORTED, 0, "Unsupported");
    }

    return nullptr;
}
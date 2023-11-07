#include "analyzer.h"
#include "../util/log.h"
#include "../metadata/schema.h"
#include "../filter/filter.h"

#include <iostream>
#include <vector>

using std::string;
using std::vector;

Analyzer::Analyzer() {
    char* op_names[] = { OPERATIONS };
    for (int i = 0; i < NUM_SUPPORTED_OPERATIONS; i++) {
        operation_lookup[op_names[i]] = i;
    }

    char* filter_ops[] = { FILTERS };
    for (int i = 0; i < NUM_SUPPORTED_FILTERS; i++) {
        filter_lookup[filter_ops[i]] = i;
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

void parse_col_ref(const rapidjson::Value& val, OperationNode* o, int ind) {
    const rapidjson::Value& target = val.FindMember(OPT_SELECT_COLREF)->
        value.GetObject().FindMember(OPT_SELECT_FIELDS)->value.GetArray().Begin()->GetObject();
    // Take only first from each field.
    // TODO: column support.
    if (target.HasMember(OPT_SELECT_ALL)) {
        o->set_string_option(OPT_SELECT_TARGET_REF(0), OPT_SELECT_ALL);
    }
    else if (target.HasMember(OPT_STRING)) {
        o->set_string_option(OPT_SELECT_TARGET_REF(ind), target.FindMember(OPT_STRING)->
            value.GetObject().FindMember(OPT_STR_VAL)->value.GetString());
    }
}

void parse_const_expr(const rapidjson::Value& val, OperationNode* o, std::string type_key, std::string key) {
    if (val.HasMember(OPT_INT_VAL)) {
        o->set_int_option(type_key, TYPE_INT);
        o->set_int_option(key, val.FindMember(OPT_INT_VAL)->value.FindMember(OPT_INT_VAL)->value.GetInt());
    }
    else if (val.HasMember(OPT_STR_VAL)) {
        o->set_int_option(type_key, TYPE_STRING);
        o->set_string_option(key, val.FindMember(OPT_STR_VAL)->value.FindMember(OPT_STR_VAL)->value.GetString());
    }
    else if (val.HasMember(OPT_BOOL_VAL)) {
        o->set_int_option(type_key, TYPE_BOOL);
        o->set_bool_option(key, val.FindMember(OPT_BOOL_VAL)->value.HasMember(OPT_BOOL_VAL));
    }
    else {
        JSON_LOG_DEBUG("Unsupported type found in", &val);
        assert(false); // TODO: better error handling for unsupported type.
    }
}

void Analyzer::parse_filter_expr(const rapidjson::Value& val, bool is_left, OperationNode* n, int level) {
    JSON_LOG_DEBUG("Parsing filter expression", &val);
    if (val.HasMember(OPT_SELECT_COLREF)) {
        n->set_int_option(OPT_FILTER_EXPR_TYPE(level, is_left), TYPE_COL_REF);
        n->set_string_option(OPT_FILTER_EXPR_VAL(level, is_left), val.FindMember(OPT_SELECT_COLREF)->
        value.GetObject().FindMember(OPT_SELECT_FIELDS)->value.GetArray().Begin()->GetObject().FindMember(OPT_STRING)->
            value.GetObject().FindMember(OPT_STR_VAL)->value.GetString());
    }
    else {
        parse_const_expr(val.FindMember(OPT_CONST)->value, n, OPT_FILTER_EXPR_TYPE(level, is_left), OPT_FILTER_EXPR_VAL(level, is_left));
    }
}

OperationNode* Analyzer::parse_filter(const rapidjson::Value* filter) {
    OperationNode* filter_node = new OperationNode(OP_FILTER, 0, "Filter");
    JSON_LOG_DEBUG("Parsing filter", filter);
    if (filter->HasMember(OPT_EXPR)) {
        std::string op = filter->FindMember(OPT_EXPR)->
            value.FindMember(OPT_NAME)->value.GetArray().Begin()->
            FindMember(OPT_STRING)->value.GetObject().FindMember(OPT_STR_VAL)->value.GetString();
        parse_filter_expr(filter->FindMember(OPT_EXPR)->
            value.FindMember(OPT_LEXPR)->value, true, filter_node, 0);
        parse_filter_expr(filter->FindMember(OPT_EXPR)->
            value.FindMember(OPT_REXPR)->value, false, filter_node, 0);
        filter_node->set_bool_option(OPT_SIMPLE_EXPR, true);
        filter_node->set_int_option(OPT_FILTER_EXPR(0), filter_lookup[op]);
    }
    else {
        // TODO: handle complex boolean predicates.
    }

    return filter_node;
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
            o->set_bool_option(OPT_DB_MISSING_OK, options.FindMember(OPT_DB_MISSING_OK)->value.IsBool()
                && options.FindMember(OPT_DB_MISSING_OK)->value.GetBool());
        }
        else if (operation_lookup.at(op_name) == OP_DROP_TBL) { // DROP TABLE
            // TODO: support more than one drop at a time.
            o->set_string_option(OPT_TABLE_NAME, options.FindMember(OPT_OBJECTS)->value.Begin()->
                FindMember(OPT_LIST)->value.FindMember(OPT_ITEMS)->value.Begin()->FindMember(OPT_STRING)->
                value.FindMember(OPT_STR_VAL)->value.GetString());
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
            // WHERE clause on the SELECT. Pushdown the filter.
            o->set_bool_option(OPT_USE_IN_MEMORY, sel_op.HasMember(OPT_FILTER));
            if (sel_op.HasMember(OPT_FILTER)) {
                o->set_num_children(1);
                o->set_child(parse_filter(&sel_op.FindMember(OPT_FILTER)->value), 0);
            }
            // Select statement comes from list of values.
            if (sel_op.HasMember(OPT_VALUE_LIST)) {
                // Currently, only gets the first list from valuesList.
                o->set_string_option(OPT_SELECT_TARGET, OPT_VALUE_LIST);
                assert(sel_op.FindMember(OPT_VALUE_LIST)->value.GetArray().Size() == 1);
                const rapidjson::Value& vals =
                    (*sel_op.FindMember(OPT_VALUE_LIST)->value.GetArray().Begin()).FindMember(OPT_LIST)->value.FindMember(OPT_ITEMS)->value;
                o->set_int_option(OPT_NUM_VALUES, vals.Size());
                int i = 0;
                for (auto itr = vals.Begin(); itr != vals.End(); ++itr) {
                    const rapidjson::Value& val = itr->FindMember(OPT_CONST)->value;
                    JSON_LOG_DEBUG("text", &val);
                    parse_const_expr(val, o, OPT_CONST_TYPE(i), OPT_CONST_VAL(i));
                    i++;
                }
            }
            // Select statement comes from a FROM statement.
            // TODO: implement filtering, joins, and multiple tables in select.
            else if (sel_op.HasMember(OPT_FROM_CLAUSE)) {
                const rapidjson::Value& val = sel_op.FindMember(OPT_FROM_CLAUSE)->value.GetArray().Begin()->GetObject();
                if (val.HasMember(OPT_RANGE_VAR)) {
                    std::string table_ref = val.FindMember(OPT_RANGE_VAR)->value.FindMember(OPT_TABLE_NAME)->value.GetString();
                    o->set_string_option(OPT_SELECT_TARGET, table_ref);
                    // Push table reference down to filter if applicable.
                    for (int i = 0; i < o->get_num_children(); i++) {
                        if (o->get_children()[i]->get_operation() == OP_FILTER) {
                            o->get_children()[i]->set_string_option(OPT_SELECT_TARGET, table_ref);
                        }
                    }
                }

                // Set targets.
                // TODO support individual columns and functions.
                const rapidjson::Value& targets = sel_op.FindMember(OPT_SELECT_TARGETS)->value;
                int i = 0;
                for (auto itr = targets.Begin(); itr != targets.End(); ++itr) {
                    parse_col_ref(itr->FindMember(OPT_SELECT_RESTARGET)->value.GetObject().FindMember(OPT_VAL)->value.GetObject(), o, i);
                    i++;
                }
                o->set_int_option(OPT_SELECT_NUM_TARGETS, i);
            }
        }

        return o;
    }
    else {
        return new OperationNode(OP_UNSUPPORTED, 0, "Unsupported");
    }

    return nullptr;
}
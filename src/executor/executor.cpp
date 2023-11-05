#include "executor.h"
#include "../analyzer/operations.h"
#include "../dataframe/in_memory_df.h"
#include "../metadata/metadata_store.h"
#include "../metadata/metadata.h"
#include "../util/log.h"
#include "../table/table.h"

#include <cstring>
#include <string>

Executor::Executor(OperationNode* operation_tree, MetadataStore* metadata_store)
: root(operation_tree), metadata_store(metadata_store), curr_result(nullptr) {}

Executor::~Executor() {
    if (curr_result != nullptr) {
        delete curr_result;
    }
}

void Executor::execute() {
    LOG_DEBUG("Executor created, num ops", root->get_num_children());
    for (int i = 0; i < root->get_num_children(); i++) {
        execute_internal(root->get_children()[i]);
    }
    LOG_DEBUG("Execution complete, num ops", root->get_num_children());
}

void Executor::execute_internal(OperationNode* curr_op) {
    if (curr_op->get_operation() == OP_CREATE_DB) {
        LOG_DEBUG("CREATE DATABASE", curr_op->get_string_option(OPT_DB_NAME));
        metadata_store->create_db(curr_op->get_string_option(OPT_DB_NAME));
    }
    else if (curr_op->get_operation() == OP_DROP_DB) {
        LOG_DEBUG("DROP DATABASE", curr_op->get_string_option(OPT_DB_NAME));
        metadata_store->drop_db(curr_op->get_string_option(OPT_DB_NAME), curr_op->get_bool_option(OPT_DB_MISSING_OK));
    }
    else if (curr_op->get_operation() == OP_CREATE) {
        LOG_DEBUG("CREATE TABLE", curr_op->get_string_option(OPT_TABLE_NAME));
        std::vector<std::pair<std::string, std::string>> cols;
        for (int i = 0; i < curr_op->get_int_option(OPT_NUM_COLUMNS); i++) {
            cols.push_back(std::make_pair<std::string, std::string>(
                curr_op->get_string_option(OPT_COL_NAME(i)),
                curr_op->get_string_option(OPT_COL_TYPE(i))));
        }

        metadata_store->create_table(curr_op->get_string_option(OPT_TABLE_NAME), cols);
    }
    else if (curr_op->get_operation() == OP_DROP_TBL) {
        LOG_DEBUG("DROP TABLE", curr_op->get_string_option(OPT_TABLE_NAME));
        Table* table_to_delete = metadata_store->get_table(curr_op->get_string_option(OPT_TABLE_NAME));
        if (table_to_delete != nullptr) {
            table_to_delete->delete_data();
            metadata_store->drop_table(curr_op->get_string_option(OPT_TABLE_NAME));
        }
        else {
            LOG_DEBUG("Table does not exist", curr_op->get_string_option(OPT_TABLE_NAME));
        }
    }
    else if (curr_op->get_operation() == OP_INSERT) {
        LOG_DEBUG("INSERT", curr_op->get_string_option(OPT_TABLE_NAME));
        assert(curr_op->get_num_children() == 1); // Expect one child.
        execute_internal(curr_op->get_children()[0]); // Execute SELECT to get result.
        curr_result->print();
        Table* table_to_insert = metadata_store->get_table(curr_op->get_string_option(OPT_TABLE_NAME));
        if (table_to_insert != nullptr) {
            table_to_insert->insert_data(curr_result);
        }
        else {
            LOG_DEBUG("Table does not exist", curr_op->get_string_option(OPT_TABLE_NAME));
        }
    }
    else if (curr_op->get_operation() == OP_SELECT) {
        LOG_DEBUG("SELECT", curr_op->get_string_option(OPT_SELECT_TARGET));
        if (curr_op->get_string_option(OPT_SELECT_TARGET) == OPT_VALUE_LIST) {
            int num_values = curr_op->get_int_option(OPT_NUM_VALUES);
            std::vector<DataType> sel_types;
            for (int i = 0; i < num_values; i++) {
                sel_types.push_back(curr_op->get_int_option(OPT_CONST_TYPE(i)));
            }

            curr_result = new InMemoryDF(sel_types);
            Record* r = curr_result->get_record();

            // No need for validation here, created record from value list.
            r->num_int = 0;
            r->num_bool = 0;
            r->num_str = 0;
            for (int i = 0; i < num_values; i++) {
                if (r->col_types[i] == TYPE_INT) {
                    r->int_vals[r->num_int] = curr_op->get_int_option(OPT_CONST_VAL(i));
                    r->num_int++;
                }
                else if (r->col_types[i] == TYPE_BOOL) {
                    r->bool_vals[r->num_bool] = curr_op->get_bool_option(OPT_CONST_VAL(i));
                    r->num_bool++;
                }
                else if (r->col_types[i] == TYPE_STRING) {
                    r->str_vals[r->num_str] = new char[1 + curr_op->get_string_option(OPT_CONST_VAL(i)).size()];
                    std::strcpy(r->str_vals[r->num_str], curr_op->get_string_option(OPT_CONST_VAL(i)).c_str());
                    r->num_str++;
                }
            }

            curr_result->insert_record(*r);
            destroy_record(r);
        }
        // Projection operation on InMemoryDF.
        else {
            Table* table_to_select = metadata_store->get_table(curr_op->get_string_option(OPT_SELECT_TARGET));
            if (table_to_select != nullptr) {
                // Special case: no-op (project all).
                if (curr_op->get_int_option(OPT_SELECT_NUM_TARGETS) == 1 && curr_op->get_string_option(OPT_SELECT_TARGET_REF(0)) == OPT_SELECT_ALL) {
                    curr_result = table_to_select->project_all();
                    curr_result->print();
                }
                else {
                    std::vector<std::string> projected_cols;
                    for (int i = 0; i < curr_op->get_int_option(OPT_SELECT_NUM_TARGETS); i++) {
                        projected_cols.push_back(curr_op->get_string_option(OPT_SELECT_TARGET_REF(i)));
                    }
                    curr_result = table_to_select->project_cols(projected_cols);
                    curr_result->print();
                }
            }
            else {
                LOG_DEBUG("Table does not exist", curr_op->get_string_option(OPT_SELECT_TARGET));
            }
        }
    }
}

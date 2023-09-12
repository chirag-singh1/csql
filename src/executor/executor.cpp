#include "executor.h"
#include "../analyzer/operations.h"
#include "../dataframe/in_memory_df.h"
#include "../metadata/metadata_store.h"
#include "../metadata/metadata.h"
#include "../util/log.h"

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
    else if (curr_op->get_operation() == OP_INSERT) {
        LOG_DEBUG("INSERT", curr_op->get_string_option(OPT_TABLE_NAME));
        assert(curr_op->get_num_children() == 1); // Expect one child.
        execute_internal(curr_op->get_children()[0]); // Execute SELECT to get result.

        curr_result->print();
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
    }
}

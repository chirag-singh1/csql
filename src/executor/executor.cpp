#include "executor.h"
#include "../analyzer/operations.h"
#include "../metadata/metadata_store.h"
#include "../metadata/metadata.h"
#include "../util/log.h"

#include <string>

Executor::Executor(OperationNode* operation_tree, MetadataStore* metadata_store)
: root(operation_tree), metadata_store(metadata_store) {}

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

    }
    else if (curr_op->get_operation() == OP_SELECT) {
    }
}

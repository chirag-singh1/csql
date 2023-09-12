#pragma once

#include "../analyzer/operations.h"

class MetadataStore;

class Executor {
    public:
        Executor(OperationNode* operation_tree, MetadataStore* metadata_store);
        void execute();
    private:
        void execute_internal(OperationNode* curr_op);
        OperationNode* root;
        MetadataStore* metadata_store;
};
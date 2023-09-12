#pragma once

#include "../analyzer/operations.h"

class InMemoryDF;
class MetadataStore;

class Executor {
    public:
        Executor(OperationNode* operation_tree, MetadataStore* metadata_store);
        ~Executor();
        void execute();
    private:
        void execute_internal(OperationNode* curr_op);
        OperationNode* root;
        MetadataStore* metadata_store;
        InMemoryDF* curr_result;
};
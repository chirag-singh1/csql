#pragma once

#include "../analyzer/operations.h"

class Executor {
    public:
        Executor(OperationNode* operation_tree);
        void execute();
};
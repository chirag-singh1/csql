# pragma once

#include "../../lib/rapidjson/document.h"

#include <string>

typedef int Operation;

class OperationNode {
    public:
        OperationNode(Operation op, int num_children, std::string name,
            const rapidjson::Value* options);
        ~OperationNode();
        void set_child(OperationNode* child, int child_ind);
        void print(int level);
        int get_num_children();
        OperationNode** get_children();
        Operation get_operation();
        const rapidjson::Value* get_options();

    private:
        int num_children;
        std::string name;
        Operation op;
        OperationNode** children;
        const rapidjson::Value* options;
};


# define NUM_SUPPORTED_OPERATIONS 2
# define OPERATIONS "CreateStmt", "CreatedbStmt"

# define OP_UNSUPPORTED -2
# define OP_NONE -1
# define OP_CREATE 0
# define OP_CREATE_DB 1
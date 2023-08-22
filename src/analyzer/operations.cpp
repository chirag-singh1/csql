#include "operations.h"

#include <iostream>

OperationNode::OperationNode(Operation op, int num_children, std::string name,
    const rapidjson::Value* options)
    : op(op), num_children(num_children), name(name), options(options) {
    children = new OperationNode*[num_children];
}

OperationNode::~OperationNode() {
    for (int i = 0; i < num_children; i++) {
        delete children[i];
    }
    delete[] children;
}

void OperationNode::set_child(OperationNode* child, int child_ind) {
    children[child_ind] = child;
}

void OperationNode::print(int level) {
    std::cout << std::string(" ", 2 * level) << "| " << name << std::endl;

    for (int i = 0; i < num_children; i++) {
        children[i]->print(level + 1);
    }
}

int OperationNode::get_num_children() {
    return num_children;
}

Operation OperationNode::get_operation() {
    return op;
}

OperationNode** OperationNode::get_children() {
    return children;
}

 const rapidjson::Value* OperationNode::get_options() {
     return options;
 }
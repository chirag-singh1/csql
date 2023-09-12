#include "operations.h"

#include <iostream>

OperationNode::OperationNode(Operation op, int num_children, std::string name)
    : op(op), num_children(num_children), name(name) {
    children = new OperationNode*[num_children];
}

OperationNode::~OperationNode() {
    for (int i = 0; i < num_children; i++) {
        delete children[i];
    }
    delete[] children;
}

void OperationNode::set_num_children(int n) {
    assert(num_children == 0); // Should only be altered when adding children.
    num_children = n;
    children = new OperationNode*[n];
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

std::string OperationNode::get_string_option(std::string key) {
    if (string_options.find(key) == string_options.end()) {
        return nullptr;
    }
    return string_options.find(key)->second;
}

void OperationNode::set_string_option(std::string key, std::string value) {
    string_options[key] = value;
}

bool OperationNode::get_bool_option(std::string key) {
    if (bool_options.find(key) == bool_options.end()) {
        return false;
    }
    return bool_options.find(key)->second;
}

void OperationNode::set_bool_option(std::string key, bool value) {
    bool_options[key] = value;
}

int OperationNode::get_int_option(std::string key) {
    if (int_options.find(key) == int_options.end()) {
        return false;
    }
    return int_options.find(key)->second;
}

void OperationNode::set_int_option(std::string key, int value) {
    int_options[key] = value;
}
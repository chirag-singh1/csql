#include "analyzer.h"
#include "../util/log.h"

#include <iostream>
#include <vector>

using std::string;
using std::vector;

Analyzer::Analyzer() {
    char* op_names[] = { OPERATIONS };
    for (int i = 0; i < NUM_SUPPORTED_OPERATIONS; i++) {
        operation_lookup[op_names[i]] = i;
    }
}

OperationNode* Analyzer::query_to_node(char* parsed_query) {
    LOG_DEBUG("Analyzing query", parsed_query);
    // Parse query as JSON.
    rapidjson::Document d;
    d.Parse(parsed_query);
    const rapidjson::Value& statements = d["stmts"];

    OperationNode* output_node =
        new OperationNode(OP_NONE, statements.Size(), "Root", nullptr);
    int i = 0;
    for (rapidjson::Value::ConstValueIterator itr = statements.Begin(); itr != statements.End(); ++itr) {
        const rapidjson::Value& statement = *itr;
        output_node->set_child(query_to_node_internal(&statement), i);
        i++;
    }
    output_node->print(0);

    return output_node;
}

OperationNode* Analyzer::query_to_node_internal(const rapidjson::Value* query) {
    JSON_LOG_DEBUG("Analyzing statement", query);

    const rapidjson::Value& stmt_obj = query->FindMember("stmt")->value;
    const string op_name = string(stmt_obj.MemberBegin()->name.GetString());
    const rapidjson::Value& options = stmt_obj.MemberBegin()->value;
    if (operation_lookup.find(op_name) != operation_lookup.end()) {
        return new OperationNode(operation_lookup.at(op_name), 0, op_name,
            &options);
    }
    else {
        return new OperationNode(OP_UNSUPPORTED, 0, "Unsupported", nullptr);
    }

    return nullptr;
}
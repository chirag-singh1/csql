#include "operations.h"
#include "../../lib/rapidjson/document.h"

#include <string>
#include <unordered_map>

class Analyzer {
    public:
        Analyzer();
        OperationNode* query_to_node(char* parsed_query);

    private:
        OperationNode* parse_filter(const rapidjson::Value* filter);
        void parse_filter_expr(const rapidjson::Value& val, bool is_left, OperationNode* n, int level);

        std::unordered_map<std::string, Operation> operation_lookup;
        std::unordered_map<std::string, Operation> filter_lookup;
        OperationNode* query_to_node_internal(
            const rapidjson::Value* statement);
};
#include "operations.h"
#include "../../lib/rapidjson/document.h"

#include <string>
#include <unordered_map>

class Analyzer {
    public:
        Analyzer();
        OperationNode* query_to_node(char* parsed_query);

    private:
        std::unordered_map<std::string, Operation> operation_lookup;
        OperationNode* query_to_node_internal(
            const rapidjson::Value* statement);
};
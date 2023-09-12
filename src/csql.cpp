#include "csql.h"
#include "util/file.h"
#include "constants.h"
#include "parser/parser.h"
#include "analyzer/analyzer.h"
#include "executor/executor.h"
#include "metadata/metadata_store.h"

#include <iostream>
#include <string>

void execute_query(Analyzer* a, MetadataStore* meta, std::string query) {
    Parser p = Parser(query);
    if (p.get_result()->error != nullptr) {
        std::cout << "ERROR:" << std::endl;
        std::cout << p.get_result()->error->message << std::endl;
    }
    else {
        OperationNode* execution_plan =
            a->query_to_node(p.get_result()->parse_tree);
        Executor e(execution_plan, meta);
        e.execute();

    }
}

void start_main_loop() {
    init_filesystem();

    std::cout << "Welcome to csql interactive shell." << std::endl;
    std::string user_in;
    std::cout << ">" << std::flush;
    std::getline(std::cin, user_in);

    Analyzer analyzer;
    MetadataStore meta;

    while (user_in.compare(EXIT_SHELL_INPUT) != 0) {
        execute_query(&analyzer, &meta, user_in);

        std::cout << ">" << std::flush;
        std::getline(std::cin, user_in);
    }
}
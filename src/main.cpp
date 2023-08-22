#include "constants.h"
#include "parser/parser.h"
#include "analyzer/analyzer.h"
#include "executor/executor.h"

#include <iostream>
#include <string>

void execute_query(Analyzer* a, std::string query) {
    Parser p = Parser(query);
    if (p.get_result()->error != nullptr) {
        std::cout << "ERROR:" << std::endl;
        std::cout << p.get_result()->error->message << std::endl;
    }
    else {
        OperationNode* execution_plan =
            a->query_to_node(p.get_result()->parse_tree);
        Executor e(execution_plan);
        e.execute();

    }
}

void start_main_loop() {
    std::string user_in;
    std::cout << ">" << std::flush;
    std::getline(std::cin, user_in);

    Analyzer analyzer;

    while (user_in.compare(EXIT_SHELL_INPUT) != 0) {
        execute_query(&analyzer, user_in);

        std::cout << ">" << std::flush;
        std::getline(std::cin, user_in);
    }
}

int main() {
    std::cout << "Welcome to csql interactive shell." << std::endl;
    start_main_loop();
}
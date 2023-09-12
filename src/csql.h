#pragma once

#include <string>

class Analyzer;
class MetadataStore;

void execute_query(Analyzer* a, MetadataStore* m, std::string query);
void start_main_loop();
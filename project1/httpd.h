#ifndef HTTPD_H
#define HTTPD_H

#include <string>
#include "httpFramer.hpp"
#include "httpParser.hpp"
#include "fileHelper.hpp"

using namespace std;

void start_httpd(unsigned short port, string doc_root);

void handle_connection (int clientSocket, string root);

httpResponse process_request(httpRequest requestMsg, string doc_root);

#endif // HTTPD_H

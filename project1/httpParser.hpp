#ifndef HTTPPARSER_HPP
#define HTTPPARSER_HPP

#include <string>
#include <stdint.h>
#include <unordered_map>

using namespace std;

// Format of http request
typedef struct http_request {
	string method;
	string URL;
	string http_version;
	unordered_map<string, string> key_values;
	bool valid;
} httpRequest;


// Format of http response
typedef struct http_response {
	string http_version;
	string status; // code with text
	string text;
	string body;
	string path;
	unordered_map<string, string> key_values;
} httpResponse;


class httpParser {
public:
	static httpRequest parse(string http_str);
};


#endif // HTTPPARSER_HPP

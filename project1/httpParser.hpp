#ifndef HTTPPARSER_HPP
#define HTTPPARSER_HPP

#include <string>
#include <stdint.h>
#include <unordered_map>

using namespace std;

// Format of http request
typedef struct http_request {
	string method;        // GET
	string URL;      
	string http_version;  // HTTP/1.1
	unordered_map<string, string> key_values;
	bool valid;
} httpRequest;


// Format of http response
typedef struct http_response {
	string http_version;
	string status;      // 200, 400, 403, 404
	string text;       
	string body;
	string path;        // requested valid file path
	unordered_map<string, string> key_values;
} httpResponse;


class httpParser {
public:
	// parse read-in message into httpRequest format
	static httpRequest parse(string http_str);

	// from httpResponse format construct response header
	static string create_response(httpResponse responseMsg);
};


#endif // HTTPPARSER_HPP

#include <assert.h>
#include <sstream>
#include <iostream>
#include <vector>
#include <algorithm>
#include "httpParser.hpp"

using namespace std;


http_request httpParser::parse(std::string http_str)
{
	httpRequest ret;
	string header; 
	vector<string> pairs;
	string key_values;
	if (http_str.size() == 0) {
		return ret;
	}
	
	ret.valid = true;
	// parse http_str into header and key_value lines
	int http_str_len = http_str.size();
	for(int i=0; i < http_str_len; i++) {
		if( i+1 < http_str_len && http_str.substr(i, 2) == "\r\n") {
			header = http_str.substr(0, i);
			key_values = http_str.substr(i+2);
			break;
		}
	}
	int j = 0;
	int key_values_len = key_values.size();
	for(int i=0; i < key_values_len; i++) {
		if( i+1 < key_values_len && key_values.substr(i, 2) == "\r\n") {
			pairs.push_back(key_values.substr(j, i-j));
			// cout << "key_value pair is " << key_values.substr(j, i-j)<<endl;
			j = i + 2;
		}
	}
	pairs.push_back(key_values.substr(j));

	// parse header into httpRequest
	std::stringstream ss(header);
	getline(ss, ret.method, ' ');
	if (ret.method != "GET") {
		ret.valid = false;
		return ret;
	}
	getline(ss, ret.URL, ' ');
	if (ret.URL == "/") {
		ret.URL = "/index.html";
	}
	getline(ss, ret.http_version);
	if (ret.http_version != "HTTP/1.1") {
		ret.valid = false;
		return ret;
	}


	// parse key_value pairs
	string key, value;
	for (auto pair: pairs) {
		pair.erase(std::remove(pair.begin(), pair.end(), ' '), pair.end());
		std::stringstream item(pair);
		getline(item, key, ':');
		getline(item, value);
		// cout << "key is: " + key << endl;
		// cout << "value is: " + value << endl;
		if (!ret.key_values.count(key)) {
			ret.key_values[key] = value;
		}
		else {
			ret.valid = false;
		}	
	}
	if (!ret.key_values.count("Host")) {
		ret.valid = false;
	}
	
	return ret;
}


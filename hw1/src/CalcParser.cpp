#include <assert.h>
#include <sstream>
#include <iostream>
#include "CalcParser.hpp"

uint64_t CalcParser::accumulator = 0;

CalcInstruction CalcParser::parse(std::string insstr)
{
	CalcInstruction ret;

	// PUT YOUR CODE HERE
	// if(insstr == "CRLF") {
	// 	return ret; 
	// }
	// cout << "input string is: " << insstr << endl;
	if (insstr.size() == 0) {
		return ret;
	}
	ret.operation = insstr.substr(0, 3);
	// cout << "operation is: " << ret.operation << endl;
	std::istringstream iss(insstr.substr(4));
	iss >> ret.value;
	// cout << "value is: " << ret.value << endl;
	if(ret.operation == "ADD") {
			accumulator += ret.value;
	}
	else if (ret.operation == "SUB") {
			accumulator -= ret.value; 
	}
	else if (ret.operation == "SET") {
			accumulator = ret.value;
	}
	else {
		// cout << "##################error#########" << endl;
	} 
	return ret;
}


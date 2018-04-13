#ifndef CALCPARSER_HPP
#define CALCPARSER_HPP

#include <string>
#include <stdint.h>

using namespace std;

typedef struct CalcInstruction_t {
	// DEFINE YOUR DATA STRUCTURE HERE
	string operation;
	uint64_t value;
} CalcInstruction;

/*
 * Alternatively:
 * class CalcInstruction {
 *   // DEFINE YOUR CLASS HERE
 * };
 *
 */

class CalcParser {
public:
	static CalcInstruction parse(string insstr);
	static uint64_t accumulator;
};


#endif // CALCPARSER_HPP

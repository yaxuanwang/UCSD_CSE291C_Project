#ifndef HTTPFRAMER_HPP
#define HTTPFRAMER_HPP

#include <iostream>


class httpFramer {
public:
	// append message to m_buffer
	void append(std::string chars);

	// check if buffer contain at least one complete message
	bool hasMessage() const;

	// return the first complete message
	std::string topMessage() const;

	// pop out the first complete message
	void popMessage();

protected:
	std::string m_buffer;
};


#endif // HTTPFRAMER_HPP

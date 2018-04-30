#include <assert.h>
#include <sstream>
#include <iostream>
#include "httpFramer.hpp"

using namespace std;


void httpFramer::append(string chars)
{
	m_buffer += chars;
}


bool httpFramer::hasMessage() const
{
	// cout << "current Message is:" << m_buffer << endl;
	int m_buffer_len = m_buffer.size();
	for (int i=0; i<m_buffer_len; i++) {
		if( i+3 < m_buffer_len && m_buffer.substr(i, 4) == "\r\n\r\n") {
			return true;
		}
	}
	return false;
}


string httpFramer::topMessage() const
{
	string ret = "";
	int m_buffer_len = m_buffer.size();
	for(int i=0; i<m_buffer_len; i++) {
		if( i+3 < m_buffer_len && m_buffer.substr(i, 4) == "\r\n\r\n") {
			ret = m_buffer.substr(0, i);
			return ret;
		}
	}
	return ret;
}


void httpFramer::popMessage()
{
	int m_buffer_len = m_buffer.size();
	for(int i=0; i<m_buffer_len; i++) {
		if( i+3 < m_buffer_len && m_buffer.substr(i, 4) == "\r\n\r\n") {
			if(i+4 < m_buffer_len) {
				m_buffer = m_buffer.substr(i+4);
			}
			else {
				m_buffer = "";
			}
			return;
		}
	}
}

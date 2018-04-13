#include <assert.h>
#include <sstream>
#include <iostream>
#include "CalcFramer.hpp"

using namespace std;

void CalcFramer::append(string chars)
{
	// PUT YOUR CODE HERE

	// std::stringstream ss(chars);
	// string item;
	// while(std::getline(ss, item, '\n')) {
	// 	m_buffer += item.substr(0, item.size()-1);
	// 	m_buffer += "CRLF";
	// }

	int j = 0;
	if (chars[0] == '\n') chars = chars.substr(1);
	for(int i=0; i<chars.size(); i++) {
		if( i+1 < chars.size() && chars.substr(i, 2) == "\r\n") {
			m_buffer += chars.substr(j, i-j);
			m_buffer += "CRLF";
			j = i + 2;
		}
	}
	m_buffer += chars.substr(j);
	int size = m_buffer.size();
	if (m_buffer[size-1] == '\r') {
		m_buffer += "CRLF";
	}
	// cout << m_buffer << endl;
}

bool CalcFramer::hasMessage() const
{
	// PUT YOUR CODE HERE
	// cout << "current Message is:" << m_buffer << endl;
	for (int i=0; i<m_buffer.size(); i++) {
		if( i+3 < m_buffer.size() && m_buffer.substr(i, 4) == "CRLF") {
			return true;
		}
	}
	// cout << "no Message" << endl;
	return false;
}

string CalcFramer::topMessage() const
{
	// PUT YOUR CODE HERE
	string ret = "";
	for(int i=0; i<m_buffer.size(); i++) {
		if( i+3 < m_buffer.size() && m_buffer.substr(i, 4) == "CRLF") {
			ret = m_buffer.substr(0, i);
			// if(ret.size()>0)cout << "topMessage is" << ret << endl;
			// else cout<<"empty"<<endl;
			return ret;
		}
	}
	return ret;
}

void CalcFramer::popMessage()
{
	// PUT YOUR CODE HERE
	for(int i=0; i<m_buffer.size(); i++) {
		if( i+3 < m_buffer.size() && m_buffer.substr(i, 4) == "CRLF") {
			if(i+4 < m_buffer.size()) {
				m_buffer = m_buffer.substr(i+4);
			}
			else {
				m_buffer = "";
			}
			return;
		}
	}
}

void CalcFramer::printToStream(ostream& stream) const
{
	// PUT YOUR CODE HERE
	string tmp;
	int j = 0;
	for(int i=0; i<m_buffer.size(); i++) {
		if( i+3 < m_buffer.size() && m_buffer.substr(i, 4) == "CRLF") {
			tmp += m_buffer.substr(j, i-j);
			j = i + 4;
		}
	}
	stream << tmp;
}

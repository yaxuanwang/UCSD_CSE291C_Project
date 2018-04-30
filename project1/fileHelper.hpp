#ifndef FILEHELPER_HPP
#define FILEHELPER_HPP

#include <string>

using namespace std;


// get last modified time of the file, return specific time format
string get_last_modified(const string abs_path);

// get type of the given file, html, png or jpg
string get_content_type(const string abs_path);

// get length of file content
long get_content_Length(const string abs_path);

// check if the given path exists a file
bool file_exists(const string abs_path);

// check if the corresponding file is "global readable"
bool has_permission(const string abs_path);


#endif // FILEHELPER_HPP

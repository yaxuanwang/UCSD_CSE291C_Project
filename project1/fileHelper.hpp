#ifndef FILEHELPER_HPP
#define FILEHELPER_HPP

#include <string>

using namespace std;


string get_last_modified(const string abs_path);

string get_content_type(const string abs_path);

long get_content_Length(const string abs_path);

bool file_exists(const string abs_path);

bool has_permission(const string abs_path);

#endif // FILEHELPER_HPP

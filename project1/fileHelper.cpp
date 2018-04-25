#include <sys/stat.h>      /* for struct stat and stat() */
#include "fileHelper.hpp"

using namespace std;


string get_last_modified(const string abs_path) {
	struct stat stbuf;
	::stat(abs_path.c_str(), &stbuf);
	struct tm *mtm = gmtime(&stbuf.st_mtime);
  	char buf[256];
  	int len = strftime(buf, 256, "%A, %d %m %Y %H:%M:%S GMT", mtm);
  	return string(buf, len);
}


string get_content_type(const string abs_path) {
	string type = abs_path.substr(abs_path.size()-4);
	string ret;
	if (type == ".png") {
		ret = "image/png";
	}
	else if (type == ".jpg") {
		ret = "image/jpeg";
	}
	else if (type == "html") {
		ret = "text/html";
	}
	return ret;
}


long get_content_Length(const string abs_path) {
	struct stat stat_buf;
    int rc = stat(abs_path.c_str(), &stat_buf);
    return rc == 0 ? stat_buf.st_size : -1;
}


bool file_exists(const string abs_path) {
    struct stat st;
    return ::stat(abs_path.c_str(), &st) == 0;
}


bool has_permission(const string abs_path) {
	struct stat st;
    ::stat(abs_path.c_str(), &st);
    return st.st_mode & S_IROTH;
}


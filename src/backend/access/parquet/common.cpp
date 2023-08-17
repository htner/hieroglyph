/*-------------------------------------------------------------------------
 *
 * common.cpp
 *		  FDW routines for parquet_s3_fdw
 *
 * Portions Copyright (c) 2021, TOSHIBA CORPORATION
 * Portions Copyright (c) 2018-2019, adjust GmbH
 *
 * IDENTIFICATION
 *		  contrib/parquet_s3_fdw/src/common.cpp
 *
 *-------------------------------------------------------------------------
 */
#include "common.hpp"

#include <sys/stat.h>
#include <errno.h>
#include <dirent.h>
#include <unistd.h>
#include <cstring>

/**
 * @brief check whether given directory existed or not
 *
 * @param path directory path
 * @return true if directory path existed
 */
bool
is_dir_exist(const std::string& path) {
    struct stat info;
    if (stat(path.c_str(), &info) != 0) {
        return false;
    }
    return (info.st_mode & S_IFDIR) != 0;
}

/**
 * @brief check whether given file existed or not
 *
 * @param path file path
 * @return true true if file path existed
 */
bool
is_file_exist(const std::string& path) {
    struct stat info;
    if (stat(path.c_str(), &info) != 0) {
        return false;
    }
    return ((info.st_mode & S_IFMT) == S_IFREG);
}

/**
 * @brief create a directory
 *
 * @param path directory path
 * @return true create successfully
 */
bool
make_path(const std::string& path) {
    mode_t mode = 0755;
    int ret = mkdir(path.c_str(), mode);

    if (ret == 0) {
        return true;
	}

    switch (errno) {
        case ENOENT: {
            /* parent didn't exist, try to create it */
            size_t pos = path.find_last_of('/');
            if (pos == std::string::npos)
                return false;
            if (!make_path( path.substr(0, pos) ))
                return false;

            /* now, try to create again */
            return 0 == mkdir(path.c_str(), mode);
        }

        case EEXIST:
            /* done! */
            return is_dir_exist(path);
        default:
            return false;
    }
}

/**
 * @brief remove recursively directoty if empty
 *
 * @param path
 * @return int
 */
int
remove_directory_if_empty(const char *path) {
    DIR        *d = opendir(path);
    int         ret = -1;
    bool        is_empty = true;

    if (d) {
        struct dirent *p;
        ret = 0;

        while (!ret && (p = readdir(d))) {
            int     ret2 = -1;

            /* Skip the names "." and ".." as we don't want to recurse on them. */
            if (strcmp(p->d_name, ".") == 0 || strcmp(p->d_name, "..") == 0)
                continue;

			std::string buf(path);
			buf += "/";
			buf += p->d_name;

			struct stat statbuf;

			if (!stat(buf.data(), &statbuf)) {
				if (S_ISDIR(statbuf.st_mode))
					ret2 = remove_directory_if_empty(buf.data());
				else
					is_empty = false;
			}
            ret = ret2;
        }
        closedir(d);
    }

    /* remove current dir if it not empty */
    if (!ret && is_empty)
        ret = rmdir(path);

    return ret;
}

/*-------------------------------------------------------------------------
 *
 * common.hpp
 *		  FDW routines for parquet_s3_fdw
 *
 * Portions Copyright (c) 2021, TOSHIBA CORPORATION
 * Portions Copyright (c) 2018-2019, adjust GmbH
 *
 * IDENTIFICATION
 *		  contrib/parquet_s3_fdw/src/common.hpp
 *
 *-------------------------------------------------------------------------
 */
#pragma once

#include <string>

bool is_dir_exist(const std::string& path);
bool is_file_exist(const std::string& path);
bool make_path(const std::string& path);
int remove_directory_if_empty(const char *path);


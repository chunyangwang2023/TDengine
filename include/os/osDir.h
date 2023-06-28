/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef _TD_OS_DIR_H_
#define _TD_OS_DIR_H_

// If the error is in a third-party library, place this header file under the third-party library header file.
// When you want to use this feature, you should find or add the same function in the following section.
#ifndef ALLOW_FORBID_FUNC
#define opendir  OPENDIR_FUNC_TAOS_FORBID
#define readdir  READDIR_FUNC_TAOS_FORBID
#define closedir CLOSEDIR_FUNC_TAOS_FORBID
#define dirname  DIRNAME_FUNC_TAOS_FORBID
#undef basename
#define basename BASENAME_FUNC_TAOS_FORBID
#endif

#ifdef __cplusplus
extern "C" {
#endif

#if defined(CUS_NAME) || defined(CUS_PROMPT) || defined(CUS_EMAIL)
#include "cus_name.h"
#endif

#ifdef WINDOWS

#define TD_TMP_DIR_PATH  "C:\\Windows\\Temp\\"
#ifdef CUS_NAME
#define TD_CFG_DIR_PATH  "C:\\"CUS_NAME"\\cfg\\"
#define TD_DATA_DIR_PATH "C:\\"CUS_NAME"\\data\\"
#define TD_LOG_DIR_PATH  "C:\\"CUS_NAME"\\log\\"
#else
#define TD_CFG_DIR_PATH  "C:\\TDengine\\cfg\\"
#define TD_DATA_DIR_PATH "C:\\TDengine\\data\\"
#define TD_LOG_DIR_PATH  "C:\\TDengine\\log\\"
#endif  // CUS_NAME

#elif defined(_TD_DARWIN_64)

#ifdef CUS_PROMPT
#define TD_TMP_DIR_PATH  "/tmp/"CUS_PROMPT"d/"
#define TD_CFG_DIR_PATH  "/etc/"CUS_PROMPT"/"
#define TD_DATA_DIR_PATH "/var/lib/"CUS_PROMPT"/"
#define TD_LOG_DIR_PATH  "/var/log/"CUS_PROMPT"/"
#else
#define TD_TMP_DIR_PATH  "/tmp/taosd/"
#define TD_CFG_DIR_PATH  "/etc/taos/"
#define TD_DATA_DIR_PATH "/var/lib/taos/"
#define TD_LOG_DIR_PATH  "/var/log/taos/"
#endif  // CUS_PROMPT

#else

#define TD_TMP_DIR_PATH  "/tmp/"
#ifdef CUS_PROMPT
#define TD_CFG_DIR_PATH  "/etc/"CUS_PROMPT"/"
#define TD_DATA_DIR_PATH "/var/lib/"CUS_PROMPT"/"
#define TD_LOG_DIR_PATH  "/var/log/"CUS_PROMPT"/"
#else
#define TD_CFG_DIR_PATH  "/etc/taos/"
#define TD_DATA_DIR_PATH "/var/lib/taos/"
#define TD_LOG_DIR_PATH  "/var/log/taos/"
#endif   // CUS_PROMPT
#endif

typedef struct TdDir      *TdDirPtr;
typedef struct TdDirEntry *TdDirEntryPtr;

void    taosRemoveDir(const char *dirname);
bool    taosDirExist(const char *dirname);
int32_t taosMkDir(const char *dirname);
int32_t taosMulMkDir(const char *dirname);
int32_t taosMulModeMkDir(const char *dirname, int mode);
void    taosRemoveOldFiles(const char *dirname, int32_t keepDays);
int32_t taosExpandDir(const char *dirname, char *outname, int32_t maxlen);
int32_t taosRealPath(char *dirname, char *realPath, int32_t maxlen);
bool    taosIsDir(const char *dirname);
char   *taosDirName(char *dirname);
char   *taosDirEntryBaseName(char *dirname);
void    taosGetCwd(char *buf, int32_t len);

TdDirPtr      taosOpenDir(const char *dirname);
TdDirEntryPtr taosReadDir(TdDirPtr pDir);
bool          taosDirEntryIsDir(TdDirEntryPtr pDirEntry);
char         *taosGetDirEntryName(TdDirEntryPtr pDirEntry);
int32_t       taosCloseDir(TdDirPtr *ppDir);

#ifdef _TD_SYLIXOS_
enum {
  WRDE_NOSPACE = 1, /* Ran out of memory.  */
  WRDE_BADCHAR,     /* A metachar appears in the wrong place.  */
  WRDE_BADVAL,      /* Undefined var reference with WRDE_UNDEF.  */
  WRDE_CMDSUB,      /* Command substitution with WRDE_NOCMD.  */
  WRDE_SYNTAX       /* Shell syntax error.  */
};

typedef struct {
  int    we_wordc;
  char **we_wordv;
  int    we_offs;
  char   wordPos[20];
} wordexp_t;

typedef int (*__compar_fn_t)(const void *, const void *);

int sendfile(int out_fd, int in_fd, off_t *offset, size_t size);
#endif

#ifdef __cplusplus
}
#endif

#endif /*_TD_OS_DIR_H_*/

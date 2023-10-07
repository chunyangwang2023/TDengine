/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3 * or later ("AGPL"), as published by the Free
 * Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include "index.h"

SIndexMultiTermQuery* indexMultiTermQueryCreate(EIndexOperatorType oper) { return NULL; }

void indexMultiTermQueryDestroy(SIndexMultiTermQuery* pQuery) {}

int32_t indexMultiTermQueryAdd(SIndexMultiTermQuery* pQuery, SIndexTerm* term, EIndexQueryType type) { return 0; }

int32_t indexOpen(SIndexOpts* opt, const char* path, SIndex** index) { return 0; }

void indexClose(SIndex* index) {}

int32_t indexPut(SIndex* index, SIndexMultiTerm* terms, uint64_t uid) { return 0; }

int32_t indexDelete(SIndex* index, SIndexMultiTermQuery* query) { return 0; }

int32_t indexSearch(SIndex* index, SIndexMultiTermQuery* query, SArray* result) { return 0; }

int32_t indexJsonOpen(SIndexJsonOpts* opts, const char* path, SIndexJson** index) { return 0; }

void indexJsonClose(SIndexJson* index) {}

int32_t indexJsonPut(SIndexJson* index, SIndexJsonMultiTerm* terms, uint64_t uid) { return 0; }

int32_t indexJsonSearch(SIndexJson* index, SIndexJsonMultiTermQuery* query, SArray* result) { return 0; }

SIndexMultiTerm* indexMultiTermCreate() { return NULL; }

int32_t indexMultiTermAdd(SIndexMultiTerm* terms, SIndexTerm* term) { return 0; }

void indexMultiTermDestroy(SIndexMultiTerm* terms) {}

SIndexOpts* indexOptsCreate(int32_t cacheSize) { return NULL; }

void indexOptsDestroy(SIndexOpts* opts) {}

SIndexTerm* indexTermCreate(int64_t suid, SIndexOperOnColumn operType, uint8_t colType, const char* colName,
                            int32_t nColName, const char* colVal, int32_t nColVal) {
  return NULL;
}

void indexTermDestroy(SIndexTerm* p) {}

void indexRebuild(SIndexJson* idx, void* iter) {}

bool indexIsRebuild(SIndex* idx) { return true; }

void indexJsonRebuild(SIndexJson* idx, void* iter) {}

bool indexJsonIsRebuild(SIndexJson* idx) { return true; }

SIdxFltStatus idxGetFltStatus(SNode* pFilterNode) { return SFLT_NOT_INDEX; }

int32_t doFilterTag(SNode* pFilterNode, SIndexMetaArg* metaArg, SArray* result, SIdxFltStatus* status) { return 0; }

void indexInit(int32_t threads) {}

void indexCleanup() {}

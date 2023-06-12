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

#include "tsdbFS.h"
#include "tsdbUtil.h"

#ifndef _TSDB_STT_FILE_RW_H
#define _TSDB_STT_FILE_RW_H

#ifdef __cplusplus
extern "C" {
#endif

typedef TARRAY2(SSttBlk) TSttBlkArray;
typedef TARRAY2(SStatisBlk) TStatisBlkArray;
typedef TARRAY2(STombBlk) TTombBlkArray;

// SSttFileReader ==========================================
typedef struct SSttFileReader       SSttFileReader;
typedef struct SSttFileReaderConfig SSttFileReaderConfig;
typedef struct SSttSegReader        SSttSegReader;
typedef TARRAY2(SSttFileReader *) TSttFileReaderArray;
typedef TARRAY2(SSttSegReader *) TSttSegReaderArray;

// SSttFileReader
int32_t tsdbSttFileReaderOpen(const char *fname, const SSttFileReaderConfig *config, SSttFileReader **reader);
int32_t tsdbSttFileReaderClose(SSttFileReader **reader);
int32_t tsdbSttFileReaderGetSegReader(SSttFileReader *reader, const TSttSegReaderArray **readerArray);

// SSttSegReader
int32_t tsdbSttFileReadSttBlk(SSttSegReader *reader, const TSttBlkArray **sttBlkArray);
int32_t tsdbSttFileReadStatisBlk(SSttSegReader *reader, const TStatisBlkArray **statisBlkArray);
int32_t tsdbSttFileReadTombBlk(SSttSegReader *reader, const TTombBlkArray **delBlkArray);

int32_t tsdbSttFileReadBlockData(SSttSegReader *reader, const SSttBlk *sttBlk, SBlockData *bData);
int32_t tsdbSttFileReadBlockDataByColumn(SSttSegReader *reader, const SSttBlk *sttBlk, SBlockData *bData,
                                         STSchema *pTSchema, int16_t cids[], int32_t ncid);
int32_t tsdbSttFileReadStatisBlock(SSttSegReader *reader, const SStatisBlk *statisBlk, STbStatisBlock *sData);
int32_t tsdbSttFileReadTombBlock(SSttSegReader *reader, const STombBlk *delBlk, STombBlock *dData);

struct SSttFileReaderConfig {
  STsdb    *tsdb;
  int32_t   szPage;
  STFile    file[1];
  uint8_t **bufArr;
};

// SSttFileWriter ==========================================
typedef struct SSttFileWriter       SSttFileWriter;
typedef struct SSttFileWriterConfig SSttFileWriterConfig;

int32_t tsdbSttFileWriterOpen(const SSttFileWriterConfig *config, SSttFileWriter **writer);
int32_t tsdbSttFileWriterClose(SSttFileWriter **writer, int8_t abort, TFileOpArray *opArray);
int32_t tsdbSttFileWriteRow(SSttFileWriter *writer, SRowInfo *row);
int32_t tsdbSttFileWriteBlockData(SSttFileWriter *writer, SBlockData *pBlockData);
int32_t tsdbSttFileWriteTombRecord(SSttFileWriter *writer, const STombRecord *record);
bool    tsdbSttFileWriterIsOpened(SSttFileWriter *writer);

struct SSttFileWriterConfig {
  STsdb    *tsdb;
  int32_t   maxRow;
  int32_t   szPage;
  int8_t    cmprAlg;
  int64_t   compactVersion;
  STFile    file;
  SSkmInfo *skmTb;
  SSkmInfo *skmRow;
  uint8_t **bufArr;
};

#ifdef __cplusplus
}
#endif

#endif /*_TSDB_STT_FILE_RW_H*/
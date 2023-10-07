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
#include "os.h"
#include "tudf.h"

int32_t udfAggFinalize(struct SqlFunctionCtx *pCtx, SSDataBlock *pBlock) {
  terrno = TSDB_CODE_OPS_NOT_SUPPORT;
  return -1;
}

int32_t udfAggProcess(struct SqlFunctionCtx *pCtx) {
  terrno = TSDB_CODE_OPS_NOT_SUPPORT;
  return -1;
}

int32_t udfcOpen() { return 0; }

int32_t udfcClose() { return 0; }

int32_t udfStartUdfd(int32_t startDnodeId) { return 0; }

int32_t udfStopUdfd() { return 0; }

int32_t callUdfScalarFunc(char *udfName, SScalarParam *input, int32_t numOfCols, SScalarParam *output) {
  terrno = TSDB_CODE_OPS_NOT_SUPPORT;
  return -1;
}

int32_t cleanUpUdfs() { return 0; }

bool udfAggGetEnv(struct SFunctionNode *pFunc, SFuncExecEnv *pEnv) {
  terrno = TSDB_CODE_OPS_NOT_SUPPORT;
  return false;
}

bool udfAggInit(struct SqlFunctionCtx *pCtx, struct SResultRowEntryInfo *pResultCellInfo) {
  terrno = TSDB_CODE_OPS_NOT_SUPPORT;
  return false;
}

/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is taosMemoryFree software: you can use, redistribute, and/or modify
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

#define _DEFAULT_SOURCE
#include "taoserror.h"
#include "tdef.h"
#include "tlog.h"
#include "tsched.h"
#include "ttimer.h"

tmr_h taosTmrStart(TAOS_TMR_CALLBACK fp, int32_t mseconds, void* param, void* handle) {
  tmr_h* timer = taosMemoryCalloc(1, 4);
  (*fp)(param, timer);
  return timer;
}

bool taosTmrStop(tmr_h timerId) {
  taosMemoryFree(timerId);
  return true;
}

bool taosTmrStopA(tmr_h* timerId) {
  bool ret = taosTmrStop(*timerId);
  *timerId = NULL;
  return ret;
}

bool taosTmrReset(TAOS_TMR_CALLBACK fp, int32_t mseconds, void* param, void* handle, tmr_h* pTmrId) { return true; }

void* taosTmrInit(int32_t maxNumOfTmrs, int32_t resolution, int32_t longest, const char* label) {
  tmr_h* tm0 = taosMemoryCalloc(1, 4);
  return tm0;
}

void taosTmrCleanUp(void* handle) { taosMemoryFree(handle); }

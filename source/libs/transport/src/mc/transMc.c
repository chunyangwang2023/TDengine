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

#include "thttp.h"
#include "trpc.h"

static SRpcInit* tsRpcHandle[TDMT_MAX] = {0};
static SRpcInit  tscHandle = {0};
static SRpcInit  dndHandle = {0};
static char      tscUser[TSDB_USER_LEN] = {0};

typedef struct {
  void*    ahandle;  // handle provided by app
  tmsg_t   msgType;  // message type
  SRpcMsg* pRsp;     // for synchronous API
  tsem_t*  pSem;     // for synchronous API
} SSlimCtx;

static void rpcInitHandle() {
  for (int32_t i = 0; i < TDMT_VND_TMQ_MAX_MSG; ++i) {
    int32_t type = TMSG_INDEX(i);
    if (type >= TDMT_MAX) continue;

    if (i % 2 == 1) {
      tsRpcHandle[type] = &dndHandle;
    } else {
      tsRpcHandle[type] = &tscHandle;
    }
  }

  if (tscHandle.user != NULL && tscHandle.user[0] != 0) {
    tstrncpy(tscUser, tscHandle.user, TSDB_USER_LEN);
  }

  // dmhandle
  tsRpcHandle[TMSG_INDEX(TDMT_MND_AUTH_RSP)] = &dndHandle;

  // mmhandle
  tsRpcHandle[TMSG_INDEX(TDMT_DND_CREATE_MNODE_RSP)] = &dndHandle;
  tsRpcHandle[TMSG_INDEX(TDMT_DND_DROP_MNODE_RSP)] = &dndHandle;
  tsRpcHandle[TMSG_INDEX(TDMT_DND_CREATE_QNODE_RSP)] = &dndHandle;
  tsRpcHandle[TMSG_INDEX(TDMT_DND_DROP_QNODE_RSP)] = &dndHandle;
  tsRpcHandle[TMSG_INDEX(TDMT_DND_CREATE_SNODE_RSP)] = &dndHandle;
  tsRpcHandle[TMSG_INDEX(TDMT_DND_DROP_SNODE_RSP)] = &dndHandle;
  tsRpcHandle[TMSG_INDEX(TDMT_DND_CREATE_VNODE_RSP)] = &dndHandle;
  tsRpcHandle[TMSG_INDEX(TDMT_DND_DROP_VNODE_RSP)] = &dndHandle;
  tsRpcHandle[TMSG_INDEX(TDMT_DND_CONFIG_DNODE_RSP)] = &dndHandle;

  tsRpcHandle[TMSG_INDEX(TDMT_MND_ALTER_MNODE_RSP)] = &dndHandle;
  tsRpcHandle[TMSG_INDEX(TDMT_MND_TMQ_DROP_CGROUP_RSP)] = &dndHandle;

  tsRpcHandle[TMSG_INDEX(TDMT_VND_CREATE_STB_RSP)] = &dndHandle;
  tsRpcHandle[TMSG_INDEX(TDMT_VND_ALTER_STB_RSP)] = &dndHandle;
  tsRpcHandle[TMSG_INDEX(TDMT_VND_DROP_STB_RSP)] = &dndHandle;
  tsRpcHandle[TMSG_INDEX(TDMT_VND_DROP_TTL_TABLE_RSP)] = &dndHandle;
  tsRpcHandle[TMSG_INDEX(TDMT_VND_CREATE_SMA_RSP)] = &dndHandle;
  tsRpcHandle[TMSG_INDEX(TDMT_VND_DROP_SMA_RSP)] = &dndHandle;
  tsRpcHandle[TMSG_INDEX(TDMT_VND_TMQ_SUBSCRIBE_RSP)] = &dndHandle;
  tsRpcHandle[TMSG_INDEX(TDMT_VND_TMQ_DELETE_SUB_RSP)] = &dndHandle;
  tsRpcHandle[TMSG_INDEX(TDMT_VND_TMQ_ADD_CHECKINFO_RSP)] = &dndHandle;
  tsRpcHandle[TMSG_INDEX(TDMT_VND_TMQ_DEL_CHECKINFO_RSP)] = &dndHandle;
  tsRpcHandle[TMSG_INDEX(TDMT_SCH_DROP_TASK)] = &dndHandle;
  tsRpcHandle[TMSG_INDEX(TDMT_STREAM_TASK_DEPLOY_RSP)] = &dndHandle;
  tsRpcHandle[TMSG_INDEX(TDMT_STREAM_TASK_DROP_RSP)] = &dndHandle;
  tsRpcHandle[TMSG_INDEX(TDMT_VND_ALTER_CONFIG_RSP)] = &dndHandle;
  tsRpcHandle[TMSG_INDEX(TDMT_VND_ALTER_REPLICA_RSP)] = &dndHandle;
  tsRpcHandle[TMSG_INDEX(TDMT_VND_ALTER_CONFIRM_RSP)] = &dndHandle;
  tsRpcHandle[TMSG_INDEX(TDMT_VND_ALTER_HASHRANGE_RSP)] = &dndHandle;
  tsRpcHandle[TMSG_INDEX(TDMT_VND_COMPACT_RSP)] = &dndHandle;
  tsRpcHandle[TMSG_INDEX(TDMT_VND_CREATE_INDEX_RSP)] = &dndHandle;
  tsRpcHandle[TMSG_INDEX(TDMT_VND_DROP_INDEX_RSP)] = &dndHandle;
  tsRpcHandle[TMSG_INDEX(TDMT_VND_DISABLE_WRITE_RSP)] = &dndHandle;

  tsRpcHandle[TMSG_INDEX(TDMT_SYNC_FORCE_FOLLOWER_RSP)] = &dndHandle;

  tsRpcHandle[TMSG_INDEX(TDMT_SYNC_SNAPSHOT_RSP)] = &dndHandle;

  // qmhandle
  // tsRpcHandle[TMSG_INDEX(TDMT_SCH_FETCH_RSP)] = &dndHandle;
  tsRpcHandle[TMSG_INDEX(TDMT_MND_AUTH_RSP)] = &dndHandle;
  tsRpcHandle[TMSG_INDEX(TDMT_MND_AUTH_RSP)] = &dndHandle;

  // smhandle
  tsRpcHandle[TMSG_INDEX(TDMT_STREAM_TASK_DISPATCH_RSP)] = &dndHandle;
  tsRpcHandle[TMSG_INDEX(TDMT_STREAM_RETRIEVE_RSP)] = &dndHandle;

  // vmhandle
  tsRpcHandle[TMSG_INDEX(TDMT_STREAM_TASK_DISPATCH_RSP)] = &dndHandle;
  tsRpcHandle[TMSG_INDEX(TDMT_STREAM_RETRIEVE_RSP)] = &dndHandle;
  tsRpcHandle[TMSG_INDEX(TDMT_STREAM_TASK_CHECK_RSP)] = &dndHandle;
  tsRpcHandle[TMSG_INDEX(TDMT_SYNC_SNAPSHOT_RSP)] = &dndHandle;
}

int32_t rpcInit() { return 0; }

void rpcCleanup() {}

void* rpcOpen(const SRpcInit* pInit) {
  if (strncmp(pInit->label, "DND", 3) == 0) {
    memcpy(&dndHandle, pInit, sizeof(SRpcInit));
    rpcInitHandle();
    return &dndHandle;
  } else if (strncmp(pInit->label, "TSC", 3) == 0) {
    memcpy(&tscHandle, pInit, sizeof(SRpcInit));
    rpcInitHandle();
    return &tscHandle;
  } else {
    return NULL;
  }
}

void rpcClose(void* pRpc) {}

void* rpcMallocCont(int64_t size) {
  char* start = taosMemoryCalloc(1, size);
  if (start == NULL) {
    uError("failed to malloc msg, size:%" PRId64, size);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  } else {
    uTrace("malloc mem:%p size:%" PRId64, start, size);
  }

  return start;
}

void rpcFreeCont(void* cont) {
  if (cont == NULL) return;
  taosMemoryFree(cont);
  uTrace("rpc free cont:%p", cont);
}

void* rpcReallocCont(void* ptr, int64_t contLen) {
  if (ptr == NULL) return rpcMallocCont(contLen);

  ptr = taosMemoryRealloc(ptr, contLen);
  if (ptr == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  uTrace("rpc realloc cont:%p", ptr);
  return ptr;
}

int32_t rpcSendRequest(void* shandle, const SEpSet* pEpSet, SRpcMsg* pMsg, int64_t* pRid) {
  SRpcInit* pRpc = tsRpcHandle[TMSG_INDEX(pMsg->msgType)];
  pMsg->info.msgType = pMsg->msgType;
  pMsg->info.ctx = 0;
  pMsg->info.handle = shandle;
  tstrncpy(pMsg->info.conn.user, tscUser, TSDB_USER_LEN);
  (pRpc->cfp)(pRpc->parent, pMsg, NULL);

  return 0;
}

int32_t rpcSendRequestWithCtx(void* shandle, const SEpSet* pEpSet, SRpcMsg* pMsg, int64_t* pRid, SRpcCtx* pCtx) {
  SRpcInit* pRpc = tsRpcHandle[TMSG_INDEX(pMsg->msgType)];
  pMsg->info.msgType = pMsg->msgType;
  pMsg->info.ctx = 0;
  pMsg->info.handle = shandle;
  tstrncpy(pMsg->info.conn.user, tscUser, TSDB_USER_LEN);
  (pRpc->cfp)(pRpc->parent, pMsg, NULL);

  return 0;
}

int rpcSendRecv(void* shandle, SEpSet* pEpSet, SRpcMsg* pMsg, SRpcMsg* pRsp) {
  tsem_t sem = {0};
  tsem_init(&sem, 0, 0);
  SSlimCtx* pCtx = taosMemoryCalloc(1, sizeof(SSlimCtx));
  pCtx->ahandle = pMsg->info.ahandle;
  pCtx->msgType = pMsg->msgType;
  pCtx->pSem = &sem;
  pCtx->pRsp = pRsp;

  SRpcInit* pRpc = tsRpcHandle[TMSG_INDEX(pMsg->msgType)];
  pMsg->info.msgType = pMsg->msgType;
  pMsg->info.ctx = pCtx;
  pMsg->info.handle = shandle;
  tstrncpy(pMsg->info.conn.user, tscUser, TSDB_USER_LEN);
  (pRpc->cfp)(pRpc->parent, pMsg, NULL);
  tsem_wait(&sem);

  tsem_destroy(&sem);
  taosMemoryFree(pCtx);
  return 0;
}

int32_t rpcSendResponse(const SRpcMsg* pMsg) {
  if (pMsg->info.noResp) {
    rpcFreeCont(pMsg->pCont);
    uTrace("no need send resp");
    return 0;
  }

  if (pMsg->info.ctx != 0) {
    SSlimCtx* pCtx = pMsg->info.ctx;
    memcpy(pCtx->pRsp, pMsg, sizeof(SRpcMsg));
    tsem_post(pCtx->pSem);
  } else {
    SRpcMsg* pVMsg = (SRpcMsg*)pMsg;
    tstrncpy(pVMsg->info.conn.user, tscUser, TSDB_USER_LEN);
    pVMsg->msgType = pMsg->info.msgType + 1;
    SRpcInit* pRpc = tsRpcHandle[TMSG_INDEX(pVMsg->msgType)];
    (pRpc->cfp)(pRpc->parent, pVMsg, NULL);
  }

  return 0;
}

int32_t rpcRegisterBrokenLinkArg(SRpcMsg* msg) { return 0; }

int32_t rpcReleaseHandle(void* handle, int8_t type) { return 0; }

int32_t taosSendHttpReport(const char* server, const char* uri, uint16_t port, char* pCont, int32_t contLen,
                           EHttpCompFlag flag) {
  return 0;
}

int32_t rpcSetDefaultAddr(void* thandle, const char* ip, const char* fqdn) { return 0; }

void* rpcAllocHandle() { return NULL; }
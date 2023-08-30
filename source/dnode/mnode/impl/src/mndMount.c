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

#define _DEFAULT_SOURCE
#include "mndMount.h"
#include "mndDb.h"
#include "mndDnode.h"
#include "mndPrivilege.h"
#include "mndShow.h"
#include "mndStb.h"
#include "mndTrans.h"
#include "mndUser.h"

#define MOUNT_VER_NUMBER   1
#define MOUNT_RESERVE_SIZE 64

static SSdbRow *mndMountActionDecode(SSdbRaw *pRaw);
SSdbRaw        *mndMountActionEncode(SMountObj *pMount);
static int32_t  mndMountActionInsert(SSdb *pSdb, SMountObj *pMount);
static int32_t  mndMountActionDelete(SSdb *pSdb, SMountObj *pMount);
static int32_t  mndMountActionUpdate(SSdb *pSdb, SMountObj *pOld, SMountObj *pNew);
static int32_t  mndProcessCreateMountReq(SRpcMsg *pReq);
static int32_t  mndProcessDropMountReq(SRpcMsg *pReq);
static int32_t  mndRetrieveMounts(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void     mndCancelGetNextMount(SMnode *pMnode, void *pIter);

int32_t mndInitMount(SMnode *pMnode) {
  SSdbTable table = {
      .sdbType = SDB_MOUNT,
      .keyType = SDB_KEY_BINARY,
      .encodeFp = (SdbEncodeFp)mndMountActionEncode,
      .decodeFp = (SdbDecodeFp)mndMountActionDecode,
      .insertFp = (SdbInsertFp)mndMountActionInsert,
      .updateFp = (SdbUpdateFp)mndMountActionUpdate,
      .deleteFp = (SdbDeleteFp)mndMountActionDelete,
  };

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_MOUNT, mndProcessCreateMountReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_MOUNT, mndProcessDropMountReq);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_MOUNT, mndRetrieveMounts);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_MOUNT, mndCancelGetNextMount);
  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupMount(SMnode *pMnode) {}

SSdbRaw *mndMountActionEncode(SMountObj *pMount) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  SSdbRaw *pRaw = sdbAllocRaw(SDB_MOUNT, MOUNT_VER_NUMBER, sizeof(SMountObj) + MOUNT_RESERVE_SIZE);
  if (pRaw == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_SET_BINARY(pRaw, dataPos, pMount->name, TSDB_MOUNT_NAME_LEN, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pMount->path, TSDB_MOUNT_PATH_LEN, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pMount->dnodeId, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pMount->createdTime, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pMount->updateTime, _OVER)

  SDB_SET_RESERVE(pRaw, dataPos, MOUNT_RESERVE_SIZE, _OVER)
  SDB_SET_DATALEN(pRaw, dataPos, _OVER)

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("mount:%s, failed to encode to raw:%p since %s", pMount->name, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("mount:%s, encode to raw:%p, row:%p", pMount->name, pRaw, pMount);
  return pRaw;
}

static SSdbRow *mndMountActionDecode(SSdbRaw *pRaw) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  SSdbRow   *pRow = NULL;
  SMountObj *pMount = NULL;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto _OVER;

  if (sver != 1) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto _OVER;
  }

  pRow = sdbAllocRow(sizeof(SMountObj));
  if (pRow == NULL) goto _OVER;

  pMount = sdbGetRowObj(pRow);
  if (pMount == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_GET_BINARY(pRaw, dataPos, pMount->name, TSDB_MOUNT_NAME_LEN, _OVER)
  SDB_GET_BINARY(pRaw, dataPos, pMount->path, TSDB_MOUNT_PATH_LEN, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pMount->dnodeId, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pMount->createdTime, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pMount->updateTime, _OVER)
  SDB_GET_RESERVE(pRaw, dataPos, MOUNT_RESERVE_SIZE, _OVER)

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("mount:%s, failed to decode from raw:%p since %s", pMount == NULL ? "null" : pMount->name, pRaw, terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("mount:%s, decode from raw:%p, row:%p", pMount->name, pRaw, pMount);
  return pRow;
}

static int32_t mndMountActionInsert(SSdb *pSdb, SMountObj *pMount) {
  mTrace("mount:%s, perform insert action, row:%p", pMount->name, pMount);
  return 0;
}

static int32_t mndMountActionDelete(SSdb *pSdb, SMountObj *pMount) {
  mTrace("mount:%s, perform delete action, row:%p", pMount->name, pMount);
  return 0;
}

static int32_t mndMountActionUpdate(SSdb *pSdb, SMountObj *pOld, SMountObj *pNew) {
  mTrace("mount:%s, perform update action, old row:%p new row:%p", pOld->name, pOld, pNew);
  tstrncpy(pOld->path, pNew->path, TSDB_MOUNT_PATH_LEN);
  pOld->dnodeId = pNew->dnodeId;
  return 0;
}

SMountObj *mndAcquireMount(SMnode *pMnode, const char *mountName) {
  SMountObj *pObj = sdbAcquire(pMnode->pSdb, SDB_MOUNT, &mountName);
  if (pObj == NULL && terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
    terrno = TSDB_CODE_MND_MOUNT_NOT_EXIST;
  }
  return pObj;
}

void mndReleaseMount(SMnode *pMnode, SMountObj *pMount) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pMount);
}

static int32_t mndSetCreateMountRedoLogs(SMnode *pMnode, STrans *pTrans, SMountObj *pMount) {
  SSdbRaw *pRedoRaw = mndMountActionEncode(pMount);
  if (pRedoRaw == NULL) return -1;
  if (mndTransAppendRedolog(pTrans, pRedoRaw) != 0) return -1;
  if (sdbSetRawStatus(pRedoRaw, SDB_STATUS_CREATING) != 0) return -1;
  return 0;
}

static int32_t mndSetCreateMountUndoLogs(SMnode *pMnode, STrans *pTrans, SMountObj *pMount) {
  SSdbRaw *pUndoRaw = mndMountActionEncode(pMount);
  if (pUndoRaw == NULL) return -1;
  if (mndTransAppendUndolog(pTrans, pUndoRaw) != 0) return -1;
  if (sdbSetRawStatus(pUndoRaw, SDB_STATUS_DROPPED) != 0) return -1;
  return 0;
}

static int32_t mndSetCreateMountCommitLogs(SMnode *pMnode, STrans *pTrans, SMountObj *pMount) {
  SSdbRaw *pCommitRaw = mndMountActionEncode(pMount);
  if (pCommitRaw == NULL) return -1;
  if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) return -1;
  if (sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY) != 0) return -1;
  return 0;
}

static int32_t mndSetCreateMountRedoActions(SMnode *pMnode, STrans *pTrans, SDnodeObj *pDnode, SMountObj *pMount) {
  return 0;
}

static int32_t mndSetCreateMountUndoActions(SMnode *pMnode, STrans *pTrans, SDnodeObj *pDnode, SMountObj *pMount) {
  return 0;
}

static int32_t mndCreateMount(SMnode *pMnode, SDnodeObj *pDnode, SCreateMountReq *pCreate, SRpcMsg *pReq) {
  int32_t code = -1;

  SMountObj mountObj = {0};
  tstrncpy(mountObj.name, pCreate->mountName, TSDB_MOUNT_NAME_LEN);
  tstrncpy(mountObj.path, pCreate->mountPath, TSDB_MOUNT_PATH_LEN);
  mountObj.dnodeId = pCreate->mountDnodeId;
  mountObj.createdTime = taosGetTimestampMs();
  mountObj.updateTime = mountObj.createdTime;

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_GLOBAL, pReq, "create-mount");
  if (pTrans == NULL) goto _OVER;
  mndTransSetSerial(pTrans);

  mInfo("trans:%d, used to create mount:%s", pTrans->id, pCreate->mountName);
  if (mndTrancCheckConflict(pMnode, pTrans) != 0) goto _OVER;

  if (mndSetCreateMountRedoLogs(pMnode, pTrans, &mountObj) != 0) goto _OVER;
  if (mndSetCreateMountUndoLogs(pMnode, pTrans, &mountObj) != 0) goto _OVER;
  if (mndSetCreateMountRedoActions(pMnode, pTrans, pDnode, &mountObj) != 0) goto _OVER;
  if (mndSetCreateMountUndoActions(pMnode, pTrans, pDnode, &mountObj) != 0) goto _OVER;
  if (mndSetCreateMountCommitLogs(pMnode, pTrans, &mountObj) != 0) goto _OVER;
  if (mndTransPrepare(pMnode, pTrans) != 0) goto _OVER;

  code = 0;

_OVER:
  mndTransDrop(pTrans);
  return code;
}

static int32_t mndProcessCreateMountReq(SRpcMsg *pReq) {
  SMnode         *pMnode = pReq->info.node;
  int32_t         code = -1;
  SMountObj      *pMount = NULL;
  SUserObj       *pOperUser = NULL;
  SDnodeObj      *pDnode = NULL;
  SCreateMountReq createReq = {0};

  if (tDeserializeSCreateMountReq(pReq->pCont, pReq->contLen, &createReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mInfo("mount:%s, start to create", createReq.mountName);
  if (mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_CREATE_MOUNT) != 0) {
    goto _OVER;
  }

  if (createReq.mountName[0] == 0) {
    terrno = TSDB_CODE_MND_INVALID_MOUNT_NAME;
    goto _OVER;
  }

  if (createReq.mountPath[0] == 0) {
    terrno = TSDB_CODE_MND_INVALID_MOUNT_PATH;
    goto _OVER;
  }

  pMount = mndAcquireMount(pMnode, createReq.mountName);
  if (pMount != NULL) {
    terrno = TSDB_CODE_MND_MOUNT_ALREADY_EXIST;
    goto _OVER;
  }

  pDnode = mndAcquireDnode(pMnode, createReq.mountDnodeId);
  if (pDnode == NULL) {
    terrno = TSDB_CODE_MND_DNODE_NOT_EXIST;
    goto _OVER;
  }

  if (!mndIsDnodeOnline(pDnode, taosGetTimestampMs())) {
    terrno = TSDB_CODE_DNODE_OFFLINE;
    goto _OVER;
  }

  pOperUser = mndAcquireUser(pMnode, pReq->info.conn.user);
  if (pOperUser == NULL) {
    terrno = TSDB_CODE_MND_NO_USER_FROM_CONN;
    goto _OVER;
  }

  if ((terrno = grantCheck(TSDB_GRANT_USER)) != 0) {
    code = terrno;
    goto _OVER;
  }

  code = mndCreateMount(pMnode, pDnode, &createReq, pReq);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("mount:%s, failed to create since %s", createReq.mountName, terrstr());
  }

  mndReleaseDnode(pMnode, pDnode);
  mndReleaseMount(pMnode, pMount);
  mndReleaseUser(pMnode, pOperUser);

  return code;
}

static int32_t mndSetDropMountRedoLogs(SMnode *pMnode, STrans *pTrans, SMountObj *pMount) {
  SSdbRaw *pRedoRaw = mndMountActionEncode(pMount);
  if (pRedoRaw == NULL) return -1;
  if (mndTransAppendRedolog(pTrans, pRedoRaw) != 0) return -1;
  if (sdbSetRawStatus(pRedoRaw, SDB_STATUS_DROPPING) != 0) return -1;
  return 0;
}

static int32_t mndSetDropMountCommitLogs(SMnode *pMnode, STrans *pTrans, SMountObj *pMount) {
  SSdbRaw *pCommitRaw = mndMountActionEncode(pMount);
  if (pCommitRaw == NULL) return -1;
  if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) return -1;
  if (sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED) != 0) return -1;
  return 0;
}

static int32_t mndSetDropMountRedoActions(SMnode *pMnode, STrans *pTrans, SDnodeObj *pDnode, SMountObj *pMount) {
  return 0;
}

static int32_t mndDropMount(SMnode *pMnode, SRpcMsg *pReq, SDnodeObj *pDnode, SMountObj *pMount) {
  int32_t code = -1;
  STrans *pTrans = NULL;

  pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_GLOBAL, pReq, "drop-mount");
  if (pTrans == NULL) goto _OVER;
  mndTransSetSerial(pTrans);

  mInfo("trans:%d, used to drop mount:%s", pTrans->id, pMount->name);
  if (mndTrancCheckConflict(pMnode, pTrans) != 0) goto _OVER;

  if (mndSetDropMountRedoLogs(pMnode, pTrans, pMount) != 0) return -1;
  if (mndSetDropMountRedoActions(pMnode, pTrans, pDnode, pMount) != 0) return -1;
  if (mndSetDropMountCommitLogs(pMnode, pTrans, pMount) != 0) return -1;
  if (mndTransPrepare(pMnode, pTrans) != 0) goto _OVER;

  code = 0;

_OVER:
  mndTransDrop(pTrans);
  return code;
}

static int32_t mndProcessDropMountReq(SRpcMsg *pReq) {
  SMnode       *pMnode = pReq->info.node;
  int32_t       code = -1;
  SMountObj    *pMount = NULL;
  SDnodeObj    *pDnode = NULL;
  SDropMountReq dropReq = {0};

  if (tDeserializeSDropMountReq(pReq->pCont, pReq->contLen, &dropReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mInfo("mount:%s, start to drop", dropReq.mountName);
  if (mndCheckOperPrivilege(pMnode, pReq->info.conn.user, MND_OPER_DROP_USER) != 0) {
    goto _OVER;
  }

  if (dropReq.mountName[0] == 0) {
    terrno = TSDB_CODE_MND_INVALID_MOUNT_NAME;
    goto _OVER;
  }

  pMount = mndAcquireMount(pMnode, dropReq.mountName);
  if (pMount == NULL) {
    terrno = TSDB_CODE_MND_MOUNT_NOT_EXIST;
    goto _OVER;
  }

  pDnode = mndAcquireDnode(pMnode, pMount->dnodeId);
  if (pDnode == NULL) {
    terrno = TSDB_CODE_MND_DNODE_NOT_EXIST;
    goto _OVER;
  }

  if (!mndIsDnodeOnline(pDnode, taosGetTimestampMs())) {
    terrno = TSDB_CODE_DNODE_OFFLINE;
    goto _OVER;
  }

  code = mndDropMount(pMnode, pReq, pDnode, pMount);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("mount:%s, failed to drop since %s", dropReq.mountName, terrstr());
  }

  mndReleaseMount(pMnode, pMount);
  mndReleaseDnode(pMnode, pDnode);
  return code;
}

static int32_t mndRetrieveMounts(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode    *pMnode = pReq->info.node;
  SSdb      *pSdb = pMnode->pSdb;
  int32_t    numOfRows = 0;
  SMountObj *pMount = NULL;
  int32_t    cols = 0;
  char      *pWrite;

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_MOUNT, pShow->pIter, (void **)&pMount);
    if (pShow->pIter == NULL) break;

    cols = 0;
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    char             name[TSDB_MOUNT_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(name, pMount->name, pShow->pMeta->pSchemas[cols].bytes);
    colDataSetVal(pColInfo, numOfRows, (const char *)name, false);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    colDataSetVal(pColInfo, numOfRows, (const char *)&pMount->dnodeId, false);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    colDataSetVal(pColInfo, numOfRows, (const char *)&pMount->createdTime, false);

    cols++;
    char path[TSDB_MOUNT_PATH_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(path, pMount->path, TSDB_MOUNT_PATH_LEN + VARSTR_HEADER_SIZE);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    colDataSetVal(pColInfo, numOfRows, (const char *)path, false);

    numOfRows++;
    sdbRelease(pSdb, pMount);
  }

  pShow->numOfRows += numOfRows;
  return numOfRows;
}

static void mndCancelGetNextMount(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}

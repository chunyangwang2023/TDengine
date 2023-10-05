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
#include "mndCluster.h"
#include "mndDb.h"
#include "mndDnode.h"
#include "mndPrivilege.h"
#include "mndShow.h"
#include "mndStb.h"
#include "mndTrans.h"
#include "mndUser.h"
#include "mndVgroup.h"
#include "tjson.h"

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

  mndSetMsgHandle(pMnode, TDMT_DND_MOUNT_VNODE_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_DND_UNMOUNT_VNODE_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_DND_SET_MOUNT_INFO_RSP, mndTransProcessRsp);

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_MOUNT, mndRetrieveMounts);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_MOUNT, mndCancelGetNextMount);
  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupMount(SMnode *pMnode) {}

SSdbRaw *mndMountActionEncode(SMountObj *pMount) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  SSdbRaw *pRaw = sdbAllocRaw(SDB_MOUNT, MOUNT_VER_NUMBER,
                              sizeof(SMountObj) + sizeof(int64_t) * pMount->dbSize +
                                  sizeof(SMountVgPair) * pMount->vgPairSize + MOUNT_RESERVE_SIZE);
  if (pRaw == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_SET_BINARY(pRaw, dataPos, pMount->name, TSDB_MOUNT_NAME_LEN, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pMount->path, TSDB_MOUNT_PATH_LEN, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pMount->dnodeId, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pMount->createdTime, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pMount->updateTime, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pMount->dbSize, _OVER)
  for (int32_t db = 0; db < pMount->dbSize; ++db) {
    SDB_SET_INT64(pRaw, dataPos, pMount->dbUids[db], _OVER)
  }
  SDB_SET_INT32(pRaw, dataPos, pMount->vgPairSize, _OVER)
  for (int32_t vg = 0; vg < pMount->vgPairSize; ++vg) {
    SDB_SET_INT32(pRaw, dataPos, pMount->vgPairs[vg].srcVgId, _OVER)
    SDB_SET_INT32(pRaw, dataPos, pMount->vgPairs[vg].dstVgId, _OVER)
  }

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
  SDB_GET_INT32(pRaw, dataPos, &pMount->dbSize, _OVER)
  pMount->dbUids = taosMemoryCalloc(pMount->dbSize, sizeof(int64_t));
  for (int32_t db = 0; db < pMount->dbSize; ++db) {
    SDB_GET_INT64(pRaw, dataPos, &pMount->dbUids[db], _OVER)
  }
  SDB_GET_INT32(pRaw, dataPos, &pMount->vgPairSize, _OVER)
  pMount->vgPairs = taosMemoryCalloc(pMount->vgPairSize, sizeof(SMountVgPair));
  for (int32_t vg = 0; vg < pMount->vgPairSize; ++vg) {
    SDB_GET_INT32(pRaw, dataPos, &pMount->vgPairs[vg].srcVgId, _OVER)
    SDB_GET_INT32(pRaw, dataPos, &pMount->vgPairs[vg].dstVgId, _OVER)
  }

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
  taosMemoryFreeClear(pMount->dbUids);
  return 0;
}

static int32_t mndMountActionUpdate(SSdb *pSdb, SMountObj *pOld, SMountObj *pNew) {
  mTrace("mount:%s, perform update action, old row:%p new row:%p", pOld->name, pOld, pNew);
  tstrncpy(pOld->path, pNew->path, TSDB_MOUNT_PATH_LEN);
  pOld->dnodeId = pNew->dnodeId;
  pOld->dbSize = pNew->dbSize;
  pOld->vgPairSize = pNew->vgPairSize;
  memcpy(pOld->dbUids, pNew->dbUids, sizeof(int64_t) * pNew->dbSize);
  memcpy(pOld->vgPairs, pNew->vgPairs, sizeof(SMountVgPair) * pNew->vgPairSize);
  return 0;
}

SMountObj *mndAcquireMount(SMnode *pMnode, const char *mountName) {
  SMountObj *pObj = sdbAcquire(pMnode->pSdb, SDB_MOUNT, mountName);
  if (pObj == NULL && terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
    terrno = TSDB_CODE_MND_MOUNT_NOT_EXIST;
  }
  return pObj;
}

SMountObj *mndAcquireMountByPathAndDnode(SMnode *pMnode, const char *mountPath, int32_t mountDnodeId) {
  SSdb *pSdb = pMnode->pSdb;
  void *pIter = NULL;

  while (1) {
    SMountObj *pMount = NULL;
    pIter = sdbFetch(pSdb, SDB_MOUNT, pIter, (void **)&pMount);
    if (pIter == NULL) break;
    if (pMount->dnodeId == mountDnodeId && strcmp(pMount->path, mountPath) == 0) {
      return pMount;
    }
    sdbRelease(pSdb, pMount);
  }

  return NULL;
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

SDbObj *mndAcquireDbByUid(SMnode *pMnode, int64_t uid) {
  SSdb *pSdb = pMnode->pSdb;
  void *pIter = NULL;

  while (1) {
    SDbObj *pDb = NULL;
    pIter = sdbFetch(pSdb, SDB_DB, pIter, (void **)&pDb);
    if (pIter == NULL) break;
    if (pDb->uid == uid) return pDb;
    sdbRelease(pSdb, pDb);
  }

  return NULL;
}

SStbObj *mndAcquireStbByUid(SMnode *pMnode, int64_t uid) {
  SSdb *pSdb = pMnode->pSdb;
  void *pIter = NULL;

  while (1) {
    SStbObj *pStb = NULL;
    pIter = sdbFetch(pSdb, SDB_STB, pIter, (void **)&pStb);
    if (pIter == NULL) break;
    if (pStb->uid == uid) return pStb;
    sdbRelease(pSdb, pStb);
  }

  return NULL;
}

void *mndBuildMountInfoReq(SMnode *pMnode, SMountObj *pMount, bool isMount, int32_t *pContLen) {
  SSetMountInfoReq mountReq = {0};
  tstrncpy(mountReq.mountName, pMount->name, sizeof(mountReq.mountName));
  tstrncpy(mountReq.mountPath, pMount->path, sizeof(mountReq.mountPath));
  mountReq.isMount = isMount;

  int32_t contLen = tSerializeSSetMountInfoReq(NULL, 0, &mountReq);
  if (contLen < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  void *pReq = taosMemoryMalloc(contLen);
  if (pReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  tSerializeSSetMountInfoReq(pReq, contLen, &mountReq);
  *pContLen = contLen;
  return pReq;
}

void *mndBuildMountVnodeReq(SMnode *pMnode, SMountObj *pMount, SDnodeObj *pDnode, SDbObj *pDb, SVgObj *pVgroup,
                            int32_t mountVgId, int8_t isMount, int32_t *pContLen) {
  SMountVnodeReq mountReq = {0};
  mountReq.vgId = pVgroup->vgId;
  mountReq.dnodeId = pDnode->id;
  mountReq.mountVgId = mountVgId;
  tstrncpy(mountReq.mountPath, pMount->path, sizeof(mountReq.mountPath));
  mountReq.dbUid = pDb->uid;
  mountReq.isMount = isMount;
  tstrncpy(mountReq.db, pDb->name, sizeof(mountReq.db));

  int32_t contLen = tSerializeSMountVnodeReq(NULL, 0, &mountReq);
  if (contLen < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  void *pReq = taosMemoryMalloc(contLen);
  if (pReq == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  tSerializeSMountVnodeReq(pReq, contLen, &mountReq);
  *pContLen = contLen;
  return pReq;
}

int32_t mndAddSetMountInfoAction(SMnode *pMnode, STrans *pTrans, SMountObj *pMount, bool isMount) {
  STransAction action = {0};

  SDnodeObj *pDnode = mndAcquireDnode(pMnode, pMount->dnodeId);
  if (pDnode == NULL) return -1;
  action.epSet = mndGetDnodeEpset(pDnode);
  mndReleaseDnode(pMnode, pDnode);

  int32_t contLen = 0;
  void   *pReq = mndBuildMountInfoReq(pMnode, pMount, isMount, &contLen);
  if (pReq == NULL) return -1;

  action.pCont = pReq;
  action.contLen = contLen;
  action.msgType = TDMT_DND_SET_MOUNT_INFO;

  if (mndTransAppendRedoAction(pTrans, &action) != 0) {
    taosMemoryFree(pReq);
    return -1;
  }

  return 0;
}

int32_t mndAddMountVnodeAction(SMnode *pMnode, STrans *pTrans, SMountObj *pMount, SDbObj *pDstDb, SVgObj *pDstVgroup,
                               SVnodeGid *pDstVgid, SVgObj *pSrcVgroup) {
  STransAction action = {0};

  SDnodeObj *pDnode = mndAcquireDnode(pMnode, pDstVgid->dnodeId);
  if (pDnode == NULL) return -1;
  action.epSet = mndGetDnodeEpset(pDnode);
  mndReleaseDnode(pMnode, pDnode);

  int32_t contLen = 0;
  void   *pReq = mndBuildMountVnodeReq(pMnode, pMount, pDnode, pDstDb, pDstVgroup, pSrcVgroup->vgId, 1, &contLen);
  if (pReq == NULL) return -1;

  action.pCont = pReq;
  action.contLen = contLen;
  action.msgType = TDMT_DND_MOUNT_VNODE;

  if (mndTransAppendRedoAction(pTrans, &action) != 0) {
    taosMemoryFree(pReq);
    return -1;
  }

  return 0;
}

int32_t mndAddUnMountVnodeAction(SMnode *pMnode, STrans *pTrans, SMountObj *pMount, SDbObj *pDstDb, SVgObj *pDstVgroup,
                                 SVnodeGid *pDstVgid, int32_t srcVgId, bool isRedo) {
  STransAction action = {0};

  SDnodeObj *pDnode = mndAcquireDnode(pMnode, pDstVgid->dnodeId);
  if (pDnode == NULL) return -1;
  action.epSet = mndGetDnodeEpset(pDnode);
  mndReleaseDnode(pMnode, pDnode);

  int32_t contLen = 0;
  void   *pReq = mndBuildMountVnodeReq(pMnode, pMount, pDnode, pDstDb, pDstVgroup, srcVgId, 0, &contLen);
  if (pReq == NULL) return -1;

  action.pCont = pReq;
  action.contLen = contLen;
  action.msgType = TDMT_DND_UNMOUNT_VNODE;
  action.acceptableCode = TSDB_CODE_VND_NOT_EXIST;

  if (isRedo) {
    if (mndTransAppendRedoAction(pTrans, &action) != 0) {
      taosMemoryFree(pReq);
      return -1;
    }
  } else {
    if (mndTransAppendUndoAction(pTrans, &action) != 0) {
      taosMemoryFree(pReq);
      return -1;
    }
  }

  return 0;
}

static SVgObj *mndGetMountVgroupByDbAndIndex(SArray *pVgroups, SDbObj *pDb, int32_t indexOfDbVgroup) {
  int32_t localIndex = 0;
  for (int32_t vg = 0; vg < (int32_t)taosArrayGetSize(pVgroups); ++vg) {
    SVgObj *pVgroup = taosArrayGet(pVgroups, vg);
    if (pDb->uid == pVgroup->dbUid) {
      if (localIndex == indexOfDbVgroup) {
        return pVgroup;
      } else {
        localIndex++;
      }
    }
  }

  return NULL;
}

static int32_t mndSetCreateMountCommitLogs(SMnode *pMnode, STrans *pTrans, SMountObj *pMount, SArray *pSrcDbs,
                                           SArray *pSrcVgroups, SArray *pSrcStbs) {
  SSdbRaw *pCommitRaw = mndMountActionEncode(pMount);
  if (pCommitRaw == NULL) return -1;
  if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) return -1;
  if (sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY) != 0) return -1;

  int32_t vgPairPos = 0;
  for (int32_t db = 0; db < (int32_t)taosArrayGetSize(pSrcDbs); ++db) {
    SDbObj *pSrcDb = taosArrayGet(pSrcDbs, db);
    char    dstDbName[TSDB_DB_FNAME_LEN] = {0};
    snprintf(dstDbName, TSDB_DB_FNAME_LEN - 1, "%s_%s", pSrcDb->name, pMount->name);

    SDbObj *pDstDb = pSrcDb;
    tstrncpy(pDstDb->name, dstDbName, TSDB_DB_FNAME_LEN);

    SDbObj *pSameNameDb = mndAcquireDb(pMnode, pDstDb->name);
    if (pSameNameDb != NULL) {
      mError("mount:%s, db:%s failed to mount since its already exist", pMount->name, pSameNameDb->name);
      mndReleaseDb(pMnode, pSameNameDb);
      return -1;
    }

    SDbObj *pSameUidDb = mndAcquireDbByUid(pMnode, pDstDb->uid);
    if (pSameUidDb != NULL) {
      mError("mount:%s, db:%s failed to mount since uid:%" PRId64 " already exist in db:%s", pMount->name, pDstDb->name,
             pDstDb->uid, pSameUidDb->name);
      mndReleaseDb(pMnode, pSameUidDb);
      return -1;
    }

    mInfo("mount:%s, db:%s uid:%" PRId64 " will be mounted, index:%d", pMount->name, pDstDb->name, pDstDb->uid, db);
    SSdbRaw *pDbRaw = mndDbActionEncode(pDstDb);
    if (pDbRaw == NULL) return -1;
    if (mndTransAppendCommitlog(pTrans, pDbRaw) != 0) return -1;
    if (sdbSetRawStatus(pDbRaw, SDB_STATUS_READY) != 0) return -1;

    SVgObj *pDstVgroups = NULL;
    if (mndAllocVgroup(pMnode, pDstDb, &pDstVgroups) != 0) {
      mError("mount:%s, db:%s failed to mount while alloc vgroup since %s", pMount->name, pDstDb->name, terrstr());
      return -1;
    }

    for (int32_t vg = 0; vg < pDstDb->cfg.numOfVgroups; ++vg) {
      SVgObj *pDstVgroup = pDstVgroups + vg;
      SVgObj *pSrcVgroup = mndGetMountVgroupByDbAndIndex(pSrcVgroups, pDstDb, vg);
      pDstVgroup->version = pSrcVgroup->version;
      pMount->vgPairs[vgPairPos].srcVgId = pSrcVgroup->vgId;
      pMount->vgPairs[vgPairPos].dstVgId = pDstVgroup->vgId;
      vgPairPos++;

      mInfo("mount:%s, dst vgId:%d mounted from src vgId:%d, version:%d", pMount->name, pDstVgroup->vgId,
            pSrcVgroup->vgId, pDstVgroup->version);

      SSdbRaw *pVgRaw = mndVgroupActionEncode(pDstVgroup);
      if (pVgRaw == NULL) return -1;
      if (mndTransAppendCommitlog(pTrans, pVgRaw) != 0) {
        taosMemoryFree(pDstVgroups);
        return -1;
      }
      if (sdbSetRawStatus(pVgRaw, SDB_STATUS_READY) != 0) {
        taosMemoryFree(pDstVgroups);
        return -1;
      }

      SVnodeGid *pDstVgid = &pDstVgroup->vnodeGid[0];
      if (mndAddCreateVnodeAction(pMnode, pTrans, pDstDb, pDstVgroup, pDstVgid) != 0) {
        taosMemoryFree(pDstVgroups);
        return -1;
      }

      if (mndAddMountVnodeAction(pMnode, pTrans, pMount, pDstDb, pDstVgroup, pDstVgid, pSrcVgroup) != 0) {
        taosMemoryFree(pDstVgroups);
        return -1;
      }

      if (mndAddUnMountVnodeAction(pMnode, pTrans, pMount, pDstDb, pDstVgroup, pDstVgid, pSrcVgroup->vgId, 0) !=
          0) {
        taosMemoryFree(pDstVgroups);
        return -1;
      }

      if (mndAddDropVnodeAction(pMnode, pTrans, pDstDb, pDstVgroup, pDstVgid, false) != 0) {
        taosMemoryFree(pDstVgroups);
        return -1;
      }
    }

    for (int32_t stb = 0; stb < (int32_t)taosArrayGetSize(pSrcDbs); ++stb) {
      SStbObj *pSrcStb = taosArrayGet(pSrcDbs, stb);
      SStbObj *pDstStb = pSrcStb;
      tstrncpy(pDstStb->db, pDstDb->name, TSDB_DB_FNAME_LEN);
      pDstStb->dbUid = pDstDb->uid;

      SStbObj *pSameNameStb = mndAcquireStb(pMnode, pDstStb->name);
      if (pSameNameStb != NULL) {
        mError("mount:%s, db:%s failed to mount since stb:%s already exist", pMount->name, pDstDb->name,
               pSameNameStb->name);
        mndReleaseStb(pMnode, pSameNameStb);
        return -1;
      }

      SStbObj *pSameUidStb = mndAcquireStbByUid(pMnode, pDstStb->uid);
      if (pSameUidStb != NULL) {
        mError("mount:%s, db:%s failed to mount since stb:%s uid:%" PRId64 " already exist in stb:%s", pMount->name,
               pDstDb->name, pDstStb->name, pDstStb->uid, pSameUidStb->name);
        mndReleaseStb(pMnode, pSameNameStb);
        return -1;
      }

      mInfo("mount:%s, stb:%s will be mounted, uid:%" PRId64, pMount->name, pDstStb->name, pDstStb->uid);
      SSdbRaw *pStbRaw = mndStbActionEncode(pDstStb);
      if (pStbRaw == NULL) return -1;
      if (mndTransAppendCommitlog(pTrans, pStbRaw) != 0) return -1;
      if (sdbSetRawStatus(pStbRaw, SDB_STATUS_READY) != 0) return -1;
    }

    taosMemoryFree(pDstVgroups);
  }

  if (mndAddSetMountInfoAction(pMnode, pTrans, pMount, true) != 0) {
    return -1;
  }

  return 0;
}

#define mountParseInt8(jsonObj, optStr, optObj) \
  {                                             \
    tjsonGetStringValue(jsonObj, optStr, tmp);  \
    if (code < 0) return -1;                    \
    optObj = (int8_t)atoi(tmp);                 \
  }

#define mountParseInt16(jsonObj, optStr, optObj) \
  {                                              \
    tjsonGetStringValue(jsonObj, optStr, tmp);   \
    if (code < 0) return -1;                     \
    optObj = (int16_t)atoi(tmp);                 \
  }

#define mountParseInt32(jsonObj, optStr, optObj) \
  {                                              \
    tjsonGetStringValue(jsonObj, optStr, tmp);   \
    if (code < 0) return -1;                     \
    optObj = (int32_t)atoi(tmp);                 \
  }

#define mountParseUInt32(jsonObj, optStr, optObj) \
  {                                               \
    tjsonGetStringValue(jsonObj, optStr, tmp);    \
    if (code < 0) return -1;                      \
    optObj = (uint32_t)atoi(tmp);                 \
  }

#define mountParseInt64(jsonObj, optStr, optObj) \
  {                                              \
    tjsonGetStringValue(jsonObj, optStr, tmp);   \
    if (code < 0) return -1;                     \
    optObj = taosStr2int64(tmp);                 \
  }

#define mountParseString(jsonObj, optStr, optObj) \
  {                                               \
    tjsonGetStringValue(jsonObj, optStr, optObj); \
    if (code < 0) return -1;                      \
  }

int32_t mmdMountParseDbs(SJson *root, SArray *pArray) {
  char    tmp[256] = {0};
  int32_t code = 0;
  SJson  *dbs = tjsonGetObjectItem(root, "dbs");
  if (dbs == NULL) return -1;

  int32_t num = (int32_t)cJSON_GetArraySize(dbs);
  for (int32_t i = 0; i < num; i++) {
    SJson *db = tjsonGetArrayItem(dbs, i);
    if (db == NULL) return -1;

    SDbObj dbObj = {0};
    mountParseString(db, "name", dbObj.name);
    mountParseString(db, "acct", dbObj.acct);
    mountParseString(db, "createUser", dbObj.createUser);
    mountParseInt64(db, "createdTime", dbObj.createdTime);
    mountParseInt64(db, "updateTime", dbObj.updateTime);
    mountParseInt64(db, "uid", dbObj.uid);
    mountParseInt32(db, "cfgVersion", dbObj.cfgVersion);
    mountParseInt32(db, "vgVersion", dbObj.vgVersion);
    mountParseInt32(db, "numOfVgroups", dbObj.cfg.numOfVgroups);
    mountParseInt32(db, "numOfStables", dbObj.cfg.numOfStables);
    mountParseInt32(db, "buffer", dbObj.cfg.buffer);
    mountParseInt32(db, "pageSize", dbObj.cfg.pageSize);
    mountParseInt32(db, "pages", dbObj.cfg.pages);
    mountParseInt32(db, "cacheLastSize", dbObj.cfg.cacheLastSize);
    mountParseInt32(db, "daysPerFile", dbObj.cfg.daysPerFile);
    mountParseInt32(db, "daysToKeep0", dbObj.cfg.daysToKeep0);
    mountParseInt32(db, "daysToKeep1", dbObj.cfg.daysToKeep1);
    mountParseInt32(db, "daysToKeep2", dbObj.cfg.daysToKeep2);
    mountParseInt32(db, "minRows", dbObj.cfg.minRows);
    mountParseInt32(db, "maxRows", dbObj.cfg.maxRows);
    mountParseInt8(db, "precision", dbObj.cfg.precision);
    mountParseInt8(db, "compression", dbObj.cfg.compression);
    mountParseInt8(db, "replications", dbObj.cfg.replications);
    mountParseInt8(db, "strict", dbObj.cfg.strict);
    mountParseInt8(db, "cacheLast", dbObj.cfg.cacheLast);
    mountParseInt8(db, "hashMethod", dbObj.cfg.hashMethod);
    mountParseInt16(db, "hashPrefix", dbObj.cfg.hashPrefix);
    mountParseInt16(db, "hashSuffix", dbObj.cfg.hashSuffix);
    mountParseInt16(db, "sstTrigger", dbObj.cfg.sstTrigger);
    mountParseInt32(db, "tsdbPageSize", dbObj.cfg.tsdbPageSize);
    mountParseInt8(db, "schemaless", dbObj.cfg.schemaless);
    mountParseInt8(db, "walLevel", dbObj.cfg.walLevel);
    mountParseInt32(db, "walFsyncPeriod", dbObj.cfg.walFsyncPeriod);
    mountParseInt32(db, "walRetentionPeriod", dbObj.cfg.walRetentionPeriod);
    mountParseInt64(db, "walRetentionSize", dbObj.cfg.walRetentionSize);
    mountParseInt32(db, "walRollPeriod", dbObj.cfg.walRollPeriod);
    mountParseInt64(db, "walSegmentSize", dbObj.cfg.walSegmentSize);
    dbObj.cfg.numOfRetensions = 0;

    if (dbObj.cfg.replications != 1) {
      terrno = TSDB_CODE_MND_MOUNT_INVALID_REPLICA;
      return -1;
    }

    if (taosArrayPush(pArray, &dbObj) == NULL) return -1;
  }

  return 0;
}

int32_t mmdMountParseVgroups(SJson *root, SArray *pArray, int32_t dnodeId) {
  char    tmp[256] = {0};
  int32_t code = 0;
  SJson  *vgroups = tjsonGetObjectItem(root, "vgroups");
  if (vgroups == NULL) return -1;

  int32_t num = (int32_t)cJSON_GetArraySize(vgroups);
  for (int32_t i = 0; i < num; i++) {
    SJson *vgroup = tjsonGetArrayItem(vgroups, i);
    if (vgroup == NULL) return -1;

    SVgObj vgroupObj = {0};
    mountParseInt32(vgroup, "vgId", vgroupObj.vgId);
    mountParseInt64(vgroup, "createdTime", vgroupObj.createdTime);
    mountParseInt64(vgroup, "updateTime", vgroupObj.updateTime);
    mountParseInt32(vgroup, "version", vgroupObj.version);
    mountParseUInt32(vgroup, "hashBegin", vgroupObj.hashBegin);
    mountParseUInt32(vgroup, "hashEnd", vgroupObj.hashEnd);
    mountParseString(vgroup, "db", vgroupObj.dbName);
    mountParseInt64(vgroup, "dbUid", vgroupObj.dbUid);
    mountParseInt64(vgroup, "replica", vgroupObj.replica);
    vgroupObj.isTsma = 0;
    vgroupObj.replica = 1;
    vgroupObj.vnodeGid[0].dnodeId = dnodeId;

    if (taosArrayPush(pArray, &vgroupObj) == NULL) return -1;
  }

  return 0;
}

int32_t mmdMountParseStbs(SJson *root, SArray *pArray) {
  char    tmp[256] = {0};
  int32_t code = 0;
  SJson  *stbs = tjsonGetObjectItem(root, "stbs");
  if (stbs == NULL) return -1;

  int32_t num = (int32_t)cJSON_GetArraySize(stbs);
  for (int32_t i = 0; i < num; i++) {
    SJson *stb = tjsonGetArrayItem(stbs, i);
    if (stb == NULL) return -1;

    SStbObj stbObj = {0};
    mountParseString(stb, "name", stbObj.name);
    mountParseString(stb, "db", stbObj.db);
    mountParseInt64(stb, "createdTime", stbObj.createdTime);
    mountParseInt64(stb, "updateTime", stbObj.updateTime);
    mountParseInt64(stb, "uid", stbObj.uid);
    mountParseInt64(stb, "dbUid", stbObj.dbUid);
    mountParseInt32(stb, "tagVer", stbObj.tagVer);
    mountParseInt32(stb, "colVer", stbObj.colVer);
    mountParseInt32(stb, "smaVer", stbObj.smaVer);
    mountParseInt32(stb, "nextColId", stbObj.nextColId);
    mountParseInt64(stb, "watermark1", stbObj.watermark[0]);
    mountParseInt64(stb, "watermark2", stbObj.watermark[1]);
    mountParseInt64(stb, "maxdelay0", stbObj.maxdelay[0]);
    mountParseInt64(stb, "maxdelay1", stbObj.maxdelay[1]);
    mountParseInt32(stb, "ttl", stbObj.ttl);
    mountParseInt32(stb, "numOfColumns", stbObj.numOfColumns);
    mountParseInt32(stb, "numOfTags", stbObj.numOfTags);

    stbObj.pColumns = taosMemoryCalloc(stbObj.numOfColumns, sizeof(SSchema));
    stbObj.pTags = taosMemoryCalloc(stbObj.numOfTags, sizeof(SSchema));
    if (stbObj.pColumns == NULL || stbObj.pTags == NULL) return -1;

    SJson *tags = tjsonGetObjectItem(stb, "tags");
    if (tags == NULL) return -1;
    for (int32_t j = 0; j < stbObj.numOfTags; ++j) {
      SJson  *tag = tjsonGetArrayItem(tags, j);
      SSchema schema = {0};
      mountParseInt8(tag, "type", schema.type);
      mountParseInt8(tag, "flags", schema.flags);
      mountParseInt16(tag, "colId", schema.colId);
      mountParseInt32(tag, "bytes", schema.bytes);
      mountParseString(tag, "name", schema.name);
      stbObj.pTags[j] = schema;
    }

    SJson *columns = tjsonGetObjectItem(stb, "columns");
    if (columns == NULL) return -1;
    for (int32_t j = 0; j < stbObj.numOfColumns; ++j) {
      SJson  *column = tjsonGetArrayItem(columns, j);
      SSchema schema = {0};
      mountParseInt8(column, "type", schema.type);
      mountParseInt8(column, "flags", schema.flags);
      mountParseInt16(column, "colId", schema.colId);
      mountParseInt32(column, "bytes", schema.bytes);
      mountParseString(column, "name", schema.name);
      stbObj.pColumns[j] = schema;
    }

    stbObj.numOfFuncs = 0;
    stbObj.ast1Len = 0;
    stbObj.ast2Len = 0;
    stbObj.commentLen = 0;

    if (taosArrayPush(pArray, &stbObj) == NULL) return -1;
  }

  return 0;
}

int32_t mmdMountParseCluster(SJson *root, SClusterObj *clusterObj) {
  char    tmp[256] = {0};
  int64_t cluster = 0;
  int32_t code = 0;
  SJson  *clusters = tjsonGetObjectItem(root, "clusters");
  if (clusters == NULL) return -1;

  int32_t num = (int32_t)cJSON_GetArraySize(clusters);
  for (int32_t i = 0; i < num; i++) {
    SJson *cluster = tjsonGetArrayItem(clusters, i);
    if (cluster == NULL) return -1;

    mountParseInt64(cluster, "id", clusterObj->id);
    mountParseInt64(cluster, "createdTime", clusterObj->createdTime);
    mountParseInt64(cluster, "updateTime", clusterObj->updateTime);
    mountParseString(cluster, "name", clusterObj->name);
  }

  return 0;
}

static int32_t mndCreateMount(SMnode *pMnode, SDnodeObj *pDnode, SCreateMountReq *pCreate, SGetMountInfoRsp *pInfo,
                              SRpcMsg *pReq) {
  int32_t     code = -1;
  SClusterObj clusterObj = {0};
  SArray     *pDbs = taosArrayInit(2, sizeof(SDbObj));
  SArray     *pVgrups = taosArrayInit(2, sizeof(SVgObj));
  SArray     *pStbs = taosArrayInit(4, sizeof(SStbObj));
  SJson      *root = tjsonParse(pInfo->jsonStr);

  if (root == NULL) {
    terrno = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _OVER;
  }

  mInfo("mount:%s, parse meta info", pCreate->mountName);
  if (mmdMountParseCluster(root, &clusterObj) != 0) goto _OVER;
  if (mmdMountParseDbs(root, pDbs) != 0) goto _OVER;
  if (mmdMountParseVgroups(root, pVgrups, pDnode->id) != 0) goto _OVER;
  if (mmdMountParseStbs(root, pStbs) != 0) goto _OVER;
  mInfo("mount:%s, meta info is parsed, cluster:%" PRId64 " dbs:%d vgroups:%d stbs:%d", pCreate->mountName,
        clusterObj.id, (int32_t)taosArrayGetSize(pDbs), (int32_t)taosArrayGetSize(pVgrups),
        (int32_t)taosArrayGetSize(pStbs));

  if (mndGetClusterId(pMnode) == clusterObj.id) {
    terrno = TSDB_CODE_MND_MOUNT_SAME_CULSTER;
    goto _OVER;
  }

  if (taosArrayGetSize(pDbs) <= 0) {
    terrno = TSDB_CODE_MND_MOUNT_DB_NOT_EXIST;
    goto _OVER;
  }

  if (taosArrayGetSize(pDbs) > 1) {
    terrno = TSDB_CODE_MND_MOUNT_TOO_MANY_DB;
    goto _OVER;
  }

#if 0
  SDbObj *pDb = taosArrayGet(pDbs, 0);
  if (strcmp(pDb->name, "tdlite") != 0) {
    terrno = TSDB_CODE_MND_MOUNT_INVALID_DB_NAME;
    goto _OVER;
  }
#endif

  SMountObj mountObj = {0};
  tstrncpy(mountObj.name, pCreate->mountName, TSDB_MOUNT_NAME_LEN);
  tstrncpy(mountObj.path, pCreate->mountPath, TSDB_MOUNT_PATH_LEN);
  mountObj.dnodeId = pCreate->mountDnodeId;
  mountObj.createdTime = taosGetTimestampMs();
  mountObj.updateTime = mountObj.createdTime;
  mountObj.dbSize = (int32_t)taosArrayGetSize(pDbs);
  mountObj.dbUids = taosMemoryCalloc(mountObj.dbSize, sizeof(int64_t));
  for (int32_t db = 0; db < mountObj.dbSize; ++db) {
    SDbObj *pDb = taosArrayGet(pDbs, db);
    mountObj.dbUids[db] = pDb->uid;
  }
  mountObj.vgPairSize = (int32_t)taosArrayGetSize(pVgrups);
  mountObj.vgPairs = taosMemoryCalloc(mountObj.vgPairSize, sizeof(SMountVgPair));

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_GLOBAL, pReq, "create-mount");
  if (pTrans == NULL) goto _OVER;
  mndTransSetSerial(pTrans);

  mInfo("trans:%d, used to create mount:%s", pTrans->id, pCreate->mountName);
  if (mndTrancCheckConflict(pMnode, pTrans) != 0) goto _OVER;
  if (mndSetCreateMountRedoLogs(pMnode, pTrans, &mountObj) != 0) goto _OVER;
  if (mndSetCreateMountUndoLogs(pMnode, pTrans, &mountObj) != 0) goto _OVER;
  if (mndSetCreateMountCommitLogs(pMnode, pTrans, &mountObj, pDbs, pVgrups, pStbs) != 0) goto _OVER;
  if (mndTransPrepare(pMnode, pTrans) != 0) goto _OVER;

  code = 0;

_OVER:
  mndTransDrop(pTrans);

  if (root != NULL) cJSON_Delete(root);
  if (pDbs != NULL) taosArrayDestroy(pDbs);
  if (pStbs != NULL) taosArrayDestroy(pStbs);
  if (pVgrups != NULL) taosArrayDestroy(pVgrups);
  taosMemoryFreeClear(mountObj.dbUids);

  return code;
}

static int32_t mndGetMountInfo(SMnode *pMnode, SDnodeObj *pDnode, SCreateMountReq *pMount, SGetMountInfoRsp *pRsp) {
  SGetMountInfoReq req = {0};
  tstrncpy(req.mountName, pMount->mountName, sizeof(req.mountName));
  tstrncpy(req.mountPath, pMount->mountPath, sizeof(req.mountPath));
  req.mountDnodeId = pMount->mountDnodeId;

  int32_t contLen = tSerializeSGetMountInfoReq(NULL, 0, &req);
  void   *pHead = rpcMallocCont(contLen);
  tSerializeSGetMountInfoReq(pHead, contLen, &req);

  SRpcMsg rpcMsg = {
      .pCont = pHead,
      .contLen = contLen,
      .msgType = TDMT_DND_GET_MOUNT_INFO,
      .info.ahandle = (void *)0x9537,
      .info.refId = 0,
      .info.noResp = 0,
  };
  SRpcMsg rpcRsp = {0};

  mInfo("mount:%s, send get-mount-info to dnode:%d path:%s", pMount->mountName, pMount->mountDnodeId,
        pMount->mountPath);

  SEpSet epSet = mndGetDnodeEpset(pDnode);
  tmsgSendRecv(&epSet, &rpcMsg, &rpcRsp);
  if (rpcRsp.code == 0 && rpcRsp.pCont != NULL && rpcRsp.contLen > 0 &&
      tDeserializeSGetMountInfoRsp(rpcRsp.pCont, rpcRsp.contLen, pRsp) == 0) {
    mInfo("mount:%s, get-mount-info rsp is received, len:%d", pMount->mountName, pRsp->jsonLen);
    return 0;
  } else {
    mError("mount:%s, failed to send get-mount-info req to dnode since %s", pMount->mountName, tstrerror(rpcRsp.code));
    terrno = rpcRsp.code;
    return -1;
  }
}

static int32_t mndProcessCreateMountReq(SRpcMsg *pReq) {
  SMnode          *pMnode = pReq->info.node;
  int32_t          code = -1;
  SMountObj       *pMount = NULL;
  SUserObj        *pOperUser = NULL;
  SDnodeObj       *pDnode = NULL;
  SCreateMountReq  createReq = {0};
  SGetMountInfoRsp mountInfo = {0};

  if (tDeserializeSCreateMountReq(pReq->pCont, pReq->contLen, &createReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  mInfo("mount:%s, create mount req is received, dnode:%d path:%s", createReq.mountName, createReq.mountDnodeId,
        createReq.mountPath);
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
    terrno = TSDB_CODE_MND_MOUNT_NAME_ALREADY_EXIST;
    goto _OVER;
  }

  pMount = mndAcquireMountByPathAndDnode(pMnode, createReq.mountPath, createReq.mountDnodeId);
  if (pMount != NULL) {
    terrno = TSDB_CODE_MND_MOUNT_PATH_ALREADY_EXIST;
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

  if ((terrno = grantCheck(TSDB_GRANT_DB)) != 0) {
    code = terrno;
    goto _OVER;
  }

  mInfo("mount:%s, get mnode info", createReq.mountName);
  if (mndGetMountInfo(pMnode, pDnode, &createReq, &mountInfo) != 0) {
    goto _OVER;
  }

  mInfo("mount:%s, start to create", createReq.mountName);
  code = mndCreateMount(pMnode, pDnode, &createReq, &mountInfo, pReq);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("mount:%s, failed to create since %s", createReq.mountName, terrstr());
  }

  mndReleaseDnode(pMnode, pDnode);
  mndReleaseMount(pMnode, pMount);
  mndReleaseUser(pMnode, pOperUser);
  tFreeSGetMountInfoRsp(&mountInfo);

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

  for (int32_t db = 0; db < pMount->dbSize; ++db) {
    int64_t dbUid = pMount->dbUids[db];
    SDbObj *pDb = mndAcquireDbByUid(pMnode, dbUid);
    if (pDb == NULL) continue;

    mInfo("mount:%s, db:%s uid:%" PRId64 " will be dropped", pMount->name, pDb->name, dbUid);
    SSdbRaw *pDbRaw = mndDbActionEncode(pDb);
    if (pDbRaw == NULL) {
      mndReleaseDb(pMnode, pDb);
      return -1;
    }
    if (mndTransAppendCommitlog(pTrans, pDbRaw) != 0) {
      mndReleaseDb(pMnode, pDb);
      return -1;
    }
    if (sdbSetRawStatus(pDbRaw, SDB_STATUS_DROPPED) != 0) {
      mndReleaseDb(pMnode, pDb);
      return -1;
    }
    mndReleaseDb(pMnode, pDb);

    SSdb *pSdb = pMnode->pSdb;
    void *pIter = NULL;
    while (1) {
      SVgObj *pVgroup = NULL;
      pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
      if (pIter == NULL) break;

      if (pVgroup->dbUid == dbUid) {
        mInfo("mount:%s, db:%s vgId:%d will be dropped", pMount->name, pDb->name, pVgroup->vgId);

        SSdbRaw *pVgRaw = mndVgroupActionEncode(pVgroup);
        if (pVgRaw == NULL || mndTransAppendCommitlog(pTrans, pVgRaw) != 0) {
          sdbCancelFetch(pSdb, pIter);
          sdbRelease(pSdb, pVgroup);
          return -1;
        }
        (void)sdbSetRawStatus(pVgRaw, SDB_STATUS_DROPPED);
      }

      sdbRelease(pSdb, pVgroup);
    }

    while (1) {
      SStbObj *pStb = NULL;
      pIter = sdbFetch(pSdb, SDB_STB, pIter, (void **)&pStb);
      if (pIter == NULL) break;

      if (pStb->dbUid == dbUid) {
        mInfo("mount:%s, db:%s stb:%s uid:%" PRId64 " will be dropped", pMount->name, pDb->name, pStb->name, pStb->uid);

        SSdbRaw *pStbRaw = mndStbActionEncode(pStb);
        if (pStbRaw == NULL || mndTransAppendCommitlog(pTrans, pStbRaw) != 0) {
          sdbCancelFetch(pSdb, pIter);
          sdbRelease(pSdb, pStbRaw);
          return -1;
        }
        (void)sdbSetRawStatus(pStbRaw, SDB_STATUS_DROPPED);
      }

      sdbRelease(pSdb, pStb);
    }
  }

  if (mndAddSetMountInfoAction(pMnode, pTrans, pMount, false) != 0) {
    return -1;
  }

  return 0;
}

static int32_t mndGetMountSrcVgId(SMountObj *pMount, int32_t dstVgId) {
  for (int32_t i = 0; i < pMount->vgPairSize; ++i) {
    if (pMount->vgPairs[i].dstVgId == dstVgId) {
      return pMount->vgPairs[i].srcVgId;
    }
  }

  return -1;
}

static int32_t mndSetDropMountRedoActions(SMnode *pMnode, STrans *pTrans, SDnodeObj *pDnode, SMountObj *pMount) {
  SSdb *pSdb = pMnode->pSdb;
  void *pIter = NULL;

  for (int32_t db = 0; db < pMount->dbSize; ++db) {
    int64_t dbUid = pMount->dbUids[db];
    SDbObj *pDb = mndAcquireDbByUid(pMnode, dbUid);
    if (pDb == NULL) continue;

    while (1) {
      SVgObj *pVgroup = NULL;
      pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
      if (pIter == NULL) break;

      if (pVgroup->dbUid == pDb->uid) {
        for (int32_t vn = 0; vn < pVgroup->replica; ++vn) {
          SVnodeGid *pVgid = pVgroup->vnodeGid + vn;
          int32_t    srcVgId = mndGetMountSrcVgId(pMount, pVgroup->vgId);
          mInfo("mount:%s, vgId:%d will send unmount to dnode:%d, src vgId:%d", pMount->name, pVgroup->vgId,
                pVgid->dnodeId, srcVgId);
          if (srcVgId <= 1) {
            mInfo("mount:%s, vgId:%d failed send unmount to dnode:%d since src vgId:%d", pMount->name, pVgroup->vgId,
                  pVgid->dnodeId, srcVgId);
            terrno = TSDB_CODE_APP_ERROR;
            return -1;
          }

          if (mndAddUnMountVnodeAction(pMnode, pTrans, pMount, pDb, pVgroup, pVgid, srcVgId, 1) != 0) {
            sdbRelease(pSdb, pVgroup);
            sdbRelease(pSdb, pDb);
            return -1;
          }

          if (mndAddDropVnodeAction(pMnode, pTrans, pDb, pVgroup, pVgid, true) != 0) {
            sdbRelease(pSdb, pVgroup);
            sdbRelease(pSdb, pDb);
            return -1;
          }
        }
      }

      sdbRelease(pSdb, pVgroup);
    }
    sdbRelease(pSdb, pDb);
  }

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

  mInfo("mount:%s, drop mount req is received", dropReq.mountName);
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

  mInfo("mount:%s, start to drop", dropReq.mountName);
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

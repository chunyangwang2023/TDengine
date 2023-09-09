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
  SMountObj *pObj = sdbAcquire(pMnode->pSdb, SDB_MOUNT, mountName);
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
    dbObj.cfg.replications = 1;

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

int32_t mmdMountCheckClusterId(SMnode *pMnode, SJson *root) {
  int64_t cluster
  int32_t code = 0;
  SJson  *clusters = tjsonGetObjectItem(root, "clusters");
  if (clusters == NULL) return -1;

  int32_t num = (int32_t)cJSON_GetArraySize(clusters);
  for (int32_t i = 0; i < num; i++) {
    SJson *cluster = tjsonGetArrayItem(clusters, i);
    if (cluster == NULL) return -1;


    mountParseInt32(cluster, "vgId", clusterObj.vgId);
    mountParseInt64(cluster, "createdTime", clusterObj.createdTime);
    mountParseInt64(cluster, "updateTime", clusterObj.updateTime);
    mountParseString(cluster, "version", clusterObj.version);
  }

  return 0;
}

static int32_t mndCreateMount(SMnode *pMnode, SDnodeObj *pDnode, SCreateMountReq *pCreate, SGetMountInfoRsp *pInfo,
                              SRpcMsg *pReq) {
  int32_t code = -1;
  SArray *pDbs = taosArrayInit(2, sizeof(SDbObj));
  SArray *pVgrups = taosArrayInit(2, sizeof(SVgObj));
  SArray *pStbs = taosArrayInit(4, sizeof(SStbObj));
  SJson  *root = tjsonParse(json);

  if (root == NULL) {
    terrno = TSDB_CODE_INVALID_JSON_FORMAT;
    goto _OVER;
  }

  if (mmdMountCheckClusterId(pMnode, root) != 0) goto _OVER;
  if (mmdMountParseDbs(pDbs, root) != 0) goto _OVER;
  if (mmdMountParseVgroups(pDbs, root, pDnode->id) != 0) goto _OVER;
  if (mmdMountParseStbs(pDbs, root) != 0) goto _OVER;

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

  // check cluster Id


  // check mount path

  // parse json, return dbobj/stbobj/vgojb
  // add to redo/undo logs
  // add commint logs
  // add redo/undo actions

  if (mndSetCreateMountRedoLogs(pMnode, pTrans, &mountObj) != 0) goto _OVER;
  if (mndSetCreateMountUndoLogs(pMnode, pTrans, &mountObj) != 0) goto _OVER;
  if (mndSetCreateMountRedoActions(pMnode, pTrans, pDnode, &mountObj) != 0) goto _OVER;
  if (mndSetCreateMountUndoActions(pMnode, pTrans, pDnode, &mountObj) != 0) goto _OVER;
  if (mndSetCreateMountCommitLogs(pMnode, pTrans, &mountObj) != 0) goto _OVER;
  if (mndTransPrepare(pMnode, pTrans) != 0) goto _OVER;

  code = 0;

_OVER:
  mndTransDrop(pTrans);

  if (root != NULL) cJSON_Delete(root);
  if (pDbs != NULL) taosArrayDestroy(pDbs);
  if (pStbs != NULL) taosArrayDestroy(pStbs);
  if (pVgrups != NULL) taosArrayDestroy(pVgrups);
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
      .msgType = TDMT_MND_GET_MOUNT_INFO,
      .info.ahandle = (void *)0x9537,
      .info.refId = 0,
      .info.noResp = 0,
  };
  SRpcMsg rpcRsp = {0};

  mInfo("mount:%s, send get-mount-info req to dnode:%d path:%s", pMount->mountName, pMount->mountDnodeId,
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

  if (mndGetMountInfo(pMnode, pDnode, &createReq, &mountInfo) != 0) {
    goto _OVER;
  }

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

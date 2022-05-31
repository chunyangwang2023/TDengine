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

#include "planInt.h"

#define SPLIT_FLAG_MASK(n) (1 << n)

#define SPLIT_FLAG_STABLE_SPLIT SPLIT_FLAG_MASK(0)

#define SPLIT_FLAG_SET_MASK(val, mask)  (val) |= (mask)
#define SPLIT_FLAG_TEST_MASK(val, mask) (((val) & (mask)) != 0)

typedef struct SSplitContext {
  uint64_t queryId;
  int32_t  groupId;
  bool     split;
} SSplitContext;

typedef int32_t (*FSplit)(SSplitContext* pCxt, SLogicSubplan* pSubplan);

typedef struct SSplitRule {
  char*  pName;
  FSplit splitFunc;
} SSplitRule;

typedef bool (*FSplFindSplitNode)(SLogicSubplan* pSubplan, void* pInfo);

static SLogicSubplan* splCreateSubplan(SSplitContext* pCxt, SLogicNode* pNode, int32_t flag) {
  SLogicSubplan* pSubplan = nodesMakeNode(QUERY_NODE_LOGIC_SUBPLAN);
  if (NULL == pSubplan) {
    return NULL;
  }
  pSubplan->id.queryId = pCxt->queryId;
  pSubplan->id.groupId = pCxt->groupId;
  pSubplan->subplanType = SUBPLAN_TYPE_SCAN;
  pSubplan->pNode = (SLogicNode*)nodesCloneNode(pNode);
  if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pNode)) {
    TSWAP(pSubplan->pVgroupList, ((SScanLogicNode*)pSubplan->pNode)->pVgroupList);
  }
  SPLIT_FLAG_SET_MASK(pSubplan->splitFlag, flag);
  return pSubplan;
}

static int32_t splCreateExchangeNode(SSplitContext* pCxt, SLogicSubplan* pSubplan, SLogicNode* pSplitNode,
                                     ESubplanType subplanType) {
  SExchangeLogicNode* pExchange = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_EXCHANGE);
  if (NULL == pExchange) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pExchange->srcGroupId = pCxt->groupId;
  pExchange->precision = pSplitNode->precision;
  pExchange->node.pTargets = nodesCloneList(pSplitNode->pTargets);
  if (NULL == pExchange->node.pTargets) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pSubplan->subplanType = subplanType;

  if (NULL == pSplitNode->pParent) {
    pSubplan->pNode = (SLogicNode*)pExchange;
    return TSDB_CODE_SUCCESS;
  }

  SNode* pNode;
  FOREACH(pNode, pSplitNode->pParent->pChildren) {
    if (nodesEqualNode(pNode, pSplitNode)) {
      REPLACE_NODE(pExchange);
      nodesDestroyNode(pNode);
      return TSDB_CODE_SUCCESS;
    }
  }
  nodesDestroyNode(pExchange);
  return TSDB_CODE_FAILED;
}

static bool splMatch(SSplitContext* pCxt, SLogicSubplan* pSubplan, int32_t flag, FSplFindSplitNode func, void* pInfo) {
  if (!SPLIT_FLAG_TEST_MASK(pSubplan->splitFlag, flag)) {
    if (func(pSubplan, pInfo)) {
      return true;
    }
  }
  SNode* pChild;
  FOREACH(pChild, pSubplan->pChildren) {
    if (splMatch(pCxt, (SLogicSubplan*)pChild, flag, func, pInfo)) {
      return true;
    }
  }
  return false;
}

typedef struct SStableSplitInfo {
  SScanLogicNode* pScan;
  SLogicSubplan*  pSubplan;
} SStableSplitInfo;

static SLogicNode* stbSplMatchByNode(SLogicNode* pNode) {
  if (QUERY_NODE_LOGIC_PLAN_SCAN == nodeType(pNode) && NULL != ((SScanLogicNode*)pNode)->pVgroupList &&
      ((SScanLogicNode*)pNode)->pVgroupList->numOfVgroups > 1) {
    return pNode;
  }
  SNode* pChild;
  FOREACH(pChild, pNode->pChildren) {
    SLogicNode* pSplitNode = stbSplMatchByNode((SLogicNode*)pChild);
    if (NULL != pSplitNode) {
      return pSplitNode;
    }
  }
  return NULL;
}

static bool stbSplFindSplitNode(SLogicSubplan* pSubplan, SStableSplitInfo* pInfo) {
  SLogicNode* pSplitNode = stbSplMatchByNode(pSubplan->pNode);
  if (NULL != pSplitNode) {
    pInfo->pScan = (SScanLogicNode*)pSplitNode;
    pInfo->pSubplan = pSubplan;
  }
  return NULL != pSplitNode;
}

static int32_t stableSplit(SSplitContext* pCxt, SLogicSubplan* pSubplan) {
  SStableSplitInfo info = {0};
  if (!splMatch(pCxt, pSubplan, SPLIT_FLAG_STABLE_SPLIT, (FSplFindSplitNode)stbSplFindSplitNode, &info)) {
    return TSDB_CODE_SUCCESS;
  }
  int32_t code = nodesListMakeStrictAppend(&info.pSubplan->pChildren,
                                           splCreateSubplan(pCxt, (SLogicNode*)info.pScan, SPLIT_FLAG_STABLE_SPLIT));
  if (TSDB_CODE_SUCCESS == code) {
    code = splCreateExchangeNode(pCxt, info.pSubplan, (SLogicNode*)info.pScan, SUBPLAN_TYPE_MERGE);
  }
  ++(pCxt->groupId);
  pCxt->split = true;
  return code;
}

typedef struct SSigTbJoinSplitInfo {
  SJoinLogicNode* pJoin;
  SLogicNode*     pSplitNode;
  SLogicSubplan*  pSubplan;
} SSigTbJoinSplitInfo;

static bool sigTbJoinSplNeedSplit(SJoinLogicNode* pJoin) {
  if (!pJoin->isSingleTableJoin) {
    return false;
  }
  return QUERY_NODE_LOGIC_PLAN_EXCHANGE != nodeType(nodesListGetNode(pJoin->node.pChildren, 0)) &&
         QUERY_NODE_LOGIC_PLAN_EXCHANGE != nodeType(nodesListGetNode(pJoin->node.pChildren, 1));
}

static SJoinLogicNode* sigTbJoinSplMatchByNode(SLogicNode* pNode) {
  if (QUERY_NODE_LOGIC_PLAN_JOIN == nodeType(pNode) && sigTbJoinSplNeedSplit((SJoinLogicNode*)pNode)) {
    return (SJoinLogicNode*)pNode;
  }
  SNode* pChild;
  FOREACH(pChild, pNode->pChildren) {
    SJoinLogicNode* pSplitNode = sigTbJoinSplMatchByNode((SLogicNode*)pChild);
    if (NULL != pSplitNode) {
      return pSplitNode;
    }
  }
  return NULL;
}

static bool sigTbJoinSplFindSplitNode(SLogicSubplan* pSubplan, SSigTbJoinSplitInfo* pInfo) {
  SJoinLogicNode* pJoin = sigTbJoinSplMatchByNode(pSubplan->pNode);
  if (NULL != pJoin) {
    pInfo->pJoin = pJoin;
    pInfo->pSplitNode = nodesListGetNode(pJoin->node.pChildren, 1);
    pInfo->pSubplan = pSubplan;
  }
  return NULL != pJoin;
}

static int32_t singleTableJoinSplit(SSplitContext* pCxt, SLogicSubplan* pSubplan) {
  SSigTbJoinSplitInfo info = {0};
  if (!splMatch(pCxt, pSubplan, 0, (FSplFindSplitNode)sigTbJoinSplFindSplitNode, &info)) {
    return TSDB_CODE_SUCCESS;
  }
  int32_t code = nodesListMakeStrictAppend(&info.pSubplan->pChildren, splCreateSubplan(pCxt, info.pSplitNode, 0));
  if (TSDB_CODE_SUCCESS == code) {
    code = splCreateExchangeNode(pCxt, info.pSubplan, info.pSplitNode, info.pSubplan->subplanType);
  }
  ++(pCxt->groupId);
  pCxt->split = true;
  return code;
}

static bool unionIsChildSubplan(SLogicNode* pLogicNode, int32_t groupId) {
  if (QUERY_NODE_LOGIC_PLAN_EXCHANGE == nodeType(pLogicNode)) {
    return ((SExchangeLogicNode*)pLogicNode)->srcGroupId == groupId;
  }

  SNode* pChild;
  FOREACH(pChild, pLogicNode->pChildren) {
    bool isChild = unionIsChildSubplan((SLogicNode*)pChild, groupId);
    if (isChild) {
      return isChild;
    }
  }
  return false;
}

static int32_t unionMountSubplan(SLogicSubplan* pParent, SNodeList* pChildren) {
  SNode* pChild = NULL;
  WHERE_EACH(pChild, pChildren) {
    if (unionIsChildSubplan(pParent->pNode, ((SLogicSubplan*)pChild)->id.groupId)) {
      int32_t code = nodesListMakeAppend(&pParent->pChildren, pChild);
      if (TSDB_CODE_SUCCESS == code) {
        REPLACE_NODE(NULL);
        ERASE_NODE(pChildren);
        continue;
      } else {
        return code;
      }
    }
    WHERE_NEXT;
  }
  return TSDB_CODE_SUCCESS;
}

static SLogicSubplan* unionCreateSubplan(SSplitContext* pCxt, SLogicNode* pNode) {
  SLogicSubplan* pSubplan = nodesMakeNode(QUERY_NODE_LOGIC_SUBPLAN);
  if (NULL == pSubplan) {
    return NULL;
  }
  pSubplan->id.queryId = pCxt->queryId;
  pSubplan->id.groupId = pCxt->groupId;
  pSubplan->subplanType = SUBPLAN_TYPE_SCAN;
  pSubplan->pNode = pNode;
  pNode->pParent = NULL;
  return pSubplan;
}

static int32_t unionSplitSubplan(SSplitContext* pCxt, SLogicSubplan* pUnionSubplan, SLogicNode* pSplitNode) {
  SNodeList* pSubplanChildren = pUnionSubplan->pChildren;
  pUnionSubplan->pChildren = NULL;

  int32_t code = TSDB_CODE_SUCCESS;

  SNode* pChild = NULL;
  FOREACH(pChild, pSplitNode->pChildren) {
    SLogicSubplan* pNewSubplan = unionCreateSubplan(pCxt, (SLogicNode*)pChild);
    code = nodesListMakeStrictAppend(&pUnionSubplan->pChildren, pNewSubplan);
    if (TSDB_CODE_SUCCESS == code) {
      REPLACE_NODE(NULL);
      code = unionMountSubplan(pNewSubplan, pSubplanChildren);
    }
    if (TSDB_CODE_SUCCESS != code) {
      break;
    }
  }
  if (TSDB_CODE_SUCCESS == code) {
    nodesDestroyList(pSubplanChildren);
    DESTORY_LIST(pSplitNode->pChildren);
  }
  return code;
}

typedef struct SUnionAllSplitInfo {
  SProjectLogicNode* pProject;
  SLogicSubplan*     pSubplan;
} SUnionAllSplitInfo;

static SLogicNode* unionAllMatchByNode(SLogicNode* pNode) {
  if (QUERY_NODE_LOGIC_PLAN_PROJECT == nodeType(pNode) && LIST_LENGTH(pNode->pChildren) > 1) {
    return pNode;
  }
  SNode* pChild;
  FOREACH(pChild, pNode->pChildren) {
    SLogicNode* pSplitNode = unionAllMatchByNode((SLogicNode*)pChild);
    if (NULL != pSplitNode) {
      return pSplitNode;
    }
  }
  return NULL;
}

static bool unionAllFindSplitNode(SLogicSubplan* pSubplan, SUnionAllSplitInfo* pInfo) {
  SLogicNode* pSplitNode = unionAllMatchByNode(pSubplan->pNode);
  if (NULL != pSplitNode) {
    pInfo->pProject = (SProjectLogicNode*)pSplitNode;
    pInfo->pSubplan = pSubplan;
  }
  return NULL != pSplitNode;
}

static int32_t unionAllCreateExchangeNode(SSplitContext* pCxt, SLogicSubplan* pSubplan, SProjectLogicNode* pProject) {
  SExchangeLogicNode* pExchange = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_EXCHANGE);
  if (NULL == pExchange) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pExchange->srcGroupId = pCxt->groupId;
  pExchange->precision = pProject->node.precision;
  pExchange->node.pTargets = nodesCloneList(pProject->node.pTargets);
  if (NULL == pExchange->node.pTargets) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pSubplan->subplanType = SUBPLAN_TYPE_MERGE;

  if (NULL == pProject->node.pParent) {
    pSubplan->pNode = (SLogicNode*)pExchange;
    nodesDestroyNode(pProject);
    return TSDB_CODE_SUCCESS;
  }

  SNode* pNode;
  FOREACH(pNode, pProject->node.pParent->pChildren) {
    if (nodesEqualNode(pNode, pProject)) {
      REPLACE_NODE(pExchange);
      nodesDestroyNode(pNode);
      return TSDB_CODE_SUCCESS;
    }
  }
  nodesDestroyNode(pExchange);
  return TSDB_CODE_FAILED;
}

static int32_t unionAllSplit(SSplitContext* pCxt, SLogicSubplan* pSubplan) {
  SUnionAllSplitInfo info = {0};
  if (!splMatch(pCxt, pSubplan, 0, (FSplFindSplitNode)unionAllFindSplitNode, &info)) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = unionSplitSubplan(pCxt, info.pSubplan, (SLogicNode*)info.pProject);
  if (TSDB_CODE_SUCCESS == code) {
    code = unionAllCreateExchangeNode(pCxt, info.pSubplan, info.pProject);
  }
  ++(pCxt->groupId);
  pCxt->split = true;
  return code;
}

typedef struct SUnionDistinctSplitInfo {
  SAggLogicNode* pAgg;
  SLogicSubplan* pSubplan;
} SUnionDistinctSplitInfo;

static SLogicNode* unionDistinctMatchByNode(SLogicNode* pNode) {
  if (QUERY_NODE_LOGIC_PLAN_AGG == nodeType(pNode) && LIST_LENGTH(pNode->pChildren) > 1) {
    return pNode;
  }
  SNode* pChild;
  FOREACH(pChild, pNode->pChildren) {
    SLogicNode* pSplitNode = unionDistinctMatchByNode((SLogicNode*)pChild);
    if (NULL != pSplitNode) {
      return pSplitNode;
    }
  }
  return NULL;
}

static int32_t unCreateExchangeNode(SSplitContext* pCxt, SLogicSubplan* pSubplan, SAggLogicNode* pAgg) {
  SExchangeLogicNode* pExchange = nodesMakeNode(QUERY_NODE_LOGIC_PLAN_EXCHANGE);
  if (NULL == pExchange) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pExchange->srcGroupId = pCxt->groupId;
  // pExchange->precision = pScan->pMeta->tableInfo.precision;
  pExchange->node.pTargets = nodesCloneList(pAgg->pGroupKeys);
  if (NULL == pExchange->node.pTargets) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pSubplan->subplanType = SUBPLAN_TYPE_MERGE;

  return nodesListMakeAppend(&pAgg->node.pChildren, pExchange);
}

static bool unionDistinctFindSplitNode(SLogicSubplan* pSubplan, SUnionDistinctSplitInfo* pInfo) {
  SLogicNode* pSplitNode = unionDistinctMatchByNode(pSubplan->pNode);
  if (NULL != pSplitNode) {
    pInfo->pAgg = (SAggLogicNode*)pSplitNode;
    pInfo->pSubplan = pSubplan;
  }
  return NULL != pSplitNode;
}

static int32_t unionDistinctSplit(SSplitContext* pCxt, SLogicSubplan* pSubplan) {
  SUnionDistinctSplitInfo info = {0};
  if (!splMatch(pCxt, pSubplan, 0, (FSplFindSplitNode)unionDistinctFindSplitNode, &info)) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = unionSplitSubplan(pCxt, info.pSubplan, (SLogicNode*)info.pAgg);
  if (TSDB_CODE_SUCCESS == code) {
    code = unCreateExchangeNode(pCxt, info.pSubplan, info.pAgg);
  }
  ++(pCxt->groupId);
  pCxt->split = true;
  return code;
}

// clang-format off
static const SSplitRule splitRuleSet[] = {
  {.pName = "SuperTableSplit",      .splitFunc = stableSplit},
  {.pName = "SingleTableJoinSplit", .splitFunc = singleTableJoinSplit},
  {.pName = "UnionAllSplit",        .splitFunc = unionAllSplit},
  {.pName = "UnionDistinctSplit",   .splitFunc = unionDistinctSplit}
};
// clang-format on

static const int32_t splitRuleNum = (sizeof(splitRuleSet) / sizeof(SSplitRule));

static void dumpLogicSubplan(const char* pRuleName, SLogicSubplan* pSubplan) {
  char* pStr = NULL;
  nodesNodeToString(pSubplan, false, &pStr, NULL);
  qDebugL("apply %s rule: %s", pRuleName, pStr);
  taosMemoryFree(pStr);
}

static int32_t applySplitRule(SLogicSubplan* pSubplan) {
  SSplitContext cxt = {.queryId = pSubplan->id.queryId, .groupId = pSubplan->id.groupId + 1, .split = false};
  bool          split = false;
  do {
    split = false;
    for (int32_t i = 0; i < splitRuleNum; ++i) {
      cxt.split = false;
      int32_t code = splitRuleSet[i].splitFunc(&cxt, pSubplan);
      if (TSDB_CODE_SUCCESS != code) {
        return code;
      }
      if (cxt.split) {
        split = true;
        dumpLogicSubplan(splitRuleSet[i].pName, pSubplan);
      }
    }
  } while (split);
  return TSDB_CODE_SUCCESS;
}

static void doSetLogicNodeParent(SLogicNode* pNode, SLogicNode* pParent) {
  pNode->pParent = pParent;
  SNode* pChild;
  FOREACH(pChild, pNode->pChildren) { doSetLogicNodeParent((SLogicNode*)pChild, pNode); }
}

static void setLogicNodeParent(SLogicNode* pNode) { doSetLogicNodeParent(pNode, NULL); }

int32_t splitLogicPlan(SPlanContext* pCxt, SLogicNode* pLogicNode, SLogicSubplan** pLogicSubplan) {
  SLogicSubplan* pSubplan = (SLogicSubplan*)nodesMakeNode(QUERY_NODE_LOGIC_SUBPLAN);
  if (NULL == pSubplan) {
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pSubplan->pNode = nodesCloneNode(pLogicNode);
  if (NULL == pSubplan->pNode) {
    nodesDestroyNode(pSubplan);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  if (QUERY_NODE_LOGIC_PLAN_VNODE_MODIF == nodeType(pLogicNode)) {
    pSubplan->subplanType = SUBPLAN_TYPE_MODIFY;
    TSWAP(((SVnodeModifLogicNode*)pLogicNode)->pDataBlocks, ((SVnodeModifLogicNode*)pSubplan->pNode)->pDataBlocks);
  } else {
    pSubplan->subplanType = SUBPLAN_TYPE_SCAN;
  }
  pSubplan->id.queryId = pCxt->queryId;
  pSubplan->id.groupId = 1;
  setLogicNodeParent(pSubplan->pNode);

  int32_t code = applySplitRule(pSubplan);
  if (TSDB_CODE_SUCCESS == code) {
    *pLogicSubplan = pSubplan;
  } else {
    nodesDestroyNode(pSubplan);
  }

  return code;
}
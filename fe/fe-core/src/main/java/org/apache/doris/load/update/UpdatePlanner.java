// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.load.update;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.common.UserException;
import org.apache.doris.planner.DataPartition;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.OlapTableSink;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.Planner;
import org.apache.doris.planner.ScanNode;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

import static org.apache.doris.alter.SchemaChangeHandler.SHADOW_NAME_PRFIX;


public class UpdatePlanner extends Planner {

    private final IdGenerator<PlanNodeId> nodeIdGenerator_ = PlanNodeId.createGenerator();
    private final IdGenerator<PlanFragmentId> fragmentIdGenerator_ =
            PlanFragmentId.createGenerator();

    private long targetDBId;
    private OlapTable targetTable;
    private List<Expr> setExprs;
    private TupleDescriptor srcTupleDesc;
    private Analyzer analyzer;

    private List<ScanNode> scanNodeList = Lists.newArrayList();

    public UpdatePlanner(long dbId, OlapTable targetTable, List<Expr> setExprs,
                         TupleDescriptor srcTupleDesc, Analyzer analyzer) {
        this.targetDBId = dbId;
        this.targetTable = targetTable;
        this.setExprs = setExprs;
        this.srcTupleDesc = srcTupleDesc;
        this.analyzer = analyzer;
    }

    @Override
    public List<ScanNode> getScanNodes() {
        return scanNodeList;
    }

    public void plan(long txnId) throws UserException {
        // 1. gen scan node
        OlapScanNode olapScanNode = new OlapScanNode(nodeIdGenerator_.getNextId(), srcTupleDesc, "OlapScanNode");
        /* BEGIN: Temporary code, this part of the code needs to be refactored */
        olapScanNode.closePreAggregation("This an update operation");
        olapScanNode.useBaseIndexId();
        /* END */
        olapScanNode.init(analyzer);
        olapScanNode.finalize(analyzer);
        scanNodeList.add(olapScanNode);
        // 2. gen olap table sink
        OlapTableSink olapTableSink = new OlapTableSink(targetTable, computeTargetTupleDesc(), null);
        olapTableSink.init(analyzer.getContext().queryId(), txnId, targetDBId,
                analyzer.getContext().getSessionVariable().queryTimeoutS);
        olapTableSink.complete();
        // 3. gen plan fragment
        PlanFragment planFragment = new PlanFragment(fragmentIdGenerator_.getNextId(), olapScanNode,
                DataPartition.RANDOM);
        planFragment.setSink(olapTableSink);
        planFragment.setOutputExprs(computeOutputExprs());
        planFragment.finalize(analyzer, false);
        fragments.add(planFragment);
    }

    private TupleDescriptor computeTargetTupleDesc() {
        DescriptorTable descTable = analyzer.getDescTbl();
        TupleDescriptor targetTupleDesc = descTable.createTupleDescriptor();
        for (Column col : targetTable.getFullSchema()) {
            SlotDescriptor slotDesc = descTable.addSlotDescriptor(targetTupleDesc);
            slotDesc.setIsMaterialized(true);
            slotDesc.setType(col.getType());
            slotDesc.setColumn(col);
            if (col.isAllowNull()) {
                slotDesc.setIsNullable(true);
            } else {
                slotDesc.setIsNullable(false);
            }
        }
        targetTupleDesc.computeStatAndMemLayout();
        return targetTupleDesc;
    }

    private List<Expr> computeOutputExprs() {
        Map<String, Expr> columnNameToSetExpr = Maps.newHashMap();
        for (Expr setExpr : setExprs) {
            Preconditions.checkState(setExpr instanceof BinaryPredicate);
            Preconditions.checkState(setExpr.getChild(0) instanceof SlotRef);
            SlotRef slotRef = (SlotRef) setExpr.getChild(0);
            // pay attention to case ignore of column name
            columnNameToSetExpr.put(slotRef.getColumnName().toLowerCase(), setExpr.getChild(1));
        }
        Map<String, SlotDescriptor> columnNameToSrcSlotDesc = Maps.newHashMap();
        for (SlotDescriptor srcSlotDesc : srcTupleDesc.getSlots()) {
            // pay attention to case ignore of column name
            columnNameToSrcSlotDesc.put(srcSlotDesc.getColumn().getName().toLowerCase(), srcSlotDesc);
        }

        // compute output expr
        List<Expr> outputExprs = Lists.newArrayList();
        for (int i = 0; i < targetTable.getFullSchema().size(); i++) {
            Column column = targetTable.getFullSchema().get(i);
            // pay attention to case ignore of column name
            String originColumnName = (column.getName().startsWith(SHADOW_NAME_PRFIX) ?
                    column.getName().substring(SHADOW_NAME_PRFIX.length()) : column.getName())
                    .toLowerCase();
            Expr setExpr = columnNameToSetExpr.get(originColumnName);
            if (setExpr == null) {
                SlotDescriptor srcSlotDesc = columnNameToSrcSlotDesc.get(originColumnName);
                Preconditions.checkState(srcSlotDesc != null, "Column not found:" + originColumnName);
                SlotRef slotRef = new SlotRef(srcSlotDesc);
                outputExprs.add(slotRef);
            } else {
                outputExprs.add(setExpr);
            }
        }
        return outputExprs;
    }
}

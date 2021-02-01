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

package org.apache.doris.analysis;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * UPDATE is a DML statement that modifies rows in a table.
 * The current update syntax only supports updating the filtered data of a single table.
 *
 * UPDATE table_reference
 *     SET assignment_list
 *     [WHERE where_condition]
 *
 * value:
 *     {expr}
 *
 * assignment:
 *     col_name = value
 *
 * assignment_list:
 *     assignment [, assignment] ...
 */
public class UpdateStmt implements ParseNode {

    private final TableName tableName;
    private final List<Expr> setExprs;
    private final Expr whereExpr;

    public UpdateStmt(TableName tableName, List<Expr> setExprs, Expr whereExpr) {
        this.tableName = tableName;
        this.setExprs = setExprs;
        this.whereExpr = whereExpr;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        tableName.analyze(analyzer);
        Set<String> columnMappingNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        // the column expr only support binary predicate which's child(0) must be a SloRef.
        // the duplicate column name of SloRef is forbidden.
        for (Expr setExpr: setExprs) {
            if (!(setExpr instanceof BinaryPredicate)) {
                throw new AnalysisException("Set function expr only support eq binary predicate. "
                        + "Expr: " + setExpr.toSql());
            }
            BinaryPredicate predicate = (BinaryPredicate) setExpr;
            if (predicate.getOp() != BinaryPredicate.Operator.EQ) {
                throw new AnalysisException("Set function expr only support eq binary predicate. "
                        + "The predicate operator error, op: " + predicate.getOp());
            }
            Expr lhs = predicate.getChild(0);
            if (!(lhs instanceof SlotRef)) {
                throw new AnalysisException("Set function expr only support eq binary predicate "
                        + "which's child(0) must be a column name. "
                        + "The child(0) expr error. expr: " + lhs.toSql());
            }
            String column = ((SlotRef) lhs).getColumnName();
            if (!columnMappingNames.add(column)) {
                throw new AnalysisException("Duplicate column setting: " + column);
            }
        }
        whereExpr.analyze(analyzer);
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder("UPDATE ");
        sb.append(tableName.toSql()).append("\n");
        sb.append("  ").append("SET ");
        for (Expr setExpr: setExprs) {
            sb.append(setExpr.toSql()).append(", ");
        }
        sb.append("\n");
        if (whereExpr!=null) {
            sb.append("  ").append("WHERE ").append(whereExpr.toSql());
        }
        return sb.toString();
    }
}

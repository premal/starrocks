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

package com.starrocks.sql;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.planner.PartitionColumnFilter;
import com.starrocks.sql.ast.IndexDef;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.ColumnFilterConverter;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class ShortCircuitPlannerHybrid {

    public static class LogicalPlanChecker extends ShortCircuitPlanner.LogicalPlanChecker {

        public LogicalPlanChecker(boolean allowFilter, boolean allowLimit, boolean allowProject, boolean allowSort,
                                  ScalarOperator predicate, List<String> orderByColumns, long limit) {
            super(allowFilter, allowLimit, allowProject, allowSort, predicate, orderByColumns, limit);
        }

        @Override
        public Boolean visitLogicalTableScan(OptExpression optExpression, Void context) {
            LogicalScanOperator scanOp = optExpression.getOp().cast();
            Table table = scanOp.getTable();
            if (!(table instanceof OlapTable)) {
                return false;
            }

            OlapTable olapTable = (OlapTable) table;
            List<ScalarOperator> conjuncts = Utils.extractConjuncts(predicate);
            // LIMIT reaches the scan check either via the context (a LIMIT
            // node) or folded into the scan operator; -1 means no LIMIT seen
            long maxReturnRows = -1;
            if (shortCircuitContext.getMaxReturnRows() != 0) {
                maxReturnRows = shortCircuitContext.getMaxReturnRows();
            } else if (scanOp.hasLimit()) {
                maxReturnRows = scanOp.getLimit();
            }
            if (KeysType.PRIMARY_KEYS.equals(olapTable.getKeysType())) {
                List<String> keyColumns = olapTable.getKeyColumns().stream().map(Column::getName).collect(
                        Collectors.toList());
                if (isPointScan(table, keyColumns, conjuncts, maxReturnRows)) {
                    return true;
                }
            }

            // a pk point read is bounded by key count anyway, but a secondary
            // =/IN match is not, so any LIMIT must run through the normal path
            if (maxReturnRows >= 0) {
                return false;
            }
            return isSecondaryIndexScan(olapTable, conjuncts, shortCircuitContext);
        }

        /**
         * Secondary-index point scan: every conjunct is an =/IN literal filter on a
         * bitmap-indexed column, so the backend can resolve rowids through the index
         * inside the bounded short-circuit path instead of the full pipeline.
         */
        protected static boolean isSecondaryIndexScan(OlapTable table,
                                                      List<ScalarOperator> conjuncts,
                                                      ShortCircuitPlanner.ShortCircuitContext shortCircuitContext) {
            // shared-data tablets are not in the local tablet manager, so the
            // short-circuit executor could not resolve them even if the gate passed
            if (table.isCloudNativeTable() || conjuncts.isEmpty()) {
                return false;
            }
            Map<String, PartitionColumnFilter> filters = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            filters.putAll(ColumnFilterConverter.convertColumnFilter(conjuncts, table));
            // a conjunct that maps to no column filter (function, OR, non-column ref)
            // would silently run unindexed here, so require full coverage
            if (filters.size() != conjuncts.size()) {
                return false;
            }

            Set<String> bitmapColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            for (Index index : table.getIndexes()) {
                if (index.getIndexType() == IndexDef.IndexType.BITMAP) {
                    index.getColumns().forEach(columnId -> bitmapColumns.add(columnId.getId()));
                }
            }
            if (bitmapColumns.isEmpty()) {
                return false;
            }

            long cardinality = 1;
            for (Map.Entry<String, PartitionColumnFilter> entry : filters.entrySet()) {
                if (!bitmapColumns.contains(entry.getKey())) {
                    return false;
                }
                PartitionColumnFilter filter = entry.getValue();
                if (filter.getInPredicateLiterals() != null) {
                    cardinality *= filter.getInPredicateLiterals().size();
                    if (cardinality > ShortCircuitPlanner.MAX_RETURN_ROWS ||
                            (shortCircuitContext.getMaxReturnRows() != 0 &&
                                    cardinality > shortCircuitContext.getMaxReturnRows())) {
                        return false;
                    }
                } else if (!filter.isPoint()) {
                    return false;
                }
            }
            return true;
        }
    }
}

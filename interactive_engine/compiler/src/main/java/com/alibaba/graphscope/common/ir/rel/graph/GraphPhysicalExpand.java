/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.common.ir.rel.graph;

import com.alibaba.graphscope.common.ir.rel.GraphRelVisitor;
import com.alibaba.graphscope.common.ir.rel.type.AliasNameWithId;
import com.alibaba.graphscope.common.ir.rel.type.TableConfig;
import com.alibaba.graphscope.common.ir.tools.config.GraphOpt;

import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class GraphPhysicalExpand extends GraphLogicalExpand {
    private final GraphOpt.PhysicalExpandOpt physicalOpt;
    private final RelDataType rowType;

    protected GraphPhysicalExpand(
            GraphOptCluster cluster,
            List<RelHint> hints,
            RelNode input,
            GraphOpt.Expand opt,
            TableConfig tableConfig,
            @Nullable String alias,
            AliasNameWithId startAlias,
            RelDataType rowType,
            GraphOpt.PhysicalExpandOpt physicalOpt) {
        super(cluster, hints, input, opt, tableConfig, alias, startAlias);
        this.physicalOpt = physicalOpt;
        this.rowType = rowType;
    }

    public static GraphPhysicalExpand create(
            RelNode input,
            GraphLogicalExpand innerExpand,
            GraphLogicalGetV innerGetV,
            @Nullable String aliasName,
            GraphOpt.PhysicalExpandOpt physicalOpt) {
        RelDataType rowType;
        if (innerGetV.getAliasName().equals(aliasName)) {
            rowType = innerGetV.deriveRowType();
        } else {
            GraphLogicalGetV newGetV =
                    GraphLogicalGetV.create(
                            (GraphOptCluster) innerGetV.getCluster(),
                            innerGetV.getHints(),
                            innerGetV.getInput(0),
                            innerGetV.getOpt(),
                            innerGetV.getTableConfig(),
                            aliasName,
                            innerGetV.getStartAlias());
            rowType = newGetV.deriveRowType();
        }
        return new GraphPhysicalExpand(
                (GraphOptCluster) innerExpand.getCluster(),
                innerExpand.getHints(),
                input,
                innerExpand.getOpt(),
                innerExpand.getTableConfig(),
                aliasName,
                innerExpand.getStartAlias(),
                rowType,
                physicalOpt);
    }

    public GraphOpt.PhysicalExpandOpt getPhysicalOpt() {
        return this.physicalOpt;
    }

    @Override
    public RelDataType deriveRowType() {
        return rowType;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("physicalOpt", getPhysicalOpt());
    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        if (shuttle instanceof GraphRelVisitor) {
            return ((GraphRelVisitor) shuttle).visit(this);
        }
        return shuttle.visit(this);
    }
}

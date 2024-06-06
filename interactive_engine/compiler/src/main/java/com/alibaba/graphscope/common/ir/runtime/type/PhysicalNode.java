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

package com.alibaba.graphscope.common.ir.runtime.type;

import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * a wrapper of node (i.e. {@link com.sun.jna.Pointer} for FFI interfaces) in ir_core
 * @param <T>
 */
public class PhysicalNode<T> extends AbstractRelNode {
    private final RelNode original;
    private final List<T> nodes;

    protected PhysicalNode(
            RelOptCluster cluster, RelTraitSet traitSet, RelNode original, List<T> nodes) {
        super(cluster, traitSet);
        this.original = Objects.requireNonNull(original);
        this.nodes = com.alibaba.graphscope.common.ir.tools.Utils.requireNonEmpty(nodes);
    }

    public PhysicalNode(RelNode original, T node) {
        this(original.getCluster(), RelTraitSet.createEmpty(), original, ImmutableList.of(node));
    }

    public PhysicalNode(RelNode original, List<T> nodes) {
        this(original.getCluster(), RelTraitSet.createEmpty(), original, nodes);
    }

    public RelNode getOriginal() {
        return original;
    }

    public T getNode() {
        return nodes.get(0);
    }

    public List<T> getNodes() {
        return Collections.unmodifiableList(nodes);
    }
}

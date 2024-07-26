/*
 *
 *  * Copyright 2020 Alibaba Group Holding Limited.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.alibaba.graphscope.common.ir.type;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class UnionVELabelTypes extends GraphLabelType {
    private final List<GraphLabelType> unionTypes;

    public UnionVELabelTypes(GraphLabelType expandLabelType, GraphLabelType getVLabelType) {
        super(expandLabelType.getLabelsEntry());
        Preconditions.checkArgument(
                !(expandLabelType instanceof UnionVELabelTypes)
                        && !(getVLabelType instanceof UnionVELabelTypes));
        this.unionTypes = ImmutableList.of(expandLabelType, getVLabelType);
    }

    public GraphLabelType getLabelType(int index) {
        return this.unionTypes.get(index);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        UnionVELabelTypes that = (UnionVELabelTypes) o;
        return Objects.equal(unionTypes, that.unionTypes);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), unionTypes);
    }

    @Override
    protected void generateTypeString(StringBuilder sb, boolean withDetail) {
        sb.append("UNION_V_E(" + unionTypes + ")");
    }
}
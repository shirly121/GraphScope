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

package com.alibaba.graphscope.common.ir.convertor.cpp;

import com.alibaba.graphscope.common.ir.convertor.ExecutionNode;
import com.alibaba.graphscope.common.ir.convertor.LogicalNode;
import com.alibaba.graphscope.gaia.proto.GraphAlgebraPhysical;

public class NativePlanner<T, R> {
    public LogicalNode parse(String query) {
        return null;
    }

    public GraphAlgebraPhysical.PhysicalPlan optimize(LogicalNode logical) {
        return null;
    }

    public ExecutionNode<T> convert(
            GraphAlgebraPhysical.PhysicalPlan physical,
            NativeExecutionConvertor<T, R> convertor,
            NativeContext<R> context) {
        return null;
    }
}

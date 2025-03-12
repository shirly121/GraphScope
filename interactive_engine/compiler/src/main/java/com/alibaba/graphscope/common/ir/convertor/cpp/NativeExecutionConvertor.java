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
import com.alibaba.graphscope.common.ir.convertor.java.Context;
import com.alibaba.graphscope.gaia.proto.GraphAlgebra;
import com.alibaba.graphscope.gaia.proto.GraphAlgebraPhysical;

public interface NativeExecutionConvertor<T, R> {
    ExecutionNode<T> convert(GraphAlgebraPhysical.Scan scan, Context<R> context);
    ExecutionNode<T> convert(GraphAlgebraPhysical.EdgeExpand expand, Context<R> context, ExecutionNode<T> child);
    ExecutionNode<T> convert(GraphAlgebraPhysical.GetV getV, Context<R> context, ExecutionNode<T> child);
    ExecutionNode<T> convert(GraphAlgebraPhysical.PathExpand pxdExpand, Context<R> context, ExecutionNode<T> child);
    ExecutionNode<T> convert(GraphAlgebraPhysical.Intersect intersect, Context<R> context, ExecutionNode<T> child);
    ExecutionNode<T> convert(GraphAlgebraPhysical.Join join, Context<R> context, ExecutionNode<T> left, ExecutionNode<T> right);
    ExecutionNode<T> convert(GraphAlgebraPhysical.Project project, Context<R> context, ExecutionNode<T> child);
    ExecutionNode<T> convert(GraphAlgebra.Select filter, Context<R> context, ExecutionNode<T> child);
    ExecutionNode<T> convert(GraphAlgebra.OrderBy sort, Context<R> context, ExecutionNode<T> child);
    ExecutionNode<T> convert(GraphAlgebraPhysical.GroupBy aggregate, Context<R> context, ExecutionNode<T> child);
}

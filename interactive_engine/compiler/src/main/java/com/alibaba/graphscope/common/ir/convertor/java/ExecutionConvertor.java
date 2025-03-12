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

package com.alibaba.graphscope.common.ir.convertor.java;

import com.alibaba.graphscope.common.ir.convertor.ExecutionNode;
import com.alibaba.graphscope.common.ir.convertor.physical.*;

import java.util.List;

public interface ExecutionConvertor<T, R> {
    ExecutionNode<T> convert(PhysicalScan scan, Context<R> context);
    ExecutionNode<T> convert(PhysicalExpand expand, Context<R> context, ExecutionNode<T> child);
    ExecutionNode<T> convert(PhysicalGetV getV, Context<R> context, ExecutionNode<T> child);
    ExecutionNode<T> convert(PhysicalPathExpand pxdExpand, Context<R> context, ExecutionNode<T> child);
    ExecutionNode<T> convert(PhysicalIntersect intersect, Context<R> context, ExecutionNode<T> child);
    ExecutionNode<T> convert(PhysicalHashJoin join, Context<R> context, ExecutionNode<T> left, ExecutionNode<T> right);
    ExecutionNode<T> convert(PhysicalProject project, Context<R> context, ExecutionNode<T> child);
    ExecutionNode<T> convert(PhysicalFilter filter, Context<R> context, ExecutionNode<T> child);
    ExecutionNode<T> convert(PhysicalSort sort, Context<R> context, ExecutionNode<T> child);
    ExecutionNode<T> convert(PhysicalAggregate aggregate, Context<R> context, ExecutionNode<T> child);
    ExecutionNode<T> convert(PhysicalNode other, Context<R> context, List<ExecutionNode<T>> children);
}

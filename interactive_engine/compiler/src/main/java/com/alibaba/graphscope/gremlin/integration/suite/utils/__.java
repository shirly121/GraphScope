/*
 * This file is referred and derived from project apache/tinkerpop
 *
 * https://github.com/apache/tinkerpop/blob/master/gremlin-core/src/main/java/org/apache/tinkerpop/gremlin/process/traversal/dsl/graph/__.java
 *
 * which has the following license:
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.alibaba.graphscope.gremlin.integration.suite.utils;

import com.alibaba.graphscope.gremlin.antlr4.GremlinAntlrToJava;
import com.alibaba.graphscope.gremlin.plugin.script.AntlrGremlinScriptEngine;
import com.alibaba.graphscope.gremlin.plugin.traversal.IrCustomizedTraversal;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.*;

import java.util.Map;

/**
 * standard {@link org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__} always returns
 * {@link org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal} by invoking start(),
 * which cannot be compared with {@link com.alibaba.graphscope.gremlin.plugin.traversal.IrCustomizedTraversal} for type difference.
 * Here rewrite these static functions to put the added step into IrCustomizedTraversal, which is comparable to the type generated by
 * {@link AntlrGremlinScriptEngine}
 */
public class __ {
    protected __() {}

    public static <A> GraphTraversal<?, ?> start() {
        return GremlinAntlrToJava.getTraversalSupplier().get();
    }

    public static <A extends Element, B> GraphTraversal<?, Map<Object, B>> valueMap(
            final String... propertyKeys) {
        return start().valueMap(propertyKeys);
    }

    public static <A extends Element, B> GraphTraversal<?, B> values(final String... propertyKeys) {
        return start().values(propertyKeys);
    }

    public static <A> GraphTraversal<?, ?> as(final String label, final String... labels) {
        return start().as(label, labels);
    }

    public static GraphTraversal<?, Vertex> out(final String... edgeLabels) {
        return start().out(edgeLabels);
    }

    public static IrCustomizedTraversal<?, ?> out(Traversal rangeTraversal, String... labels) {
        return (IrCustomizedTraversal<?, ?>)
                ((IrCustomizedTraversal) start()).out(rangeTraversal, labels);
    }

    public static GraphTraversal<?, Vertex> bothV() {
        return start().bothV();
    }

    public static GraphTraversal<?, Vertex> in(final String... edgeLabels) {
        return start().in(edgeLabels);
    }

    public static GraphTraversal<?, Edge> outE(final String... edgeLabels) {
        return start().outE(edgeLabels);
    }

    public static GraphTraversal<?, ?> endV() {
        return ((IrCustomizedTraversal) start()).endV();
    }

    public static <A> GraphTraversal<?, Long> count() {
        return start().count();
    }

    public static <A> GraphTraversal<?, ?> fold() {
        return start().fold();
    }

    public static <A> GraphTraversal<?, ?> range(final long low, final long high) {
        return start().range(low, high);
    }

    public static <A, B> GraphTraversal<?, B> select(final String selectKey) {
        return start().select(selectKey);
    }

    public static <A> GraphTraversal<?, ?> where(Traversal<?, ?> whereTraversal) {
        return start().where(whereTraversal);
    }

    public static <A> GraphTraversal<?, ?> not(Traversal<?, ?> notTraversal) {
        return start().not(notTraversal);
    }

    public static <A> GraphTraversal<?, ?> dedup(String... dedupLabels) {
        return start().dedup(dedupLabels);
    }

    public static <A> GraphTraversal<?, ?> sum() {
        return start().sum();
    }

    public static <A> GraphTraversal<?, ?> has(final String propertyKey, final Object value) {
        return start().has(propertyKey, value);
    }

    public static <A> GraphTraversal<?, ?> identity() {
        return start().identity();
    }
}

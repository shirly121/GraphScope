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

package com.alibaba.graphscope.common.ir.glogue;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.config.PlannerConfig;
import com.alibaba.graphscope.common.ir.Utils;
import com.alibaba.graphscope.common.ir.meta.glogue.calcite.GraphRelMetadataQuery;
import com.alibaba.graphscope.common.ir.meta.glogue.calcite.handler.GraphMetadataHandlerProvider;
import com.alibaba.graphscope.common.ir.meta.schema.GraphOptSchema;
import com.alibaba.graphscope.common.ir.planner.GraphIOProcessor;
import com.alibaba.graphscope.common.ir.planner.GraphOptimizer;
import com.alibaba.graphscope.common.ir.planner.rules.ExtendIntersectRule;
import com.alibaba.graphscope.common.ir.planner.volcano.VolcanoPlannerX;
import com.alibaba.graphscope.common.ir.rel.GraphPattern;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.Glogue;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.GlogueQuery;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.Pattern;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.PatternVertex;
import com.alibaba.graphscope.common.ir.rel.metadata.glogue.pattern.SinglePatternVertex;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.EdgeTypeId;
import com.alibaba.graphscope.common.ir.rel.metadata.schema.GlogueSchema;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.alibaba.graphscope.common.ir.tools.GraphPlanner;
import com.google.common.collect.ImmutableMap;

import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.GraphOptCluster;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMdRowCount;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RelMetadataQueryTest {
        // @Test
        public void test() throws Exception {
                VolcanoPlanner planner = new VolcanoPlannerX();
                planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
                RelOptCluster optCluster = GraphOptCluster.create(planner, Utils.rexBuilder);

                GlogueSchema g = new GlogueSchema().DefaultGraphSchema();
                Glogue gl = new Glogue().create(g, 3);
                GlogueQuery gq = new GlogueQuery(gl, g);
                GraphRelMetadataQuery mq = new GraphRelMetadataQuery(
                                new GraphMetadataHandlerProvider(planner, new RelMdRowCount(), gq));

                optCluster.setMetadataQuerySupplier(() -> mq);

                GraphBuilder builder = (GraphBuilder) GraphPlanner.relBuilderFactory.create(
                                optCluster,
                                new GraphOptSchema(optCluster, Utils.schemaMeta.getSchema()));
                RelNode node = com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "Match (p1:person)-[:knows]->(p2:person),"
                                                + " (p2:person)-[:knows]->(p3:person),"
                                                + " (p3:person)-[:knows]->(p4:person),"
                                                + " (p4:person)-[:created]->(s:software) Return p1",
                                builder)
                                .build();
                RelNode match = node.getInput(0);
                System.out.println(match.explain());

                planner.setTopDownOpt(true);
                planner.setNoneConventionHasInfiniteCost(false);
                planner.addRule(
                                ExtendIntersectRule.Config.DEFAULT
                                                .withRelBuilderFactory(GraphPlanner.relBuilderFactory)
                                                .withMaxPatternSizeInGlogue(gq.getMaxPatternSize())
                                                .toRule());

                GraphIOProcessor ioProcessor = new GraphIOProcessor(builder, Utils.schemaMeta);
                planner.setRoot(ioProcessor.processInput(match));
                RelNode after = planner.findBestExp();
                // RelNode output = ioProcessor.processOutput(after);

                // planner.dump(new PrintWriter(new FileOutputStream("set1.out"), true));
                System.out.println(after.explain());

                Map<Integer, Pattern> patterns = com.alibaba.graphscope.common.ir.tools.Utils.getAllPatterns(after);
                patterns.forEach(
                                (k, v) -> {
                                        System.out.println(v);
                                });
        }

        // @Test
        public void test_2() {
                Pattern p = new Pattern();
                // p1 -> s0 <- p2 + p1 -> p2
                PatternVertex v0 = new SinglePatternVertex(1, 0);
                PatternVertex v1 = new SinglePatternVertex(0, 1);
                PatternVertex v2 = new SinglePatternVertex(0, 2);
                // p -> s
                EdgeTypeId e = new EdgeTypeId(0, 1, 1);
                // p -> p
                EdgeTypeId e1 = new EdgeTypeId(0, 0, 0);
                p.addVertex(v0);
                p.addVertex(v1);
                p.addVertex(v2);
                p.addEdge(v1, v0, e);
                p.addEdge(v2, v0, e);
                p.addEdge(v1, v2, e1);
                System.out.println(
                                System.identityHashCode(v0)
                                                + " "
                                                + System.identityHashCode(v1)
                                                + " "
                                                + System.identityHashCode(v2));

                Pattern p1 = new Pattern(p);
                for (PatternVertex v : p1.getVertexSet()) {
                        System.out.println(System.identityHashCode(v));
                }
        }

        // @Test
        public void test_3() {
                PlannerConfig plannerConfig = PlannerConfig.create(
                                new Configs(
                                                ImmutableMap.of(
                                                                "graph.planner.is.on", "true",
                                                                "graph.planner.opt", "CBO",
                                                                "graph.planner.rules", "ExtendIntersectRule")));
                GraphOptimizer optimizer = new GraphOptimizer(plannerConfig);
                RelOptCluster optCluster = GraphOptCluster.create(optimizer.getGraphOptPlanner(), Utils.rexBuilder);
                optCluster.setMetadataQuerySupplier(() -> optimizer.createMetaDataQuery());
                GraphBuilder builder = (GraphBuilder) GraphPlanner.relBuilderFactory.create(
                                optCluster,
                                new GraphOptSchema(optCluster, Utils.schemaMeta.getSchema()));
                RelNode node = com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "Match (p1:person)-[:knows]->(p2:person),"
                                                + " (p2:person)-[:knows]->(p3:person),"
                                                + " (p1:person)-[:knows]->(p3:person),"
                                                + " (p1:person)-[:created]->(s:software),"
                                                + " (p2:person)-[:created]->(s:software),"
                                                + " (p3:person)-[:created]->(s:software) Return p1, p2, p3",
                                builder)
                                .build();
                RelNode after = optimizer.optimize(node, new GraphIOProcessor(builder, Utils.schemaMeta));
                System.out.println(com.alibaba.graphscope.common.ir.tools.Utils.toString(after));
        }

        @Test
        public void test_ldbc_patterns() throws Exception {
                VolcanoPlanner planner = new VolcanoPlannerX();
                planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
                RelOptCluster optCluster = GraphOptCluster.create(planner, Utils.rexBuilder);

                GlogueSchema g = new GlogueSchema()
                                .SchemaFromFile(
                                                "/workspaces/GraphScope/interactive_engine/compiler/src/main/java/com/alibaba/graphscope/common/ir/rel/metadata/schema/resource/ldbc1_statistics.txt");

                Glogue gl = new Glogue().create(g, 3);
                System.out.println(gl.toString());
                GlogueQuery gq = new GlogueQuery(gl, g);

                GraphRelMetadataQuery mq = new GraphRelMetadataQuery(
                                new GraphMetadataHandlerProvider(planner, new RelMdRowCount(), gq));

                optCluster.setMetadataQuerySupplier(() -> mq);

                planner.setTopDownOpt(true);
                planner.setNoneConventionHasInfiniteCost(false);
                planner.addRule(
                                ExtendIntersectRule.Config.DEFAULT
                                                .withRelBuilderFactory(GraphPlanner.relBuilderFactory)
                                                .withMaxPatternSizeInGlogue(gq.getMaxPatternSize())
                                                .toRule());

                List<Pattern> patterns = new ArrayList<>();
                patterns.add(test_ldbc_p1());
                patterns.add(test_ldbc_p2());
                patterns.add(test_ldbc_p3());
                patterns.add(test_ldbc_p4());
                patterns.add(test_ldbc_p5());
                patterns.add(test_ldbc_p6());
                patterns.add(test_ldbc_p7());
                patterns.add(test_ldbc_p8());
                patterns.add(test_ldbc_p9());

                for (Pattern pattern : patterns) {
                        GraphPattern graphPattern = new GraphPattern(optCluster, planner.emptyTraitSet(), pattern);
                        planner.setRoot(graphPattern);
                        RelNode after = planner.findBestExp();
                        System.out.println(after.explain());
                        Map<Integer, Pattern> genPath = com.alibaba.graphscope.common.ir.tools.Utils
                                        .getAllPatterns(after);
                                        genPath.forEach(
                                        (k, v) -> {
                                                System.out.println(v);
                                        });
                }
        }

        public Pattern test_ldbc_p1() throws Exception {

                Pattern p = new Pattern();
                // comment -> tag <- person + comment -> person
                // v0: comment
                PatternVertex v0 = new SinglePatternVertex(2, 0);
                // v1: tag
                PatternVertex v1 = new SinglePatternVertex(7, 1);
                // v2: person
                PatternVertex v2 = new SinglePatternVertex(1, 2);
                // comment -hastag-> tag
                EdgeTypeId e0 = new EdgeTypeId(2, 7, 1);
                // person -hasInterest-> tag
                EdgeTypeId e1 = new EdgeTypeId(1, 7, 10);
                // comment -hasCreator-> person
                EdgeTypeId e2 = new EdgeTypeId(2, 1, 0);
                p.addVertex(v0);
                p.addVertex(v1);
                p.addVertex(v2);
                p.addEdge(v0, v1, e0);
                p.addEdge(v2, v1, e1);
                p.addEdge(v0, v2, e2);
                p.reordering();

                return p;
        }

        // person1 -likes-> comment, person1 -livesIn-> city, comment -created person2,
        // person2 -livesIn -> city
        public Pattern test_ldbc_p2() throws Exception {

                Pattern p = new Pattern();
                // v0: person
                PatternVertex v0 = new SinglePatternVertex(1, 0);
                // v1: comment
                PatternVertex v1 = new SinglePatternVertex(2, 1);
                // v2: city
                PatternVertex v2 = new SinglePatternVertex(0, 2);
                // v3: person
                PatternVertex v3 = new SinglePatternVertex(1, 3);
                // person -likes-> comment
                EdgeTypeId e0 = new EdgeTypeId(1, 2, 13);
                // person -livesIn-> city
                EdgeTypeId e1 = new EdgeTypeId(1, 0, 11);
                // comment -hasCreator-> person
                EdgeTypeId e2 = new EdgeTypeId(2, 1, 0);

                p.addVertex(v0);
                p.addVertex(v1);
                p.addVertex(v2);
                p.addVertex(v3);
                p.addEdge(v0, v1, e0);
                p.addEdge(v0, v2, e1);
                p.addEdge(v1, v3, e2);
                p.addEdge(v3, v2, e1);
                p.reordering();

                return p;
        }

        // comment -hasCreator-> person1, person2 -knows-> person1, comment -replyOf->
        // post, post <-likes- person2
        public Pattern test_ldbc_p3() throws Exception {

                Pattern p = new Pattern();
                // comment -hasCreator-> person1, person2 -knows-> person1, comment -replyOf->
                // post, person2 -likes-> post
                // v0: comment
                PatternVertex v0 = new SinglePatternVertex(2, 0);
                // v1: person1
                PatternVertex v1 = new SinglePatternVertex(1, 1);
                // v2: post
                PatternVertex v2 = new SinglePatternVertex(3, 2);
                // v3: person2
                PatternVertex v3 = new SinglePatternVertex(1, 3);
                // comment -hasCreator-> person
                EdgeTypeId e0 = new EdgeTypeId(2, 1, 0);
                // person -knows-> person
                EdgeTypeId e1 = new EdgeTypeId(1, 1, 12);
                // comment -replyOf-> post
                EdgeTypeId e2 = new EdgeTypeId(2, 3, 3);
                // post-hasCreator-> person
                EdgeTypeId e3 = new EdgeTypeId(3, 1, 0);

                p.addVertex(v0);
                p.addVertex(v1);
                p.addVertex(v2);
                p.addVertex(v3);
                p.addEdge(v0, v1, e0);
                p.addEdge(v1, v3, e1);
                p.addEdge(v0, v2, e2);
                p.addEdge(v2, v3, e3);
                p.reordering();

                return p;
        }

        // person<-comment->post<-forum->person
        public Pattern test_ldbc_p4() throws Exception {

                Pattern p = new Pattern();
                // person1<-comment->post<-forum->person2
                // v0: person
                PatternVertex v0 = new SinglePatternVertex(1, 0);
                // v1: comment
                PatternVertex v1 = new SinglePatternVertex(2, 1);
                // v2: post
                PatternVertex v2 = new SinglePatternVertex(3, 2);
                // v3: forum
                PatternVertex v3 = new SinglePatternVertex(4, 3);
                // v4: person
                PatternVertex v4 = new SinglePatternVertex(1, 4);
                // comment -hasCreator-> person
                EdgeTypeId e0 = new EdgeTypeId(2, 1, 0);
                // comment -replyOf-> post
                EdgeTypeId e1 = new EdgeTypeId(2, 3, 3);
                // forum -containerOf-> post
                EdgeTypeId e2 = new EdgeTypeId(4, 3, 5);
                // forum -hasMember-> person
                EdgeTypeId e3 = new EdgeTypeId(4, 1, 6);

                p.addVertex(v0);
                p.addVertex(v1);
                p.addVertex(v2);
                p.addVertex(v3);
                p.addVertex(v4);
                p.addEdge(v1, v0, e0);
                p.addEdge(v1, v2, e1);
                p.addEdge(v3, v2, e2);
                p.addEdge(v3, v4, e3);
                p.reordering();

                return p;
        }

        // comment1 -hasCreator-> person1, person1 -likes-> comment2, comment2
        // -hasCreator-> person2, person2 -likes-> comment1, comment1-replyOf->comment2
        public Pattern test_ldbc_p5() throws Exception {
                Pattern p = new Pattern();
                // comment1 -hasCreator-> person1, person1 -likes-> comment2, comment2
                // -hasCreator-> person2, person2 -likes-> comment1, comment1-replyOf->comment2
                // v0: comment1
                PatternVertex v0 = new SinglePatternVertex(2, 0);
                // v1: person1
                PatternVertex v1 = new SinglePatternVertex(1, 1);
                // v2: comment2
                PatternVertex v2 = new SinglePatternVertex(2, 2);
                // v3: person2
                PatternVertex v3 = new SinglePatternVertex(1, 3);
                // comment -hasCreator-> person
                EdgeTypeId e0 = new EdgeTypeId(2, 1, 0);
                // person -likes-> comment
                EdgeTypeId e1 = new EdgeTypeId(1, 2, 13);
                // comment -replyOf-> comment
                EdgeTypeId e2 = new EdgeTypeId(2, 2, 3);

                p.addVertex(v0);
                p.addVertex(v1);
                p.addVertex(v2);
                p.addVertex(v3);
                p.addEdge(v0, v1, e0);
                p.addEdge(v1, v2, e1);
                p.addEdge(v2, v3, e0);
                p.addEdge(v3, v0, e1);
                p.addEdge(v0, v2, e2);
                p.reordering();

                return p;
        }

        // forum->person1, forum->person2, forum-> post, person1->person2,
        // person1->post, person2->post
        public Pattern test_ldbc_p6() throws Exception {

                Pattern p = new Pattern();
                // forum->person1, forum->person2, forum-> post, person1->person2,
                // person1->post, person2->post
                // v0: forum
                PatternVertex v0 = new SinglePatternVertex(4, 0);
                // v1: person1
                PatternVertex v1 = new SinglePatternVertex(1, 1);
                // v2: person2
                PatternVertex v2 = new SinglePatternVertex(1, 2);
                // v3: post
                PatternVertex v3 = new SinglePatternVertex(3, 3);
                // forum -hasMember-> person
                EdgeTypeId e0 = new EdgeTypeId(4, 1, 6);
                // forum -containerOf-> post
                EdgeTypeId e1 = new EdgeTypeId(4, 3, 5);
                // person -knows-> person
                EdgeTypeId e2 = new EdgeTypeId(1, 1, 12);
                // person -likes-> post
                EdgeTypeId e3 = new EdgeTypeId(1, 3, 13);

                p.addVertex(v0);
                p.addVertex(v1);
                p.addVertex(v2);
                p.addVertex(v3);
                p.addEdge(v0, v1, e0);
                p.addEdge(v0, v2, e0);
                p.addEdge(v0, v3, e1);
                p.addEdge(v1, v2, e2);
                p.addEdge(v1, v3, e3);
                p.addEdge(v2, v3, e3);
                p.reordering();

                return p;
        }

        // forum -> person1, forum->person2, person2->person1, comment1->person1,
        // comment2->person2, comment2->comment1
        public Pattern test_ldbc_p7() throws Exception {

                Pattern p = new Pattern();
                // forum -> person1, forum->person2, person2->person1, comment1->person1,
                // comment2->person2, comment2->comment1
                // v0: forum
                PatternVertex v0 = new SinglePatternVertex(4, 0);
                // v1: person1
                PatternVertex v1 = new SinglePatternVertex(1, 1);
                // v2: person2
                PatternVertex v2 = new SinglePatternVertex(1, 2);
                // v3: comment1
                PatternVertex v3 = new SinglePatternVertex(2, 3);
                // v4: comment2
                PatternVertex v4 = new SinglePatternVertex(2, 4);
                // forum -hasMember-> person
                EdgeTypeId e0 = new EdgeTypeId(4, 1, 6);
                // person -knows-> person
                EdgeTypeId e1 = new EdgeTypeId(1, 1, 12);
                // comment -hasCreator-> person
                EdgeTypeId e2 = new EdgeTypeId(2, 1, 0);
                // comment -replyOf-> comment
                EdgeTypeId e3 = new EdgeTypeId(2, 2, 3);

                p.addVertex(v0);
                p.addVertex(v1);
                p.addVertex(v2);
                p.addVertex(v3);
                p.addVertex(v4);
                p.addEdge(v0, v1, e0);
                p.addEdge(v0, v2, e0);
                p.addEdge(v2, v1, e1);
                p.addEdge(v3, v1, e2);
                p.addEdge(v4, v2, e2);
                p.addEdge(v4, v3, e3);
                p.reordering();

                return p;
        }

        // comment1->tag, comment2->tag, comment1->comment2, comment1->person1,
        // comment2->person2, person1->person2
        public Pattern test_ldbc_p8() throws Exception {

                Pattern p = new Pattern();
                // comment1->tag, comment2->tag, comment1->comment2, comment1->person1,
                // comment2->person2, person1->person2
                // v0: tag
                PatternVertex v0 = new SinglePatternVertex(7, 0);
                // v1: comment1
                PatternVertex v1 = new SinglePatternVertex(2, 1);
                // v2: comment2
                PatternVertex v2 = new SinglePatternVertex(2, 2);
                // v3: person1
                PatternVertex v3 = new SinglePatternVertex(1, 3);
                // v4: person2
                PatternVertex v4 = new SinglePatternVertex(1, 4);
                // comment -hasTag-> tag
                EdgeTypeId e0 = new EdgeTypeId(2, 7, 1);
                // comment -replyOf-> comment
                EdgeTypeId e1 = new EdgeTypeId(2, 2, 3);
                // comment -hasCreator-> person
                EdgeTypeId e2 = new EdgeTypeId(2, 1, 0);
                // person -knows-> person
                EdgeTypeId e3 = new EdgeTypeId(1, 1, 12);

                p.addVertex(v0);
                p.addVertex(v1);
                p.addVertex(v2);
                p.addVertex(v3);
                p.addVertex(v4);
                p.addEdge(v1, v0, e0);
                p.addEdge(v2, v0, e0);
                p.addEdge(v1, v2, e1);
                p.addEdge(v1, v3, e2);
                p.addEdge(v2, v4, e2);
                p.addEdge(v3, v4, e3);
                p.reordering();

                return p;
        }

        // person1 -> person2, person1 -> person3, person2 -> person3, person1 ->city1,
        // person2->city2, person3->city3, city1->country, city2->country,
        // city3->country
        public Pattern test_ldbc_p9() throws Exception {

                Pattern p = new Pattern();
                // person1 -> person2, person1 -> person3, person2 -> person3, person1 ->city1,
                // person2->city2, person3->city3, city1->country, city2->country,
                // city3->country
                // v0: person1
                PatternVertex v0 = new SinglePatternVertex(1, 0);
                // v1: person2
                PatternVertex v1 = new SinglePatternVertex(1, 1);
                // v2: person3
                PatternVertex v2 = new SinglePatternVertex(1, 2);
                // v3: city1
                PatternVertex v3 = new SinglePatternVertex(0, 3);
                // v4: city2
                PatternVertex v4 = new SinglePatternVertex(0, 4);
                // v5: city3
                PatternVertex v5 = new SinglePatternVertex(0, 5);
                // v6: country
                PatternVertex v6 = new SinglePatternVertex(0, 6);
                // person -knows-> person
                EdgeTypeId e0 = new EdgeTypeId(1, 1, 12);
                // person -isLocatedIn-> city
                EdgeTypeId e1 = new EdgeTypeId(1, 0, 11);
                // city -isPartOf-> country
                EdgeTypeId e2 = new EdgeTypeId(0, 0, 17);

                p.addVertex(v0);
                p.addVertex(v1);
                p.addVertex(v2);
                p.addVertex(v3);
                p.addVertex(v4);
                p.addVertex(v5);
                p.addVertex(v6);
                p.addEdge(v0, v1, e0);
                p.addEdge(v0, v2, e0);
                p.addEdge(v1, v2, e0);
                p.addEdge(v0, v3, e1);
                p.addEdge(v1, v4, e1);
                p.addEdge(v2, v5, e1);
                p.addEdge(v3, v6, e2);
                p.addEdge(v4, v6, e2);
                p.addEdge(v5, v6, e2);
                p.reordering();

                return p;
        }

        // person1 -> comment1 -> person2, person1 -> city, person2 -> city, person3 ->
        // comment2 ->
        // person4, person3 -> city, person4 -> city
        public Pattern test_ldbc_p10() throws Exception {

                Pattern p = new Pattern();
                // person1 -> comment1 -> person2, person1 -> city, person2 -> city, person3 ->
                // comment2 ->
                // person4, person3 -> city, person4 -> city

                // v0: person1
                PatternVertex v0 = new SinglePatternVertex(1, 0);
                // v1: comment1
                PatternVertex v1 = new SinglePatternVertex(2, 1);
                // v2: person2
                PatternVertex v2 = new SinglePatternVertex(1, 2);
                // v3: city
                PatternVertex v3 = new SinglePatternVertex(0, 3);
                // v4: person3
                PatternVertex v4 = new SinglePatternVertex(1, 4);
                // v5: comment2
                PatternVertex v5 = new SinglePatternVertex(2, 5);
                // v6: person4
                PatternVertex v6 = new SinglePatternVertex(1, 6);
                // person -> comment
                EdgeTypeId e0 = new EdgeTypeId(1, 2, 13);
                // comment -> person
                EdgeTypeId e1 = new EdgeTypeId(2, 1, 0);
                // person -> city
                EdgeTypeId e2 = new EdgeTypeId(1, 0, 11);

                p.addVertex(v0);
                p.addVertex(v1);
                p.addVertex(v2);
                p.addVertex(v3);
                p.addVertex(v4);
                p.addVertex(v5);
                p.addVertex(v6);
                p.addEdge(v0, v1, e0);
                p.addEdge(v1, v2, e1);
                p.addEdge(v0, v3, e2);
                p.addEdge(v2, v3, e2);
                p.addEdge(v4, v5, e0);
                p.addEdge(v5, v6, e1);
                p.addEdge(v4, v3, e2);
                p.addEdge(v6, v3, e2);
                p.reordering();

                return p;
        }
}

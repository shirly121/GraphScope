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

package com.alibaba.graphscope.common.ir.planner.cbo;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.ir.Utils;
import com.alibaba.graphscope.common.ir.meta.IrMeta;
import com.alibaba.graphscope.common.ir.planner.GraphIOProcessor;
import com.alibaba.graphscope.common.ir.planner.GraphRelOptimizer;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.google.common.collect.ImmutableMap;

import org.apache.calcite.rel.RelNode;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class CSRBITest {
    private static Configs configs;
    private static IrMeta irMeta;
    private static GraphRelOptimizer optimizer;

    @BeforeClass
    public static void beforeClass() {
        configs =
                new Configs(
                        ImmutableMap.of(
                                "graph.planner.is.on",
                                "true",
                                "graph.planner.opt",
                                "CBO",
                                "graph.planner.rules",
                                "FilterIntoJoinRule, FilterMatchRule, FlatJoinToExpandRule,"
                                    + " ExtendIntersectRule, DegreeFusionRule,"
                                    + " ExpandGetVFusionRule"));
        optimizer = new GraphRelOptimizer(configs);
        irMeta =
                Utils.mockIrMeta(
                        "schema/ldbc_schema_csr.yaml",
                        "statistics/ldbc_statistics_csr.json",
                        optimizer.getGlogueHolder());
    }

    @Test
    public void bi1_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before = com.alibaba.graphscope.cypher.antlr4.Utils.eval("", builder).build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
    }

    @Test
    public void bi2_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH (tag:TAG)-[:HASTYPE]->(:TAGCLASS {name: $tagClass})\n"
                                    + "OPTIONAL MATCH (tag:TAG)<-[:HASTAG]-(message:POST|COMMENT)\n"
                                    + "WITH\n"
                                    + "  tag,\n"
                                    + "  CASE\n"
                                    + "    WHEN message.creationDate <  $dateEnd1\n"
                                    + "  AND message.creationDate >= $date THEN 1\n"
                                    + "    ELSE                                     0\n"
                                    + "    END AS count1,\n"
                                    + "  CASE\n"
                                    + "    WHEN message.creationDate <  $dateEnd2\n"
                                    + "  AND message.creationDate >= $dateEnd1 THEN 1\n"
                                    + "    ELSE                                     0\n"
                                    + "    END AS count2\n"
                                    + "WITH\n"
                                    + "  tag,\n"
                                    + "  sum(count1) AS countWindow1,\n"
                                    + "  sum(count2) AS countWindow2\n"
                                    + "RETURN\n"
                                    + "  tag.name as name,\n"
                                    + "  countWindow1,\n"
                                    + "  countWindow2,\n"
                                    + "  gs.function.abs(countWindow1 - countWindow2) AS diff\n"
                                    + "ORDER BY\n"
                                    + "diff DESC,\n"
                                    + "name ASC\n"
                                    + "LIMIT 100",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalSort(sort0=[diff], sort1=[name], dir0=[DESC], dir1=[ASC],"
                    + " fetch=[100])\n"
                    + "  GraphLogicalProject(name=[tag.name], countWindow1=[countWindow1],"
                    + " countWindow2=[countWindow2], diff=[gs.function.abs(-(countWindow1,"
                    + " countWindow2))], isAppend=[false])\n"
                    + "    GraphLogicalAggregate(keys=[{variables=[tag], aliases=[tag]}],"
                    + " values=[[{operands=[count1], aggFunction=SUM, alias='countWindow1',"
                    + " distinct=false}, {operands=[count2], aggFunction=SUM, alias='countWindow2',"
                    + " distinct=false}]])\n"
                    + "      GraphLogicalProject(tag=[tag],"
                    + " count1=[CASE(AND(<(message.creationDate, ?1), >=(message.creationDate,"
                    + " ?2)), 1, 0)], count2=[CASE(AND(<(message.creationDate, ?3),"
                    + " >=(message.creationDate, ?1)), 1, 0)], isAppend=[false])\n"
                    + "        GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[POST,"
                    + " COMMENT]}], alias=[message], opt=[START], physicalOpt=[ITSELF])\n"
                    + "          GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASTAG, COMMENT, TAG),"
                    + " EdgeLabel(HASTAG, POST, TAG)]], alias=[_], startAlias=[tag], opt=[IN],"
                    + " physicalOpt=[VERTEX], optional=[true])\n"
                    + "            GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[HASTYPE]}], alias=[tag], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "              GraphLogicalSource(tableConfig=[{isAll=false,"
                    + " tables=[TAGCLASS]}], alias=[_], fusedFilter=[[=(_.name, ?0)]],"
                    + " opt=[VERTEX])",
                after.explain().trim());
    }

    @Test
    public void bi3_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH\n"
                                    + "  (country:PLACE {name:"
                                    + " $country})<-[:ISPARTOF]-()<-[:ISLOCATEDIN]-\n"
                                    + "  (person:PERSON)<-[:HASMODERATOR]-(forum:FORUM)\n"
                                    + "WITH person, forum\n"
                                    + "MATCH"
                                    + " (forum)-[:CONTAINEROF]->(post:POST)<-[:REPLYOF*0..30]-(message)-[:HASTAG]->(:TAG)-[:HASTYPE]->(:TAGCLASS"
                                    + " {name: $tagClass})\n"
                                    + "RETURN\n"
                                    + "  forum.id as id,\n"
                                    + "  forum.title,\n"
                                    + "  forum.creationDate,\n"
                                    + "  person.id as personId,\n"
                                    + "  count(DISTINCT message) AS messageCount\n"
                                    + "  ORDER BY\n"
                                    + "  messageCount DESC,\n"
                                    + "  id ASC\n"
                                    + "  LIMIT 20",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        System.out.println(after.explain());
    }

    @Test
    public void bi4_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH (forum:FORUM)\n"
                                    + "WHERE forum.creationDate > $creationDate\n"
                                    + "WITH forum\n"
                                    + "ORDER BY\n"
                                    + "  forum.popularity DESC\n"
                                    + "LIMIT 100\n"
                                    + "WITH collect(forum) AS topForums\n"
                                    + "CALL {\n"
                                    + "  UNWIND topForums AS topForums1\n"
                                    + "  MATCH"
                                    + " (topForums1:FORUM)-[:CONTAINEROF]->(post:POST)<-[:REPLYOF*0..10]-(message:POST|COMMENT)-[:HASCREATOR]->(person:PERSON)<-[:HASMEMBER]-(topForums2:FORUM)\n"
                                    + "  WHERE topForums2 IN topForums\n"
                                    + "  RETURN\n"
                                    + "    person,\n"
                                    + "    count(message) AS messageCount\n"
                                    + "  ORDER BY\n"
                                    + "    messageCount DESC,\n"
                                    + "    person.id ASC\n"
                                    + "  LIMIT 100\n"
                                    + "}\n"
                                    + "UNION\n"
                                    + "CALL {\n"
                                    + "  // Ensure that people who are members of top forums but"
                                    + " have 0 messages are also returned.\n"
                                    + "  // To this end, we return each person with a 0"
                                    + " messageCount\n"
                                    + "  UNWIND topForums AS topForum1\n"
                                    + "  MATCH (person:PERSON)<-[:HASMEMBER]-(topForum1:FORUM)\n"
                                    + "  RETURN person, 0 AS messageCount\n"
                                    + "  ORDER BY\n"
                                    + "    person.id ASC\n"
                                    + "  LIMIT 100            \n"
                                    + "}\n"
                                    + "RETURN\n"
                                    + "  person.id AS personId,\n"
                                    + "  person.firstName AS personFirstName,\n"
                                    + "  person.lastName AS personLastName,\n"
                                    + "  person.creationDate AS personCreationDate,\n"
                                    + "  sum(messageCount) AS messageCount\n"
                                    + "ORDER BY\n"
                                    + "  messageCount DESC,\n"
                                    + "  personId ASC\n"
                                    + "LIMIT 100 ",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        System.out.println(com.alibaba.graphscope.common.ir.tools.Utils.toString(after));
    }

    @Test
    public void bi5_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "Match (tag:TAG {name: $tag})<-[:HASTAG]-(message:POST|COMMENT)\n"
                                    + "WITH DISTINCT message\n"
                                    + "OPTIONAL MATCH (message)<-[:LIKES]-(liker:PERSON)\n"
                                    + "WITH message, count(liker) as likeCount\n"
                                    + "OPTIONAL MATCH (message)<-[:REPLYOF]-(comment:COMMENT)\n"
                                    + "WITH message, likeCount, count(comment) as replyCount\n"
                                    + "MATCH (message)-[:HASCREATOR]->(person:PERSON)\n"
                                    + "WITH\n"
                                    + "  person.id AS id,\n"
                                    + "  sum(replyCount) as replyCount,\n"
                                    + "  sum(likeCount) as likeCount,\n"
                                    + "  count(message) as messageCount\n"
                                    + "RETURN\n"
                                    + "  id,\n"
                                    + "  replyCount,\n"
                                    + "  likeCount,\n"
                                    + "  messageCount,\n"
                                    + "  1*messageCount + 2*replyCount + 10*likeCount AS score\n"
                                    + "ORDER BY\n"
                                    + "  score DESC,\n"
                                    + "  id ASC\n"
                                    + "LIMIT 100;",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        System.out.println(after.explain());
    }
}

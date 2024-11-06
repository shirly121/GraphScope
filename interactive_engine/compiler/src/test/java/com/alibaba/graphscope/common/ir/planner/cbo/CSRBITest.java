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
                                    + " FlatJoinToCommonRule, ExtendIntersectRule,"
                                    + " DegreeFusionRule, ExpandGetVFusionRule"));
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
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH (message:COMMENT|POST)\n"
                                    + "WHERE message.creationDate < $datetime\n"
                                    + "WITH count(message) AS totalMessageCount\n"
                                    + "\n"
                                    + "MATCH (message:COMMENT|POST)\n"
                                    + "WHERE message.creationDate < $datetime\n"
                                    + "AND message.length > 0\n"
                                    + "WITH\n"
                                    + "  totalMessageCount,\n"
                                    + "  message,\n"
                                    + "  message.creationDate AS date\n"
                                    + "WITH\n"
                                    + "  totalMessageCount,\n"
                                    + "  date.year AS year,\n"
                                    + "  CASE\n"
                                    + "    WHEN 'POST' in labels(message)  THEN 0\n"
                                    + "    ELSE                                 1\n"
                                    + "    END AS isComment,\n"
                                    + "  CASE\n"
                                    + "    WHEN message.length <  40 THEN 0\n"
                                    + "    WHEN message.length <  80 THEN 1\n"
                                    + "    WHEN message.length < 160 THEN 2\n"
                                    + "    ELSE                           3\n"
                                    + "    END AS lengthCategory,\n"
                                    + "  count(message) AS messageCount,\n"
                                    + "  sum(message.length) / count(message) AS"
                                    + " averageMessageLength,\n"
                                    + "  count(message.length) AS sumMessageLength\n"
                                    + "\n"
                                    + "RETURN\n"
                                    + "  year,\n"
                                    + "  isComment,\n"
                                    + "  lengthCategory,\n"
                                    + "  messageCount,\n"
                                    + "  averageMessageLength,\n"
                                    + "  sumMessageLength,\n"
                                    + "  messageCount / totalMessageCount AS percentageOfMessages\n"
                                    + "  ORDER BY\n"
                                    + "  year DESC,\n"
                                    + "  isComment ASC,\n"
                                    + "  lengthCategory ASC",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalSort(sort0=[year], sort1=[isComment], sort2=[lengthCategory],"
                    + " dir0=[DESC], dir1=[ASC], dir2=[ASC])\n"
                    + "  GraphLogicalProject(year=[year], isComment=[isComment],"
                    + " lengthCategory=[lengthCategory], messageCount=[messageCount],"
                    + " averageMessageLength=[averageMessageLength],"
                    + " sumMessageLength=[sumMessageLength], percentageOfMessages=[/(messageCount,"
                    + " totalMessageCount)], isAppend=[false])\n"
                    + "    GraphLogicalProject(totalMessageCount=[totalMessageCount], year=[year],"
                    + " isComment=[isComment], lengthCategory=[lengthCategory],"
                    + " messageCount=[messageCount], sumMessageLength=[sumMessageLength],"
                    + " averageMessageLength=[/(EXPR$2, EXPR$3)], isAppend=[false])\n"
                    + "      GraphLogicalAggregate(keys=[{variables=[totalMessageCount, $f0, $f1,"
                    + " $f2], aliases=[totalMessageCount, year, isComment, lengthCategory]}],"
                    + " values=[[{operands=[message], aggFunction=COUNT, alias='messageCount',"
                    + " distinct=false}, {operands=[message.length], aggFunction=SUM,"
                    + " alias='EXPR$2', distinct=false}, {operands=[message], aggFunction=COUNT,"
                    + " alias='EXPR$3', distinct=false}, {operands=[message.length],"
                    + " aggFunction=COUNT, alias='sumMessageLength', distinct=false}]])\n"
                    + "        GraphLogicalProject($f0=[EXTRACT(FLAG(YEAR), date)],"
                    + " $f1=[CASE(IN(_UTF-8'POST', message.~label), 0, 1)],"
                    + " $f2=[CASE(<(message.length, 40), 0, <(message.length, 80), 1,"
                    + " <(message.length, 160), 2, 3)], isAppend=[true])\n"
                    + "          GraphLogicalProject(totalMessageCount=[totalMessageCount],"
                    + " message=[message], date=[message.creationDate], isAppend=[false])\n"
                    + "            LogicalJoin(condition=[true], joinType=[inner])\n"
                    + "              GraphLogicalAggregate(keys=[{variables=[], aliases=[]}],"
                    + " values=[[{operands=[message], aggFunction=COUNT, alias='totalMessageCount',"
                    + " distinct=false}]])\n"
                    + "                GraphLogicalSource(tableConfig=[{isAll=false,"
                    + " tables=[COMMENT, POST]}], alias=[message], fusedFilter=[[<(_.creationDate,"
                    + " ?0)]], opt=[VERTEX])\n"
                    + "              GraphLogicalSource(tableConfig=[{isAll=false, tables=[POST,"
                    + " COMMENT]}], alias=[message], fusedFilter=[[AND(<(_.creationDate, ?0),"
                    + " >(_.length, 0))]], opt=[VERTEX])",
                after.explain().trim());
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
        Assert.assertEquals(
                "GraphLogicalSort(sort0=[messageCount], sort1=[id], dir0=[DESC], dir1=[ASC],"
                    + " fetch=[20])\n"
                    + "  GraphLogicalAggregate(keys=[{variables=[forum.id, forum.title,"
                    + " forum.creationDate, person.id], aliases=[id, title, creationDate,"
                    + " personId]}], values=[[{operands=[message], aggFunction=COUNT,"
                    + " alias='messageCount', distinct=true}]])\n"
                    + "    LogicalJoin(condition=[=(forum, forum)], joinType=[inner])\n"
                    + "      GraphLogicalProject(person=[person], forum=[forum],"
                    + " isAppend=[false])\n"
                    + "        GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[HASMODERATOR]}], alias=[forum], startAlias=[person], opt=[IN],"
                    + " physicalOpt=[VERTEX])\n"
                    + "          GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[person], opt=[START], physicalOpt=[ITSELF])\n"
                    + "            GraphPhysicalExpand(tableConfig=[[EdgeLabel(ISLOCATEDIN, PERSON,"
                    + " PLACE)]], alias=[_], startAlias=[PATTERN_VERTEX$1], opt=[IN],"
                    + " physicalOpt=[VERTEX])\n"
                    + "              GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[ISPARTOF]}], alias=[PATTERN_VERTEX$1], startAlias=[country],"
                    + " opt=[IN], physicalOpt=[VERTEX])\n"
                    + "                GraphLogicalSource(tableConfig=[{isAll=false,"
                    + " tables=[PLACE]}], alias=[country], fusedFilter=[[=(_.name, ?0)]],"
                    + " opt=[VERTEX])\n"
                    + "      GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[CONTAINEROF]}],"
                    + " alias=[forum], startAlias=[post], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "        GraphLogicalGetV(tableConfig=[{isAll=false, tables=[POST]}],"
                    + " alias=[post], opt=[END])\n"
                    + "         "
                    + " GraphLogicalPathExpand(fused=[GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[REPLYOF]}], alias=[_], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "], fetch=[30], path_opt=[ARBITRARY], result_opt=[END_V], alias=[_],"
                    + " start_alias=[message])\n"
                    + "            GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[POST,"
                    + " COMMENT]}], alias=[message], opt=[START], physicalOpt=[ITSELF])\n"
                    + "              GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASTAG, COMMENT,"
                    + " TAG), EdgeLabel(HASTAG, POST, TAG)]], alias=[_],"
                    + " startAlias=[PATTERN_VERTEX$5], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "                GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[HASTYPE]}], alias=[PATTERN_VERTEX$5], opt=[IN],"
                    + " physicalOpt=[VERTEX])\n"
                    + "                  GraphLogicalSource(tableConfig=[{isAll=false,"
                    + " tables=[TAGCLASS]}], alias=[_], fusedFilter=[[=(_.name, ?1)]],"
                    + " opt=[VERTEX])",
                after.explain().trim());
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
        Assert.assertEquals(
                "root:\n"
                    + "GraphLogicalSort(sort0=[messageCount], sort1=[personId], dir0=[DESC],"
                    + " dir1=[ASC], fetch=[100])\n"
                    + "  GraphLogicalAggregate(keys=[{variables=[person.id, person.firstName,"
                    + " person.lastName, person.creationDate], aliases=[personId, personFirstName,"
                    + " personLastName, personCreationDate]}], values=[[{operands=[messageCount],"
                    + " aggFunction=SUM, alias='messageCount', distinct=false}]])\n"
                    + "    LogicalUnion(all=[true])\n"
                    + "      GraphLogicalSort(sort0=[messageCount], sort1=[person.id], dir0=[DESC],"
                    + " dir1=[ASC], fetch=[100])\n"
                    + "        GraphLogicalAggregate(keys=[{variables=[person], aliases=[person]}],"
                    + " values=[[{operands=[message], aggFunction=COUNT, alias='messageCount',"
                    + " distinct=false}]])\n"
                    + "          LogicalFilter(condition=[IN(topForums2, topForums)])\n"
                    + "            GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[HASMEMBER]}], alias=[topForums2], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "              GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASCREATOR, POST,"
                    + " PERSON), EdgeLabel(HASCREATOR, COMMENT, PERSON)]], alias=[person],"
                    + " opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "                GraphLogicalGetV(tableConfig=[{isAll=false, tables=[POST,"
                    + " COMMENT]}], alias=[message], opt=[START])\n"
                    + "                 "
                    + " GraphLogicalPathExpand(fused=[GraphPhysicalExpand(tableConfig=[[EdgeLabel(REPLYOF,"
                    + " COMMENT, POST), EdgeLabel(REPLYOF, COMMENT, COMMENT)]], alias=[_],"
                    + " opt=[IN], physicalOpt=[VERTEX])\n"
                    + "], fetch=[10], path_opt=[ARBITRARY], result_opt=[END_V], alias=[_])\n"
                    + "                    GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[CONTAINEROF]}], alias=[post], startAlias=[topForums1], opt=[OUT],"
                    + " physicalOpt=[VERTEX])\n"
                    + "                      GraphLogicalUnfold(key=[topForums],"
                    + " alias=[topForums1])\n"
                    + "                        CommonTableScan(table=[[common#637689160]])\n"
                    + "      GraphLogicalSort(sort0=[person.id], dir0=[ASC], fetch=[100])\n"
                    + "        GraphLogicalProject(person=[person], messageCount=[0],"
                    + " isAppend=[false])\n"
                    + "          GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[HASMEMBER]}], alias=[person], startAlias=[topForum1], opt=[OUT],"
                    + " physicalOpt=[VERTEX])\n"
                    + "            GraphLogicalUnfold(key=[topForums], alias=[topForum1])\n"
                    + "              CommonTableScan(table=[[common#637689160]])\n"
                    + "common#637689160:\n"
                    + "GraphLogicalAggregate(keys=[{variables=[], aliases=[]}],"
                    + " values=[[{operands=[forum], aggFunction=COLLECT, alias='topForums',"
                    + " distinct=false}]])\n"
                    + "  GraphLogicalSort(sort0=[forum.popularity], dir0=[DESC], fetch=[100])\n"
                    + "    GraphLogicalProject(forum=[forum], isAppend=[false])\n"
                    + "      GraphLogicalSource(tableConfig=[{isAll=false, tables=[FORUM]}],"
                    + " alias=[forum], fusedFilter=[[>(_.creationDate, ?0)]], opt=[VERTEX])",
                com.alibaba.graphscope.common.ir.tools.Utils.toString(after).trim());
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
        Assert.assertEquals(
                "GraphLogicalSort(sort0=[score], sort1=[id], dir0=[DESC], dir1=[ASC],"
                    + " fetch=[100])\n"
                    + "  GraphLogicalProject(id=[id], replyCount=[replyCount],"
                    + " likeCount=[likeCount], messageCount=[messageCount],"
                    + " score=[+(+(messageCount, *(2, replyCount)), *(10, likeCount))],"
                    + " isAppend=[false])\n"
                    + "    GraphLogicalAggregate(keys=[{variables=[person.id], aliases=[id]}],"
                    + " values=[[{operands=[replyCount], aggFunction=SUM, alias='replyCount',"
                    + " distinct=false}, {operands=[likeCount], aggFunction=SUM, alias='likeCount',"
                    + " distinct=false}, {operands=[message], aggFunction=COUNT,"
                    + " alias='messageCount', distinct=false}]])\n"
                    + "      GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASCREATOR, POST, PERSON),"
                    + " EdgeLabel(HASCREATOR, COMMENT, PERSON)]], alias=[person],"
                    + " startAlias=[message], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "        GraphLogicalProject(message=[message], likeCount=[likeCount],"
                    + " replyCount=[replyCount], isAppend=[false])\n"
                    + "          GraphPhysicalExpand(tableConfig=[[EdgeLabel(REPLYOF, COMMENT,"
                    + " POST), EdgeLabel(REPLYOF, COMMENT, COMMENT)]], alias=[replyCount],"
                    + " startAlias=[message], opt=[IN], physicalOpt=[DEGREE], optional=[true])\n"
                    + "            GraphLogicalProject(message=[message], likeCount=[likeCount],"
                    + " isAppend=[false])\n"
                    + "              GraphPhysicalExpand(tableConfig=[[EdgeLabel(LIKES, PERSON,"
                    + " POST), EdgeLabel(LIKES, PERSON, COMMENT)]], alias=[likeCount],"
                    + " startAlias=[message], opt=[IN], physicalOpt=[DEGREE], optional=[true])\n"
                    + "                GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[POST,"
                    + " COMMENT]}], alias=[message], opt=[START], physicalOpt=[ITSELF])\n"
                    + "                  GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASTAG,"
                    + " COMMENT, TAG), EdgeLabel(HASTAG, POST, TAG)]], alias=[_], startAlias=[tag],"
                    + " opt=[IN], physicalOpt=[VERTEX])\n"
                    + "                    GraphLogicalSource(tableConfig=[{isAll=false,"
                    + " tables=[TAG]}], alias=[tag], fusedFilter=[[=(_.name, ?0)]], opt=[VERTEX])",
                after.explain().trim());
    }

    @Test
    public void bi6_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH\n"
                                    + "  (tag:TAG {name:"
                                    + " $tag})<-[:HASTAG]-(message1:COMMENT|POST)-[:HASCREATOR]->(person1:PERSON)\n"
                                    + "OPTIONAL MATCH\n"
                                    + "  (message1:COMMENT|POST)<-[:LIKES]-(person2)\n"
                                    + "WITH DISTINCT person1, person2\n"
                                    + "RETURN\n"
                                    + "  person1.id AS id,\n"
                                    + "  sum(person2.popularityScore) AS score\n"
                                    + "ORDER BY score DESC, id ASC\n"
                                    + "LIMIT 100",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalSort(sort0=[score], sort1=[id], dir0=[DESC], dir1=[ASC],"
                    + " fetch=[100])\n"
                    + "  GraphLogicalAggregate(keys=[{variables=[person1.id], aliases=[id]}],"
                    + " values=[[{operands=[person2.popularityScore], aggFunction=SUM,"
                    + " alias='score', distinct=false}]])\n"
                    + "    GraphLogicalAggregate(keys=[{variables=[person1, person2],"
                    + " aliases=[person1, person2]}], values=[[]])\n"
                    + "      GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[LIKES]}],"
                    + " alias=[person2], startAlias=[message1], opt=[IN], physicalOpt=[VERTEX],"
                    + " optional=[true])\n"
                    + "        GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[HASCREATOR]}], alias=[person1], startAlias=[message1], opt=[OUT],"
                    + " physicalOpt=[VERTEX])\n"
                    + "          GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[POST,"
                    + " COMMENT]}], alias=[message1], opt=[START], physicalOpt=[ITSELF])\n"
                    + "            GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASTAG, COMMENT,"
                    + " TAG), EdgeLabel(HASTAG, POST, TAG)]], alias=[_], startAlias=[tag],"
                    + " opt=[IN], physicalOpt=[VERTEX])\n"
                    + "              GraphLogicalSource(tableConfig=[{isAll=false, tables=[TAG]}],"
                    + " alias=[tag], fusedFilter=[[=(_.name, ?0)]], opt=[VERTEX])",
                after.explain().trim());
    }

    @Test
    public void bi7_1_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH\n"
                                        + "  (tag:TAG {name:"
                                        + " $tag})<-[:HASTAG]-(message:POST|COMMENT),\n"
                                        + "  (message)<-[:REPLYOF]-(comment:COMMENT)\n"
                                        + "WHERE NOT (comment:COMMENT)-[:HASTAG]->(tag:TAG {name:"
                                        + " $tag})\n"
                                        + "MATCH (comment:COMMENT)-[:HASTAG]->(relatedTag:TAG)\n"
                                        + "RETURN\n"
                                        + "  relatedTag.name as name,\n"
                                        + "  count(DISTINCT comment) AS count\n"
                                        + "ORDER BY\n"
                                        + "  count DESC,\n"
                                        + "  name ASC\n"
                                        + "LIMIT 100",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalSort(sort0=[count], sort1=[name], dir0=[DESC], dir1=[ASC],"
                    + " fetch=[100])\n"
                    + "  GraphLogicalAggregate(keys=[{variables=[relatedTag.name],"
                    + " aliases=[name]}], values=[[{operands=[comment], aggFunction=COUNT,"
                    + " alias='count', distinct=true}]])\n"
                    + "    GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASTAG, COMMENT, TAG)]],"
                    + " alias=[relatedTag], startAlias=[comment], opt=[OUT],"
                    + " physicalOpt=[VERTEX])\n"
                    + "      LogicalJoin(condition=[AND(=(tag, tag), =(comment, comment))],"
                    + " joinType=[anti])\n"
                    + "        GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[REPLYOF]}],"
                    + " alias=[comment], startAlias=[message], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "          GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[POST,"
                    + " COMMENT]}], alias=[message], opt=[START], physicalOpt=[ITSELF])\n"
                    + "            GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASTAG, COMMENT,"
                    + " TAG), EdgeLabel(HASTAG, POST, TAG)]], alias=[_], startAlias=[tag],"
                    + " opt=[IN], physicalOpt=[VERTEX])\n"
                    + "              GraphLogicalSource(tableConfig=[{isAll=false, tables=[TAG]}],"
                    + " alias=[tag], fusedFilter=[[=(_.name, ?0)]], opt=[VERTEX])\n"
                    + "        GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[COMMENT]}],"
                    + " alias=[comment], opt=[START], physicalOpt=[ITSELF])\n"
                    + "          GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASTAG, COMMENT,"
                    + " TAG)]], alias=[_], startAlias=[tag], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "            GraphLogicalSource(tableConfig=[{isAll=false, tables=[TAG]}],"
                    + " alias=[tag], fusedFilter=[[=(_.name, ?0)]], opt=[VERTEX])",
                after.explain().trim());
    }

    @Test
    public void bi7_2_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH\n"
                                    + "  (tag:TAG {name:"
                                    + " $tag})<-[:HASTAG]-(message:POST|COMMENT),\n"
                                    + "  (message)<-[:REPLYOF]-(comment:COMMENT)-[:HASTAG]->(relatedTag:TAG)\n"
                                    + "WITH tag, comment, collect(relatedTag) as tags\n"
                                    + "WHERE NOT tag IN tags\n"
                                    + "UNWIND tags as relatedTag\n"
                                    + "RETURN\n"
                                    + "  relatedTag.name as name,\n"
                                    + "  count(DISTINCT comment) AS count\n"
                                    + "ORDER BY\n"
                                    + "  count DESC,\n"
                                    + "  name ASC\n"
                                    + "LIMIT 100",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalSort(sort0=[count], sort1=[name], dir0=[DESC], dir1=[ASC],"
                    + " fetch=[100])\n"
                    + "  GraphLogicalAggregate(keys=[{variables=[relatedTag.name],"
                    + " aliases=[name]}], values=[[{operands=[comment], aggFunction=COUNT,"
                    + " alias='count', distinct=true}]])\n"
                    + "    GraphLogicalUnfold(key=[tags], alias=[relatedTag])\n"
                    + "      LogicalFilter(condition=[NOT(IN(tag, tags))])\n"
                    + "        GraphLogicalAggregate(keys=[{variables=[tag, comment], aliases=[tag,"
                    + " comment]}], values=[[{operands=[relatedTag], aggFunction=COLLECT,"
                    + " alias='tags', distinct=false}]])\n"
                    + "          GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASTAG, COMMENT,"
                    + " TAG)]], alias=[relatedTag], startAlias=[comment], opt=[OUT],"
                    + " physicalOpt=[VERTEX])\n"
                    + "            GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[REPLYOF]}], alias=[comment], startAlias=[message], opt=[IN],"
                    + " physicalOpt=[VERTEX])\n"
                    + "              GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[POST,"
                    + " COMMENT]}], alias=[message], opt=[START], physicalOpt=[ITSELF])\n"
                    + "                GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASTAG, COMMENT,"
                    + " TAG), EdgeLabel(HASTAG, POST, TAG)]], alias=[_], startAlias=[tag],"
                    + " opt=[IN], physicalOpt=[VERTEX])\n"
                    + "                  GraphLogicalSource(tableConfig=[{isAll=false,"
                    + " tables=[TAG]}], alias=[tag], fusedFilter=[[=(_.name, ?0)]], opt=[VERTEX])",
                after.explain().trim());
    }

    @Test
    public void bi8_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH (tag:TAG {name: $tag})\n"
                                    + "CALL {\n"
                                    + "  OPTIONAL MATCH"
                                    + " (tag)<-[interest:HASINTEREST]-(person:PERSON)\n"
                                    + "  RETURN person, count(tag) as cnt1, 0 as cnt2\n"
                                    + "}\n"
                                    + "UNION\n"
                                    + "CALL {\n"
                                    + "  MATCH (tag)<-[:HASTAG]-(message:POST|COMMENT)\n"
                                    + "  OPTIONAL MATCH (message)-[:HASCREATOR]->(person:PERSON)\n"
                                    + "  WHERE $startDate < message.creationDate AND"
                                    + " message.creationDate < $endDate\n"
                                    + "  RETURN person, 0 as cnt1, count(tag) as cnt2              "
                                    + "                                                    \n"
                                    + "}\n"
                                    + "WITH person, sum(cnt1) * 100 + sum(cnt2) as score\n"
                                    + "CALL {\n"
                                    + "  MATCH (person)-[:KNOWS]-(person2:PERSON)\n"
                                    + "  RETURN person2 as person, 0 as score, sum(score) as"
                                    + " friendScore\n"
                                    + "}\n"
                                    + "UNION \n"
                                    + "CALL {\n"
                                    + "  RETURN person, score, 0 as friendScore\n"
                                    + "}\n"
                                    + "RETURN person.id as id, sum(score) as score,"
                                    + " sum(friendScore) as friendScore\n"
                                    + "ORDER BY\n"
                                    + "  score + friendScore DESC,\n"
                                    + "  id ASC\n"
                                    + "LIMIT 100",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "root:\n"
                    + "GraphLogicalProject(id=[id], score=[score], friendScore=[friendScore],"
                    + " isAppend=[false])\n"
                    + "  GraphLogicalSort(sort0=[$f0], sort1=[id], dir0=[DESC], dir1=[ASC],"
                    + " fetch=[100])\n"
                    + "    GraphLogicalProject($f0=[+(score, friendScore)], isAppend=[true])\n"
                    + "      GraphLogicalAggregate(keys=[{variables=[person.id], aliases=[id]}],"
                    + " values=[[{operands=[score], aggFunction=SUM, alias='score',"
                    + " distinct=false}, {operands=[friendScore], aggFunction=SUM,"
                    + " alias='friendScore', distinct=false}]])\n"
                    + "        LogicalUnion(all=[true])\n"
                    + "          GraphLogicalAggregate(keys=[{variables=[person2, $f0],"
                    + " aliases=[person, score]}], values=[[{operands=[score], aggFunction=SUM,"
                    + " alias='friendScore', distinct=false}]])\n"
                    + "            GraphLogicalProject($f0=[0], isAppend=[true])\n"
                    + "              GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[KNOWS]}], alias=[person2], startAlias=[person], opt=[BOTH],"
                    + " physicalOpt=[VERTEX])\n"
                    + "                CommonTableScan(table=[[common#573790069]])\n"
                    + "          GraphLogicalProject(person=[person], score=[score],"
                    + " friendScore=[0], isAppend=[false])\n"
                    + "            CommonTableScan(table=[[common#573790069]])\n"
                    + "common#573790069:\n"
                    + "GraphLogicalProject(person=[person], score=[+(*(EXPR$0, 100), EXPR$1)],"
                    + " isAppend=[false])\n"
                    + "  GraphLogicalAggregate(keys=[{variables=[person], aliases=[person]}],"
                    + " values=[[{operands=[cnt1], aggFunction=SUM, alias='EXPR$0',"
                    + " distinct=false}, {operands=[cnt2], aggFunction=SUM, alias='EXPR$1',"
                    + " distinct=false}]])\n"
                    + "    LogicalUnion(all=[true])\n"
                    + "      GraphLogicalAggregate(keys=[{variables=[person, $f0], aliases=[person,"
                    + " cnt2]}], values=[[{operands=[tag], aggFunction=COUNT, alias='cnt1',"
                    + " distinct=false}]])\n"
                    + "        GraphLogicalProject($f0=[0], isAppend=[true])\n"
                    + "          GraphLogicalGetV(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[person], opt=[START])\n"
                    + "            GraphLogicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[HASINTEREST]}], alias=[interest], startAlias=[tag], opt=[IN],"
                    + " optional=[true])\n"
                    + "              CommonTableScan(table=[[common#-95383647]])\n"
                    + "      GraphLogicalAggregate(keys=[{variables=[person, $f0], aliases=[person,"
                    + " cnt1]}], values=[[{operands=[tag], aggFunction=COUNT, alias='cnt2',"
                    + " distinct=false}]])\n"
                    + "        GraphLogicalProject($f0=[0], isAppend=[true])\n"
                    + "          GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASCREATOR, POST,"
                    + " PERSON), EdgeLabel(HASCREATOR, COMMENT, PERSON)]], alias=[person],"
                    + " startAlias=[message], opt=[OUT], physicalOpt=[VERTEX], optional=[true])\n"
                    + "            GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[POST,"
                    + " COMMENT]}], alias=[message], fusedFilter=[[AND(<(?0, _.creationDate),"
                    + " <(_.creationDate, ?1))]], opt=[START], physicalOpt=[ITSELF])\n"
                    + "              GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASTAG, COMMENT,"
                    + " TAG), EdgeLabel(HASTAG, POST, TAG)]], alias=[_], startAlias=[tag],"
                    + " opt=[IN], physicalOpt=[VERTEX])\n"
                    + "                CommonTableScan(table=[[common#-95383647]])\n"
                    + "common#-95383647:\n"
                    + "GraphLogicalSource(tableConfig=[{isAll=false, tables=[TAG]}], alias=[tag],"
                    + " fusedFilter=[[=(_.name, ?0)]], opt=[VERTEX])",
                com.alibaba.graphscope.common.ir.tools.Utils.toString(after).trim());
    }

    @Test
    public void bi9_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH\n"
                                    + "  (person:PERSON)<-[:HASCREATOR]-(post:POST)<-[:REPLYOF*0..7]-(message)\n"
                                    + "  WHERE\n"
                                    + "  post.creationDate >= $startDate AND post.creationDate <="
                                    + " $endDate AND\n"
                                    + "  message.creationDate >= $startDate AND"
                                    + " message.creationDate <= $endDate\n"
                                    + "WITH\n"
                                    + "  person,\n"
                                    + "  count(distinct post) as threadCnt,\n"
                                    + "  count(message) as msgCnt\n"
                                    + "RETURN\n"
                                    + "  person.id as id,\n"
                                    + "  person.firstName,\n"
                                    + "  person.lastName,\n"
                                    + "  threadCnt,\n"
                                    + "  msgCnt\n"
                                    + "  ORDER BY\n"
                                    + "  msgCnt DESC,\n"
                                    + "  id ASC\n"
                                    + "  LIMIT 100",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalSort(sort0=[msgCnt], sort1=[id], dir0=[DESC], dir1=[ASC],"
                    + " fetch=[100])\n"
                    + "  GraphLogicalProject(id=[person.id], firstName=[person.firstName],"
                    + " lastName=[person.lastName], threadCnt=[threadCnt], msgCnt=[msgCnt],"
                    + " isAppend=[false])\n"
                    + "    GraphLogicalAggregate(keys=[{variables=[person], aliases=[person]}],"
                    + " values=[[{operands=[post], aggFunction=COUNT, alias='threadCnt',"
                    + " distinct=true}, {operands=[message], aggFunction=COUNT, alias='msgCnt',"
                    + " distinct=false}]])\n"
                    + "      GraphLogicalGetV(tableConfig=[{isAll=false, tables=[POST, COMMENT]}],"
                    + " alias=[message], fusedFilter=[[AND(>=(_.creationDate, ?0),"
                    + " <=(_.creationDate, ?1))]], opt=[END])\n"
                    + "       "
                    + " GraphLogicalPathExpand(fused=[GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[REPLYOF]}], alias=[_], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "], fetch=[7], path_opt=[ARBITRARY], result_opt=[END_V], alias=[_],"
                    + " start_alias=[post])\n"
                    + "          GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASCREATOR, POST,"
                    + " PERSON)]], alias=[person], startAlias=[post], opt=[OUT],"
                    + " physicalOpt=[VERTEX])\n"
                    + "            GraphLogicalSource(tableConfig=[{isAll=false, tables=[POST]}],"
                    + " alias=[post], fusedFilter=[[AND(>=(_.creationDate, ?0), <=(_.creationDate,"
                    + " ?1))]], opt=[VERTEX])",
                after.explain().trim());
    }

    @Test
    public void bi10_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH shortestPath((p1:PERSON {id :"
                                    + " $personId})-[:KNOWS*1..4]-(expert:PERSON))\n"
                                    + "MATCH"
                                    + " (expert)-[:ISLOCATEDIN]->(:PLACE)-[:ISPARTOF]->(country:PLACE"
                                    + " {name: $country}),\n"
                                    + "     "
                                    + " (expert)<-[:HASCREATOR]-(message)-[:HASTAG]->(:TAG)-[:HASTYPE]->(:TAGCLASS"
                                    + " {name: $tagClass})\n"
                                    + "WITH DISTINCT expert, message\n"
                                    + "MATCH (message)-[:HASTAG]->(tag:TAG)\n"
                                    + "RETURN\n"
                                    + "  expert.id as id,\n"
                                    + "  tag.name as name,\n"
                                    + "  count(message) AS messageCount\n"
                                    + "ORDER BY\n"
                                    + "  messageCount DESC,\n"
                                    + "  name ASC,\n"
                                    + "  id ASC\n"
                                    + "LIMIT 100",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalSort(sort0=[messageCount], sort1=[name], sort2=[id], dir0=[DESC],"
                    + " dir1=[ASC], dir2=[ASC], fetch=[100])\n"
                    + "  GraphLogicalAggregate(keys=[{variables=[expert.id, tag.name], aliases=[id,"
                    + " name]}], values=[[{operands=[message], aggFunction=COUNT,"
                    + " alias='messageCount', distinct=false}]])\n"
                    + "    GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASTAG, POST, TAG),"
                    + " EdgeLabel(HASTAG, COMMENT, TAG), EdgeLabel(HASTAG, FORUM, TAG)]],"
                    + " alias=[tag], startAlias=[message], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "      GraphLogicalAggregate(keys=[{variables=[expert, message],"
                    + " aliases=[expert, message]}], values=[[]])\n"
                    + "        GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[TAGCLASS]}],"
                    + " alias=[_], fusedFilter=[[=(_.name, ?2)]], opt=[END],"
                    + " physicalOpt=[ITSELF])\n"
                    + "          GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[HASTYPE]}],"
                    + " alias=[_], startAlias=[PATTERN_VERTEX$9], opt=[OUT],"
                    + " physicalOpt=[VERTEX])\n"
                    + "            GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASTAG, COMMENT,"
                    + " TAG), EdgeLabel(HASTAG, POST, TAG)]], alias=[PATTERN_VERTEX$9],"
                    + " startAlias=[message], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "              GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[HASCREATOR]}], alias=[message], startAlias=[expert], opt=[IN],"
                    + " physicalOpt=[VERTEX])\n"
                    + "                GraphPhysicalGetV(tableConfig=[{isAll=false,"
                    + " tables=[PLACE]}], alias=[country], fusedFilter=[[=(_.name, ?1)]],"
                    + " opt=[END], physicalOpt=[ITSELF])\n"
                    + "                  GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[ISPARTOF]}], alias=[_], startAlias=[PATTERN_VERTEX$3], opt=[OUT],"
                    + " physicalOpt=[VERTEX])\n"
                    + "                    GraphPhysicalExpand(tableConfig=[[EdgeLabel(ISLOCATEDIN,"
                    + " PERSON, PLACE)]], alias=[PATTERN_VERTEX$3], startAlias=[expert], opt=[OUT],"
                    + " physicalOpt=[VERTEX])\n"
                    + "                      GraphLogicalGetV(tableConfig=[{isAll=false,"
                    + " tables=[PERSON]}], alias=[expert], opt=[END])\n"
                    + "                       "
                    + " GraphLogicalPathExpand(fused=[GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[KNOWS]}], alias=[_], opt=[BOTH], physicalOpt=[VERTEX])\n"
                    + "], offset=[1], fetch=[3], path_opt=[ANY_SHORTEST], result_opt=[END_V],"
                    + " alias=[_], start_alias=[p1])\n"
                    + "                          GraphLogicalSource(tableConfig=[{isAll=false,"
                    + " tables=[PERSON]}], alias=[p1], opt=[VERTEX], uniqueKeyFilters=[=(_.id,"
                    + " ?0)])",
                after.explain().trim());
    }

    @Test
    public void bi12_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "CALL {\n"
                                    + "  MATCH (p2:PERSON)\n"
                                    + "  WITH count(p2) as personCnt\n"
                                    + "  RETURN 0 as msgCnt, personCnt\n"
                                    + "}\n"
                                    + "UNION\n"
                                    + "CALL {\n"
                                    + "  MATCH"
                                    + " (person:PERSON)<-[:HASCREATOR]-(message:COMMENT|POST),\n"
                                    + "        (message)-[:REPLYOF * 0..30]->(post:POST)\n"
                                    + "  WHERE message.length > $lengthThreshold AND"
                                    + " message.creationDate > $startDate\n"
                                    + "        AND post.language IN [\"a\", \"b\"]\n"
                                    + "  WITH person, count(message) as msgCnt\n"
                                    + "  WITH msgCnt, count(person) as personCnt\n"
                                    + "  CALL {\n"
                                    + "    RETURN msgCnt, personCnt\n"
                                    + "  }\n"
                                    + "  UNION \n"
                                    + "  CALL {\n"
                                    + "    RETURN 0 as msgCnt, -1 * sum(personCnt) as personCnt\n"
                                    + "  }\n"
                                    + "  RETURN msgCnt, personCnt\n"
                                    + "}\n"
                                    + "RETURN msgCnt, sum(personCnt) as personCnt\n"
                                    + "ORDER BY\n"
                                    + "  personCnt DESC,\n"
                                    + "  msgCnt DESC",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "root:\n"
                    + "GraphLogicalSort(sort0=[personCnt], sort1=[msgCnt], dir0=[DESC],"
                    + " dir1=[DESC])\n"
                    + "  GraphLogicalAggregate(keys=[{variables=[msgCnt], aliases=[msgCnt]}],"
                    + " values=[[{operands=[personCnt], aggFunction=SUM, alias='personCnt',"
                    + " distinct=false}]])\n"
                    + "    LogicalUnion(all=[true])\n"
                    + "      GraphLogicalProject(msgCnt=[0], personCnt=[personCnt],"
                    + " isAppend=[false])\n"
                    + "        GraphLogicalAggregate(keys=[{variables=[], aliases=[]}],"
                    + " values=[[{operands=[p2], aggFunction=COUNT, alias='personCnt',"
                    + " distinct=false}]])\n"
                    + "          GraphLogicalSource(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[p2], opt=[VERTEX])\n"
                    + "      GraphLogicalProject(msgCnt=[msgCnt], personCnt=[personCnt],"
                    + " isAppend=[false])\n"
                    + "        LogicalUnion(all=[true])\n"
                    + "          GraphLogicalProject(msgCnt=[msgCnt], personCnt=[personCnt],"
                    + " isAppend=[false])\n"
                    + "            CommonTableScan(table=[[common#-410914212]])\n"
                    + "          GraphLogicalProject(msgCnt=[msgCnt], personCnt=[*(-(1), EXPR$0)],"
                    + " isAppend=[false])\n"
                    + "            GraphLogicalAggregate(keys=[{variables=[$f0],"
                    + " aliases=[msgCnt]}], values=[[{operands=[personCnt], aggFunction=SUM,"
                    + " alias='EXPR$0', distinct=false}]])\n"
                    + "              GraphLogicalProject($f0=[0], isAppend=[true])\n"
                    + "                CommonTableScan(table=[[common#-410914212]])\n"
                    + "common#-410914212:\n"
                    + "GraphLogicalAggregate(keys=[{variables=[msgCnt], aliases=[msgCnt]}],"
                    + " values=[[{operands=[person], aggFunction=COUNT, alias='personCnt',"
                    + " distinct=false}]])\n"
                    + "  GraphLogicalAggregate(keys=[{variables=[person], aliases=[person]}],"
                    + " values=[[{operands=[message], aggFunction=COUNT, alias='msgCnt',"
                    + " distinct=false}]])\n"
                    + "    GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[HASCREATOR]}],"
                    + " alias=[person], startAlias=[message], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "      GraphLogicalGetV(tableConfig=[{isAll=false, tables=[POST, COMMENT]}],"
                    + " alias=[message], fusedFilter=[[AND(>(_.length, ?0), >(_.creationDate,"
                    + " ?1))]], opt=[END])\n"
                    + "       "
                    + " GraphLogicalPathExpand(fused=[GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[REPLYOF]}], alias=[_], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "], fetch=[30], path_opt=[ARBITRARY], result_opt=[END_V], alias=[_],"
                    + " start_alias=[post])\n"
                    + "          GraphLogicalSource(tableConfig=[{isAll=false, tables=[POST]}],"
                    + " alias=[post], fusedFilter=[[SEARCH(_.language, Sarg[_UTF-8'a',"
                    + " _UTF-8'b']:CHAR(1) CHARACTER SET \"UTF-8\")]], opt=[VERTEX])",
                com.alibaba.graphscope.common.ir.tools.Utils.toString(after).trim());
    }

    @Test
    public void bi13_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH (country:PLACE {name:"
                                    + " $country})<-[:ISPARTOF]-(:PLACE)<-[:ISLOCATEDIN]-(zombie:PERSON)\n"
                                    + "WHERE zombie.creationDate < $endDate\n"
                                    + "OPTIONAL MATCH"
                                    + " (zombie)<-[:HASCREATOR]-(message:POST|COMMENT)\n"
                                    + "WHERE message.creationDate < $endDate\n"
                                    + "WITH\n"
                                    + "  country,\n"
                                    + "  zombie,\n"
                                    + "  gs.function.datetime($endDate) AS idate,\n"
                                    + "  zombie.creationDate AS zdate,\n"
                                    + "  count(message) AS messageCount\n"
                                    + "WITH\n"
                                    + "  country,\n"
                                    + "  zombie,\n"
                                    + "  12 * (idate.year  - zdate.year )\n"
                                    + "  + (idate.month - zdate.month)\n"
                                    + "  + 1 AS months,\n"
                                    + "  messageCount\n"
                                    + "WHERE messageCount / months < 1\n"
                                    + "WITH\n"
                                    + "  country,\n"
                                    + "  collect(zombie) AS zombies\n"
                                    + "UNWIND zombies AS zombie\n"
                                    + "OPTIONAL MATCH\n"
                                    + "  (zombie)<-[:HASCREATOR]-(message:POST|COMMENT)<-[:LIKES]-(likerPerson:PERSON)\n"
                                    + "WHERE likerPerson.creationDate < $endDate\n"
                                    + "WITH\n"
                                    + "  zombie,\n"
                                    + "  likerPerson,\n"
                                    + "  CASE WHEN likerPerson IN zombies THEN 1\n"
                                    + "  ELSE 0\n"
                                    + "  END as likerZombie\n"
                                    + "WITH\n"
                                    + "  zombie,\n"
                                    + "  count(likerZombie) AS zombieLikeCount,\n"
                                    + "  count(likerPerson) as totalLikeCount\n"
                                    + "RETURN\n"
                                    + "  zombie.id AS zid,\n"
                                    + "  zombieLikeCount,\n"
                                    + "  totalLikeCount,\n"
                                    + "  CASE totalLikeCount\n"
                                    + "    WHEN 0 THEN 0.0\n"
                                    + "    ELSE zombieLikeCount / totalLikeCount\n"
                                    + "    END AS zombieScore\n"
                                    + "ORDER BY\n"
                                    + "  zombieScore DESC,\n"
                                    + "  zid ASC\n"
                                    + "LIMIT 100",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalSort(sort0=[zombieScore], sort1=[zid], dir0=[DESC], dir1=[ASC],"
                    + " fetch=[100])\n"
                    + "  GraphLogicalProject(zid=[zombie.id], zombieLikeCount=[zombieLikeCount],"
                    + " totalLikeCount=[totalLikeCount], zombieScore=[CASE(=(totalLikeCount, 0),"
                    + " 0E0:DOUBLE, /(zombieLikeCount, totalLikeCount))], isAppend=[false])\n"
                    + "    GraphLogicalAggregate(keys=[{variables=[zombie], aliases=[zombie]}],"
                    + " values=[[{operands=[likerZombie], aggFunction=COUNT,"
                    + " alias='zombieLikeCount', distinct=false}, {operands=[likerPerson],"
                    + " aggFunction=COUNT, alias='totalLikeCount', distinct=false}]])\n"
                    + "      GraphLogicalProject(zombie=[zombie], likerPerson=[likerPerson],"
                    + " likerZombie=[CASE(IN(likerPerson, zombies), 1, 0)], isAppend=[false])\n"
                    + "        GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[likerPerson], fusedFilter=[[<(_.creationDate, ?1)]], opt=[START],"
                    + " physicalOpt=[ITSELF])\n"
                    + "          GraphPhysicalExpand(tableConfig=[[EdgeLabel(LIKES, PERSON, POST),"
                    + " EdgeLabel(LIKES, PERSON, COMMENT)]], alias=[_], opt=[IN],"
                    + " physicalOpt=[VERTEX], optional=[true])\n"
                    + "            GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[HASCREATOR]}], alias=[message], startAlias=[zombie], opt=[IN],"
                    + " physicalOpt=[VERTEX], optional=[true])\n"
                    + "              GraphLogicalUnfold(key=[zombies], alias=[zombie])\n"
                    + "                GraphLogicalAggregate(keys=[{variables=[country],"
                    + " aliases=[country]}], values=[[{operands=[zombie], aggFunction=COLLECT,"
                    + " alias='zombies', distinct=false}]])\n"
                    + "                  LogicalFilter(condition=[<(/(messageCount, months), 1)])\n"
                    + "                    GraphLogicalProject(country=[country], zombie=[zombie],"
                    + " months=[+(+(*(12, -(EXTRACT(FLAG(YEAR), idate), EXTRACT(FLAG(YEAR),"
                    + " zdate))), -(EXTRACT(FLAG(MONTH), idate), EXTRACT(FLAG(MONTH), zdate))),"
                    + " 1)], messageCount=[messageCount], isAppend=[false])\n"
                    + "                      GraphLogicalAggregate(keys=[{variables=[country,"
                    + " zombie, $f0, zombie.creationDate], aliases=[country, zombie, idate,"
                    + " zdate]}], values=[[{operands=[message], aggFunction=COUNT,"
                    + " alias='messageCount', distinct=false}]])\n"
                    + "                        GraphLogicalProject($f0=[gs.function.datetime(?1)],"
                    + " isAppend=[true])\n"
                    + "                          GraphPhysicalGetV(tableConfig=[{isAll=false,"
                    + " tables=[POST, COMMENT]}], alias=[message], fusedFilter=[[<(_.creationDate,"
                    + " ?1)]], opt=[START], physicalOpt=[ITSELF])\n"
                    + "                            GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[HASCREATOR]}], alias=[_], startAlias=[zombie], opt=[IN],"
                    + " physicalOpt=[VERTEX], optional=[true])\n"
                    + "                              GraphPhysicalGetV(tableConfig=[{isAll=false,"
                    + " tables=[PERSON]}], alias=[zombie], fusedFilter=[[<(_.creationDate, ?1)]],"
                    + " opt=[START], physicalOpt=[ITSELF])\n"
                    + "                               "
                    + " GraphPhysicalExpand(tableConfig=[[EdgeLabel(ISLOCATEDIN, PERSON, PLACE)]],"
                    + " alias=[_], startAlias=[PATTERN_VERTEX$1], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "                                 "
                    + " GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[ISPARTOF]}],"
                    + " alias=[PATTERN_VERTEX$1], startAlias=[country], opt=[IN],"
                    + " physicalOpt=[VERTEX])\n"
                    + "                                   "
                    + " GraphLogicalSource(tableConfig=[{isAll=false, tables=[PLACE]}],"
                    + " alias=[country], fusedFilter=[[=(_.name, ?0)]], opt=[VERTEX])",
                after.explain().trim());
    }

    @Test
    public void bi14_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH\n"
                                    + "  (country1:PLACE {name:"
                                    + " $country1})<-[:ISPARTOF]-(city1:PLACE)<-[:ISLOCATEDIN]-(person1:PERSON),\n"
                                    + "  (country2:PLACE {name:"
                                    + " $country2})<-[:ISPARTOF]-(city2:PLACE)<-[:ISLOCATEDIN]-(person2:PERSON),\n"
                                    + "  (person1)-[knows:KNOWS]-(person2)\n"
                                    + "\n"
                                    + "WITH person1, person2, knows, city1, 0 AS score\n"
                                    + "\n"
                                    + "WITH DISTINCT person1, person2, city1, knows,\n"
                                    + "              score + (CASE WHEN knows.interaction2_count=0"
                                    + " THEN 0\n"
                                    + "                ELSE  4\n"
                                    + "                END) AS score\n"
                                    + "\n"
                                    + "WITH DISTINCT person1, person2, city1, knows,\n"
                                    + "              score + (CASE WHEN knows.interaction1_count=0"
                                    + " THEN 0\n"
                                    + "                ELSE  1\n"
                                    + "                END) AS score\n"
                                    + "\n"
                                    + "OPTIONAL MATCH"
                                    + " (person1)-[:LIKES]->(m:COMMENT|POST)-[:HASCREATOR]->(person2)\n"
                                    + "\n"
                                    + "WITH DISTINCT person1, person2, city1,\n"
                                    + "              score + (CASE WHEN m IS NULL THEN 0\n"
                                    + "                ELSE 10\n"
                                    + "                END) AS score\n"
                                    + "\n"
                                    + "OPTIONAL MATCH"
                                    + " (person1)<-[:HASCREATOR]-(m:COMMENT|POST)<-[:LIKES]-(person2)\n"
                                    + "\n"
                                    + "WITH DISTINCT person1, person2, city1,\n"
                                    + "              score + (CASE WHEN m IS NULL THEN 0\n"
                                    + "                ELSE  1\n"
                                    + "                END) AS score\n"
                                    + "ORDER BY\n"
                                    + "  city1.name  ASC,\n"
                                    + "  score DESC,\n"
                                    + "  person1.id ASC,\n"
                                    + "  person2.id ASC\n"
                                    + "WITH city1, head(collect(person1.id)) AS person1Id,"
                                    + " head(collect(person2.id)) AS person2Id,"
                                    + " head(collect(score)) AS score\n"
                                    + "RETURN\n"
                                    + "  person1Id,\n"
                                    + "  person2Id,\n"
                                    + "  city1.name AS cityName,\n"
                                    + "  score\n"
                                    + "  ORDER BY\n"
                                    + "  score DESC,\n"
                                    + "  person1Id ASC,\n"
                                    + "  person2Id ASC\n"
                                    + "  LIMIT 100",
                                builder)
                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "root:\n"
                    + "GraphLogicalSort(sort0=[score], sort1=[person1Id], sort2=[person2Id],"
                    + " dir0=[DESC], dir1=[ASC], dir2=[ASC], fetch=[100])\n"
                    + "  GraphLogicalProject(person1Id=[person1Id], person2Id=[person2Id],"
                    + " cityName=[city1.name], score=[score], isAppend=[false])\n"
                    + "    GraphLogicalAggregate(keys=[{variables=[city1], aliases=[city1]}],"
                    + " values=[[{operands=[person1.id], aggFunction=FIRST_VALUE,"
                    + " alias='person1Id', distinct=false}, {operands=[person2.id],"
                    + " aggFunction=FIRST_VALUE, alias='person2Id', distinct=false},"
                    + " {operands=[score], aggFunction=FIRST_VALUE, alias='score',"
                    + " distinct=false}]])\n"
                    + "      GraphLogicalSort(sort0=[city1.name], sort1=[score],"
                    + " sort2=[person1.id], sort3=[person2.id], dir0=[ASC], dir1=[DESC],"
                    + " dir2=[ASC], dir3=[ASC])\n"
                    + "        GraphLogicalAggregate(keys=[{variables=[person1, person2, city1,"
                    + " $f0], aliases=[person1, person2, city1, score]}], values=[[]])\n"
                    + "          GraphLogicalProject($f0=[+(score, CASE(IS NULL(m), 0, 1))],"
                    + " isAppend=[true])\n"
                    + "            LogicalJoin(condition=[AND(=(person1, person1), =(person2,"
                    + " person2))], joinType=[left])\n"
                    + "              CommonTableScan(table=[[common#-883215245]])\n"
                    + "              GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[LIKES]}], alias=[person2], opt=[IN], physicalOpt=[VERTEX],"
                    + " optional=[true])\n"
                    + "                GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[HASCREATOR]}], alias=[m], startAlias=[person1], opt=[IN],"
                    + " physicalOpt=[VERTEX], optional=[true])\n"
                    + "                  GraphLogicalAggregate(keys=[{variables=[person1],"
                    + " aliases=[person1]}], values=[[]])\n"
                    + "                    CommonTableScan(table=[[common#-883215245]])\n"
                    + "common#-883215245:\n"
                    + "GraphLogicalAggregate(keys=[{variables=[person1, person2, city1, $f0],"
                    + " aliases=[person1, person2, city1, score]}], values=[[]])\n"
                    + "  GraphLogicalProject($f0=[+(score, CASE(IS NULL(m), 0, 10))],"
                    + " isAppend=[true])\n"
                    + "    LogicalJoin(condition=[AND(=(person1, person1), =(person2, person2))],"
                    + " joinType=[left])\n"
                    + "      CommonTableScan(table=[[common#-1994418361]])\n"
                    + "      GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[HASCREATOR]}],"
                    + " alias=[person2], opt=[OUT], physicalOpt=[VERTEX], optional=[true])\n"
                    + "        GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[LIKES]}],"
                    + " alias=[m], startAlias=[person1], opt=[OUT], physicalOpt=[VERTEX],"
                    + " optional=[true])\n"
                    + "          GraphLogicalAggregate(keys=[{variables=[person1],"
                    + " aliases=[person1]}], values=[[]])\n"
                    + "            CommonTableScan(table=[[common#-1994418361]])\n"
                    + "common#-1994418361:\n"
                    + "GraphLogicalAggregate(keys=[{variables=[person1, person2, city1, knows,"
                    + " $f0], aliases=[person1, person2, city1, knows, score]}], values=[[]])\n"
                    + "  GraphLogicalProject($f0=[+(score, CASE(=(knows.interaction1_count, 0), 0,"
                    + " 1))], isAppend=[true])\n"
                    + "    GraphLogicalAggregate(keys=[{variables=[person1, person2, city1, knows,"
                    + " $f0], aliases=[person1, person2, city1, knows, score]}], values=[[]])\n"
                    + "      GraphLogicalProject($f0=[+(score, CASE(=(knows.interaction2_count, 0),"
                    + " 0, 4))], isAppend=[true])\n"
                    + "        GraphLogicalProject(person1=[person1], person2=[person2],"
                    + " knows=[knows], city1=[city1], score=[0], isAppend=[false])\n"
                    + "          GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[PLACE]}],"
                    + " alias=[country1], fusedFilter=[[=(_.name, ?0)]], opt=[END],"
                    + " physicalOpt=[ITSELF])\n"
                    + "            GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[ISPARTOF]}], alias=[_], startAlias=[city1], opt=[OUT],"
                    + " physicalOpt=[VERTEX])\n"
                    + "              GraphPhysicalExpand(tableConfig=[[EdgeLabel(ISLOCATEDIN,"
                    + " PERSON, PLACE)]], alias=[city1], startAlias=[person1], opt=[OUT],"
                    + " physicalOpt=[VERTEX])\n"
                    + "                GraphLogicalGetV(tableConfig=[{isAll=false,"
                    + " tables=[PERSON]}], alias=[person1], opt=[OTHER])\n"
                    + "                  GraphLogicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[KNOWS]}], alias=[knows], startAlias=[person2], opt=[BOTH])\n"
                    + "                    GraphPhysicalGetV(tableConfig=[{isAll=false,"
                    + " tables=[PERSON]}], alias=[person2], opt=[START], physicalOpt=[ITSELF])\n"
                    + "                     "
                    + " GraphPhysicalExpand(tableConfig=[[EdgeLabel(ISLOCATEDIN, PERSON, PLACE)]],"
                    + " alias=[_], startAlias=[city2], opt=[IN], physicalOpt=[VERTEX])\n"
                    + "                        GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[ISPARTOF]}], alias=[city2], startAlias=[country2], opt=[IN],"
                    + " physicalOpt=[VERTEX])\n"
                    + "                          GraphLogicalSource(tableConfig=[{isAll=false,"
                    + " tables=[PLACE]}], alias=[country2], fusedFilter=[[=(_.name, ?1)]],"
                    + " opt=[VERTEX])",
                com.alibaba.graphscope.common.ir.tools.Utils.toString(after).trim());
    }

    @Test
    public void bi15_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH\n"
                                    + "  (person1:PERSON {id: $person1Id})\n"
                                    + "WITH person1 AS person1\n"
                                    + "MATCH\n"
                                    + "  (person2:PERSON {id: $person2Id})\n"
                                    + "CALL shortestPath.dijkstra.stream(\n"
                                    + "'(personA:PERSON)-[knows:KNOWS]-(personB:PERSON)',\n"
                                    + "person1,\n"
                                    + "person2,\n"
                                    + "'OPTIONAL MATCH"
                                    + " (personA)<-[:HASCREATOR]-(m1:POST|COMMENT)-[r:REPLYOF]-(m2:POST|COMMENT)-[:HASCREATOR]->(personB)\n"
                                    + " OPTIONAL MATCH"
                                    + " (m1)-[:REPLYOF*0..]->(:POST)<-[:CONTAINEROF]-(forum:FORUM)\n"
                                    + "  WHERE forum.creationDate >= $startDate AND"
                                    + " forum.creationDate <= $endDate\n"
                                    + " WITH sum(CASE forum IS NOT NULL\n"
                                    + "  WHEN true THEN\n"
                                    + "      CASE (m1:POST OR m2:POST) WHEN true THEN 1.0\n"
                                    + "      ELSE 0.5 END\n"
                                    + "  ELSE 0.0 END)'\n"
                                    + ")\n"
                                    + "WITH totalCost AS totalWeight\n"
                                    + "RETURN totalWeight\n"
                                    + "  ORDER BY totalWeight\n"
                                    + "  LIMIT 1",
                                builder,
                                irMeta)
                        .getRegularQuery();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalSort(sort0=[totalWeight], dir0=[ASC], fetch=[1])\n"
                    + "  GraphLogicalProject(totalWeight=[totalWeight], isAppend=[false])\n"
                    + "    GraphLogicalProject(totalWeight=[totalCost], isAppend=[false])\n"
                    + "     "
                    + " GraphProcedureCall(procedure=[shortestPath.dijkstra.stream(_UTF-8'(personA:PERSON)-[knows:KNOWS]-(personB:PERSON)',"
                    + " person1, person2, _UTF-8'OPTIONAL MATCH"
                    + " (personA)<-[:HASCREATOR]-(m1:POST|COMMENT)-[r:REPLYOF]-(m2:POST|COMMENT)-[:HASCREATOR]->(personB)\n"
                    + " OPTIONAL MATCH (m1)-[:REPLYOF*0..]->(:POST)<-[:CONTAINEROF]-(forum:FORUM)\n"
                    + "  WHERE forum.creationDate >= $startDate AND forum.creationDate <="
                    + " $endDate\n"
                    + " WITH sum(CASE forum IS NOT NULL\n"
                    + "  WHEN true THEN\n"
                    + "      CASE (m1:POST OR m2:POST) WHEN true THEN 1.0\n"
                    + "      ELSE 0.5 END\n"
                    + "  ELSE 0.0 END)')])\n"
                    + "        LogicalJoin(condition=[true], joinType=[inner])\n"
                    + "          GraphLogicalProject(person1=[person1], isAppend=[false])\n"
                    + "            GraphLogicalSource(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[person1], opt=[VERTEX], uniqueKeyFilters=[=(_.id, ?0)])\n"
                    + "          GraphLogicalSource(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[person2], opt=[VERTEX], uniqueKeyFilters=[=(_.id, ?1)])",
                after.explain().trim());
    }

    @Test
    public void bi16_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "CALL {\n"
                                    + "  MATCH"
                                    + " (person:PERSON)<-[:HASCREATOR]-(msg:COMMENT|POST)-[:HASTAG]->(tag:TAG"
                                    + " {name:$tagA})\n"
                                    + "  WHERE gs.function.date32(msg.creationDate) = $dateA\n"
                                    + "  WITH person, count(msg) as aCount\n"
                                    + "\n"
                                    + "  CALL {\n"
                                    + "    RETURN person, aCount, -1 * $maxKnows as degree\n"
                                    + "  }\n"
                                    + "  UNION                                        \n"
                                    + "  CALL {\n"
                                    + "    MATCH (person)-[:KNOWS]-(person2:PERSON)\n"
                                    + "    RETURN person2 as person, 0 as aCount, count(person) as"
                                    + " degree\n"
                                    + "  }\n"
                                    + "\n"
                                    + "  WITH person, sum(aCount) as aCount, sum(degree) as"
                                    + " degree\n"
                                    + "  WHERE degree <= 0\n"
                                    + "  RETURN person, aCount, 0 as bCount\n"
                                    + "}\n"
                                    + "UNION\n"
                                    + "CALL {\n"
                                    + "  MATCH"
                                    + " (person:PERSON)<-[:HASCREATOR]-(msg:COMMENT|POST)-[:HASTAG]->(tag:TAG"
                                    + " {name:$tagB})\n"
                                    + "  WHERE gs.function.date32(msg.creationDate) = $dateB\n"
                                    + "  WITH person, count(msg) as bCount\n"
                                    + "\n"
                                    + "  CALL {\n"
                                    + "    RETURN person, bCount, -1 * $maxKnows as degree\n"
                                    + "  }\n"
                                    + "  UNION                                        \n"
                                    + "  CALL {\n"
                                    + "    MATCH (person)-[:KNOWS]-(person2:PERSON)\n"
                                    + "    RETURN person2 as person, 0 as bCount, count(person) as"
                                    + " degree\n"
                                    + "  }\n"
                                    + "\n"
                                    + "  WITH person, sum(bCount) as bCount, sum(degree) as"
                                    + " degree\n"
                                    + "  WHERE degree <= 0\n"
                                    + "  RETURN person, 0 as aCount, bCount\n"
                                    + "}\n"
                                    + "Return person.id as id, sum(aCount) as aCount, sum(bCount)"
                                    + " as bCount\n"
                                    + "ORDER BY aCount + bCount DESC, id ASC\n"
                                    + "LIMIT 20;",
                                builder,
                                irMeta)
                        .getRegularQuery();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "root:\n"
                    + "GraphLogicalProject(id=[id], aCount=[aCount], bCount=[bCount],"
                    + " isAppend=[false])\n"
                    + "  GraphLogicalSort(sort0=[$f0], sort1=[id], dir0=[DESC], dir1=[ASC],"
                    + " fetch=[20])\n"
                    + "    GraphLogicalProject($f0=[+(aCount, bCount)], isAppend=[true])\n"
                    + "      GraphLogicalAggregate(keys=[{variables=[person.id], aliases=[id]}],"
                    + " values=[[{operands=[aCount], aggFunction=SUM, alias='aCount',"
                    + " distinct=false}, {operands=[bCount], aggFunction=SUM, alias='bCount',"
                    + " distinct=false}]])\n"
                    + "        LogicalUnion(all=[true])\n"
                    + "          GraphLogicalProject(person=[person], aCount=[aCount], bCount=[0],"
                    + " isAppend=[false])\n"
                    + "            LogicalFilter(condition=[<=(degree, 0)])\n"
                    + "              GraphLogicalAggregate(keys=[{variables=[person],"
                    + " aliases=[person]}], values=[[{operands=[aCount], aggFunction=SUM,"
                    + " alias='aCount', distinct=false}, {operands=[degree], aggFunction=SUM,"
                    + " alias='degree', distinct=false}]])\n"
                    + "                LogicalUnion(all=[true])\n"
                    + "                  GraphLogicalProject(person=[person], aCount=[aCount],"
                    + " degree=[*(-(1), ?0)], isAppend=[false])\n"
                    + "                    CommonTableScan(table=[[common#-1354776032]])\n"
                    + "                  GraphLogicalAggregate(keys=[{variables=[person2, $f0],"
                    + " aliases=[person, aCount]}], values=[[{operands=[person], aggFunction=COUNT,"
                    + " alias='degree', distinct=false}]])\n"
                    + "                    GraphLogicalProject($f0=[0], isAppend=[true])\n"
                    + "                      GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[KNOWS]}], alias=[person2], startAlias=[person], opt=[BOTH],"
                    + " physicalOpt=[VERTEX])\n"
                    + "                        CommonTableScan(table=[[common#-1354776032]])\n"
                    + "          GraphLogicalProject(person=[person], aCount=[0], bCount=[bCount],"
                    + " isAppend=[false])\n"
                    + "            LogicalFilter(condition=[<=(degree, 0)])\n"
                    + "              GraphLogicalAggregate(keys=[{variables=[person],"
                    + " aliases=[person]}], values=[[{operands=[bCount], aggFunction=SUM,"
                    + " alias='bCount', distinct=false}, {operands=[degree], aggFunction=SUM,"
                    + " alias='degree', distinct=false}]])\n"
                    + "                LogicalUnion(all=[true])\n"
                    + "                  GraphLogicalProject(person=[person], bCount=[bCount],"
                    + " degree=[*(-(1), ?0)], isAppend=[false])\n"
                    + "                    CommonTableScan(table=[[common#1427089185]])\n"
                    + "                  GraphLogicalAggregate(keys=[{variables=[person2, $f0],"
                    + " aliases=[person, bCount]}], values=[[{operands=[person], aggFunction=COUNT,"
                    + " alias='degree', distinct=false}]])\n"
                    + "                    GraphLogicalProject($f0=[0], isAppend=[true])\n"
                    + "                      GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[KNOWS]}], alias=[person2], startAlias=[person], opt=[BOTH],"
                    + " physicalOpt=[VERTEX])\n"
                    + "                        CommonTableScan(table=[[common#1427089185]])\n"
                    + "common#-1354776032:\n"
                    + "GraphLogicalAggregate(keys=[{variables=[person], aliases=[person]}],"
                    + " values=[[{operands=[msg], aggFunction=COUNT, alias='aCount',"
                    + " distinct=false}]])\n"
                    + "  GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[HASCREATOR]}],"
                    + " alias=[person], startAlias=[msg], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "    GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[POST, COMMENT]}],"
                    + " alias=[msg], fusedFilter=[[=(gs.function.date32(_.creationDate), ?1)]],"
                    + " opt=[START], physicalOpt=[ITSELF])\n"
                    + "      GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASTAG, COMMENT, TAG),"
                    + " EdgeLabel(HASTAG, POST, TAG)]], alias=[_], startAlias=[tag], opt=[IN],"
                    + " physicalOpt=[VERTEX])\n"
                    + "        GraphLogicalSource(tableConfig=[{isAll=false, tables=[TAG]}],"
                    + " alias=[tag], fusedFilter=[[=(_.name, ?0)]], opt=[VERTEX])\n"
                    + "common#1427089185:\n"
                    + "GraphLogicalAggregate(keys=[{variables=[person], aliases=[person]}],"
                    + " values=[[{operands=[msg], aggFunction=COUNT, alias='bCount',"
                    + " distinct=false}]])\n"
                    + "  GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[HASCREATOR]}],"
                    + " alias=[person], startAlias=[msg], opt=[OUT], physicalOpt=[VERTEX])\n"
                    + "    GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[POST, COMMENT]}],"
                    + " alias=[msg], fusedFilter=[[=(gs.function.date32(_.creationDate), ?3)]],"
                    + " opt=[START], physicalOpt=[ITSELF])\n"
                    + "      GraphPhysicalExpand(tableConfig=[[EdgeLabel(HASTAG, COMMENT, TAG),"
                    + " EdgeLabel(HASTAG, POST, TAG)]], alias=[_], startAlias=[tag], opt=[IN],"
                    + " physicalOpt=[VERTEX])\n"
                    + "        GraphLogicalSource(tableConfig=[{isAll=false, tables=[TAG]}],"
                    + " alias=[tag], fusedFilter=[[=(_.name, ?2)]], opt=[VERTEX])",
                com.alibaba.graphscope.common.ir.tools.Utils.toString(after).trim());
    }

    @Test
    public void bi18_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH (tag:TAG {name:"
                                    + " $tag})<-[:HASINTEREST]-(person1:PERSON)-[:KNOWS]-(friend:PERSON)\n"
                                    + "WITH friend, collect(person1) as persons\n"
                                    + "UNWIND persons as person1\n"
                                    + "UNWIND persons as person2\n"
                                    + "WITH person1, person2, friend\n"
                                    + "WHERE person1 <> person2 AND NOT"
                                    + " (person1)-[:KNOWS]-(person2)\n"
                                    + "RETURN person1.id AS person1Id, person2.id AS person2Id,"
                                    + " count(DISTINCT friend) AS friendCount\n"
                                    + "ORDER BY friendCount DESC, person1Id ASC, person2Id ASC\n"
                                    + "LIMIT 20",
                                builder,
                                irMeta)
                        .getRegularQuery();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "root:\n"
                    + "GraphLogicalSort(sort0=[friendCount], sort1=[person1Id], sort2=[person2Id],"
                    + " dir0=[DESC], dir1=[ASC], dir2=[ASC], fetch=[20])\n"
                    + "  GraphLogicalAggregate(keys=[{variables=[person1.id, person2.id],"
                    + " aliases=[person1Id, person2Id]}], values=[[{operands=[friend],"
                    + " aggFunction=COUNT, alias='friendCount', distinct=true}]])\n"
                    + "    LogicalJoin(condition=[AND(=(person1, person1), =(person2, person2))],"
                    + " joinType=[anti])\n"
                    + "      CommonTableScan(table=[[common#2137110854]])\n"
                    + "      GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[KNOWS]}],"
                    + " alias=[person2], startAlias=[person1], opt=[BOTH], physicalOpt=[VERTEX])\n"
                    + "        GraphLogicalAggregate(keys=[{variables=[person1],"
                    + " aliases=[person1]}], values=[[]])\n"
                    + "          CommonTableScan(table=[[common#2137110854]])\n"
                    + "common#2137110854:\n"
                    + "LogicalFilter(condition=[<>(person1, person2)])\n"
                    + "  GraphLogicalProject(person1=[person1], person2=[person2], friend=[friend],"
                    + " isAppend=[false])\n"
                    + "    GraphLogicalUnfold(key=[persons], alias=[person2])\n"
                    + "      GraphLogicalUnfold(key=[persons], alias=[person1])\n"
                    + "        GraphLogicalAggregate(keys=[{variables=[friend], aliases=[friend]}],"
                    + " values=[[{operands=[person1], aggFunction=COLLECT, alias='persons',"
                    + " distinct=false}]])\n"
                    + "          GraphPhysicalExpand(tableConfig=[{isAll=false, tables=[KNOWS]}],"
                    + " alias=[friend], startAlias=[person1], opt=[BOTH], physicalOpt=[VERTEX])\n"
                    + "            GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[HASINTEREST]}], alias=[person1], startAlias=[tag], opt=[IN],"
                    + " physicalOpt=[VERTEX])\n"
                    + "              GraphLogicalSource(tableConfig=[{isAll=false, tables=[TAG]}],"
                    + " alias=[tag], fusedFilter=[[=(_.name, ?0)]], opt=[VERTEX])",
                com.alibaba.graphscope.common.ir.tools.Utils.toString(after).trim());
    }

    @Test
    public void bi19_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH\n"
                                    + "  (person1:PERSON)-[:ISLOCATEDIN]->(city1Id:PLACE {id:"
                                    + " $city1Id})\n"
                                    + "WITH person1 AS person1\n"
                                    + "MATCH\n"
                                    + "  (person2:PERSON)-[:ISLOCATEDIN]->(city2Id:PLACE {id:"
                                    + " $city2Id})\n"
                                    + "CALL shortestPath.dijkstra.stream(\n"
                                    + "'(personA:PERSON)-[knows:KNOWS]-(personB:PERSON)',\n"
                                    + "person1,\n"
                                    + "person2,\n"
                                    + "'max(round(40 - sqrt(knows.interaction1_count +"
                                    + " knows.interaction2_count)), 1)'\n"
                                    + ")\n"
                                    + "WITH person1.id AS person1Id, person2.id AS person2Id,"
                                    + " totalCost AS totalWeight \n"
                                    + "RETURN totalWeight, collect(person1Id, person2Id) AS"
                                    + " personIds\n"
                                    + "ORDER BY totalWeight ASC\n"
                                    + "LIMIT 1",
                                builder,
                                irMeta)
                        .getRegularQuery();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalSort(sort0=[totalWeight], dir0=[ASC], fetch=[1])\n"
                    + "  GraphLogicalAggregate(keys=[{variables=[totalWeight],"
                    + " aliases=[totalWeight]}], values=[[{operands=[person1Id, person2Id],"
                    + " aggFunction=COLLECT, alias='personIds', distinct=false}]])\n"
                    + "    GraphLogicalProject(person1Id=[person1.id], person2Id=[person2.id],"
                    + " totalWeight=[totalCost], isAppend=[false])\n"
                    + "     "
                    + " GraphProcedureCall(procedure=[shortestPath.dijkstra.stream(_UTF-8'(personA:PERSON)-[knows:KNOWS]-(personB:PERSON)',"
                    + " person1, person2, _UTF-8'max(round(40 - sqrt(knows.interaction1_count +"
                    + " knows.interaction2_count)), 1)')])\n"
                    + "        LogicalJoin(condition=[true], joinType=[inner])\n"
                    + "          GraphLogicalProject(person1=[person1], isAppend=[false])\n"
                    + "            GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[person1], opt=[START], physicalOpt=[ITSELF])\n"
                    + "              GraphPhysicalExpand(tableConfig=[[EdgeLabel(ISLOCATEDIN,"
                    + " PERSON, PLACE)]], alias=[_], startAlias=[city1Id], opt=[IN],"
                    + " physicalOpt=[VERTEX])\n"
                    + "                GraphLogicalSource(tableConfig=[{isAll=false,"
                    + " tables=[PLACE]}], alias=[city1Id], opt=[VERTEX], uniqueKeyFilters=[=(_.id,"
                    + " ?0)])\n"
                    + "          GraphPhysicalGetV(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[person2], opt=[START], physicalOpt=[ITSELF])\n"
                    + "            GraphPhysicalExpand(tableConfig=[[EdgeLabel(ISLOCATEDIN, PERSON,"
                    + " PLACE)]], alias=[_], startAlias=[city2Id], opt=[IN],"
                    + " physicalOpt=[VERTEX])\n"
                    + "              GraphLogicalSource(tableConfig=[{isAll=false,"
                    + " tables=[PLACE]}], alias=[city2Id], opt=[VERTEX], uniqueKeyFilters=[=(_.id,"
                    + " ?1)])",
                after.explain().trim());
    }

    @Test
    public void bi20_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH\n"
                                    + "  (company:ORGANISATION {name:"
                                    + " $company})<-[:WORKAT]-(person1:PERSON)\n"
                                    + "WITH person1 AS person1\n"
                                    + "MATCH\n"
                                    + "  (person2:PERSON {id: $person2Id})\n"
                                    + "CALL shortestPath.dijkstra.stream(\n"
                                    + "'(personA:PERSON)-[knows:KNOWS]-(personB:PERSON)',\n"
                                    + "person1,\n"
                                    + "person2,\n"
                                    + "'knows.bi20_precompute'\n"
                                    + ")\n"
                                    + "WITH person1.id AS person1Id, person2.id AS person2Id,"
                                    + " totalCost AS totalWeight\n"
                                    + "RETURN person1Id, person2Id, totalWeight\n"
                                    + "  ORDER BY totalWeight ASC, person1Id ASC, person2Id ASC\n"
                                    + "  LIMIT 1",
                                builder,
                                irMeta)
                        .getRegularQuery();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "GraphLogicalSort(sort0=[totalWeight], sort1=[person1Id], sort2=[person2Id],"
                    + " dir0=[ASC], dir1=[ASC], dir2=[ASC], fetch=[1])\n"
                    + "  GraphLogicalProject(person1Id=[person1Id], person2Id=[person2Id],"
                    + " totalWeight=[totalWeight], isAppend=[false])\n"
                    + "    GraphLogicalProject(person1Id=[person1.id], person2Id=[person2.id],"
                    + " totalWeight=[totalCost], isAppend=[false])\n"
                    + "     "
                    + " GraphProcedureCall(procedure=[shortestPath.dijkstra.stream(_UTF-8'(personA:PERSON)-[knows:KNOWS]-(personB:PERSON)',"
                    + " person1, person2, _UTF-8'knows.bi20_precompute')])\n"
                    + "        LogicalJoin(condition=[true], joinType=[inner])\n"
                    + "          GraphLogicalProject(person1=[person1], isAppend=[false])\n"
                    + "            GraphPhysicalExpand(tableConfig=[{isAll=false,"
                    + " tables=[WORKAT]}], alias=[person1], startAlias=[company], opt=[IN],"
                    + " physicalOpt=[VERTEX])\n"
                    + "              GraphLogicalSource(tableConfig=[{isAll=false,"
                    + " tables=[ORGANISATION]}], alias=[company], fusedFilter=[[=(_.name, ?0)]],"
                    + " opt=[VERTEX])\n"
                    + "          GraphLogicalSource(tableConfig=[{isAll=false, tables=[PERSON]}],"
                    + " alias=[person2], opt=[VERTEX], uniqueKeyFilters=[=(_.id, ?1)])",
                after.explain().trim());
    }
}

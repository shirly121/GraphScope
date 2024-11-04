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

package com.alibaba.graphscope.cypher.antlr4.write;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.ir.meta.IrMeta;
import com.alibaba.graphscope.common.ir.planner.GraphIOProcessor;
import com.alibaba.graphscope.common.ir.planner.GraphRelOptimizer;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.google.common.collect.ImmutableMap;

import org.apache.calcite.rel.RelNode;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class ICUpdateTest {
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
                                "FilterIntoJoinRule, FilterMatchRule,"
                                        + " ExtendIntersectRule, ExpandGetVFusionRule"));
        optimizer = new GraphRelOptimizer(configs);
        irMeta =
                com.alibaba.graphscope.common.ir.Utils.mockIrMeta(
                        "schema/ldbc_schema_exp_hierarchy.json",
                        "statistics/ldbc30_hierarchy_statistics.json",
                        optimizer.getGlogueHolder());
    }

    @Test
    public void iu_1_test() {
        String query =
                "WITH\n"
                        + "  $personId as personId,\n"
                        + "  $personFirstName as firstName,\n"
                        + "  $personLastName as lastName,\n"
                        + "  $gender as gender,\n"
                        + "  $birthday as birthday,\n"
                        + "  $creationDate as creationDate,\n"
                        + "  $locationIP as locationIP,\n"
                        + "  $browserUsed as browserUsed,\n"
                        + "  $cityId as cityId,\n"
                        + "  $tagIds as tagIds,\n"
                        + "  $studyAts as studyAts,\n"
                        + "  $workAts as workAts\n"
                        + "CREATE (p:PERSON {\n"
                        + "  id: personId,\n"
                        + "  firstName: firstName,\n"
                        + "  lastName: lastName,\n"
                        + "  gender: gender,\n"
                        + "  birthday: birthday,\n"
                        + "  creationDate: creationDate,\n"
                        + "  locationIP: locationIP,\n"
                        + "  browserUsed: browserUsed\n"
                        + "})\n"
                        + "CREATE (p:PERSON {id: personId})-[:ISLOCATEDIN]->(c:CITY {id: cityId})\n"
                        + "UNWIND tagIds AS tagId\n"
                        + "CREATE (p:PERSON {id: personId})-[:HASINTEREST]->(t:TAG {id: tagId})\n"
                        + "WITH distinct personId, studyAts, workAts\n"
                        + "UNWIND studyAts AS studyAt\n"
                        + "WITH personId, workAts, gs.function.first(studyAt) as studyAt_0,"
                        + " gs.function.second(studyAt) as studyAt_1\n"
                        + "CREATE (p:PERSON {id:personId})-[:STUDYAT"
                        + " {classYear:studyAt_1}]->(u:UNIVERSITY {id:studyAt_0})\n"
                        + "WITH distinct personId, workAts\n"
                        + "UNWIND workAts AS workAt\n"
                        + "WITH personId, gs.function.first(workAt) as workAt_0,"
                        + " gs.function.second(workAt) as workAt_1\n"
                        + "CREATE (p:PERSON {id: personId})-[:WORKAT {workFrom:"
                        + " workAt_1}]->(comp:COMPANY {id: workAt_0})";
        GraphBuilder builder =
                com.alibaba.graphscope.common.ir.Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before = com.alibaba.graphscope.cypher.antlr4.Utils.eval(query, builder).build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "Insert(operation=[INSERT], target=[Edge{table=[WORKAT],"
                    + " mappings=FieldMappings{mappings=[Entry{source=workAt_1,"
                    + " target=_.workFrom}]}, srcVertex=Vertex{table=[PERSON],"
                    + " mappings=FieldMappings{mappings=[Entry{source=personId, target=_.id}]}},"
                    + " dstVertex=Vertex{table=[COMPANY],"
                    + " mappings=FieldMappings{mappings=[Entry{source=workAt_0, target=_.id}]}}}],"
                    + " alias=[_])\n"
                    + "  GraphLogicalProject(personId=[personId],"
                    + " workAt_0=[gs.function.first(workAt)],"
                    + " workAt_1=[gs.function.second(workAt)], isAppend=[false])\n"
                    + "    GraphLogicalUnfold(key=[workAts], alias=[workAt])\n"
                    + "      GraphLogicalAggregate(keys=[{variables=[personId, workAts],"
                    + " aliases=[personId, workAts]}], values=[[]])\n"
                    + "        Insert(operation=[INSERT], target=[Edge{table=[STUDYAT],"
                    + " mappings=FieldMappings{mappings=[Entry{source=studyAt_1,"
                    + " target=_.classYear}]}, srcVertex=Vertex{table=[PERSON],"
                    + " mappings=FieldMappings{mappings=[Entry{source=personId, target=_.id}]}},"
                    + " dstVertex=Vertex{table=[UNIVERSITY],"
                    + " mappings=FieldMappings{mappings=[Entry{source=studyAt_0, target=_.id}]}}}],"
                    + " alias=[_])\n"
                    + "          GraphLogicalProject(personId=[personId], workAts=[workAts],"
                    + " studyAt_0=[gs.function.first(studyAt)],"
                    + " studyAt_1=[gs.function.second(studyAt)], isAppend=[false])\n"
                    + "            GraphLogicalUnfold(key=[studyAts], alias=[studyAt])\n"
                    + "              GraphLogicalAggregate(keys=[{variables=[personId, studyAts,"
                    + " workAts], aliases=[personId, studyAts, workAts]}], values=[[]])\n"
                    + "                Insert(operation=[INSERT], target=[Edge{table=[HASINTEREST],"
                    + " mappings=FieldMappings{mappings=[]}, srcVertex=Vertex{table=[PERSON],"
                    + " mappings=FieldMappings{mappings=[Entry{source=personId, target=_.id}]}},"
                    + " dstVertex=Vertex{table=[TAG],"
                    + " mappings=FieldMappings{mappings=[Entry{source=tagId, target=_.id}]}}}],"
                    + " alias=[_])\n"
                    + "                  GraphLogicalUnfold(key=[tagIds], alias=[tagId])\n"
                    + "                    Insert(operation=[INSERT],"
                    + " target=[Edge{table=[ISLOCATEDIN], mappings=FieldMappings{mappings=[]},"
                    + " srcVertex=Vertex{table=[PERSON],"
                    + " mappings=FieldMappings{mappings=[Entry{source=personId, target=_.id}]}},"
                    + " dstVertex=Vertex{table=[CITY],"
                    + " mappings=FieldMappings{mappings=[Entry{source=cityId, target=_.id}]}}}],"
                    + " alias=[_])\n"
                    + "                      Insert(operation=[INSERT],"
                    + " target=[Vertex{table=[PERSON],"
                    + " mappings=FieldMappings{mappings=[Entry{source=personId, target=_.id},"
                    + " Entry{source=firstName, target=_.firstName}, Entry{source=lastName,"
                    + " target=_.lastName}, Entry{source=gender, target=_.gender},"
                    + " Entry{source=birthday, target=_.birthday}, Entry{source=creationDate,"
                    + " target=_.creationDate}, Entry{source=locationIP, target=_.locationIP},"
                    + " Entry{source=browserUsed, target=_.browserUsed}]}}], alias=[p])\n"
                    + "                        GraphLogicalProject(personId=[?0], firstName=[?1],"
                    + " lastName=[?2], gender=[?3], birthday=[?4], creationDate=[?5],"
                    + " locationIP=[?6], browserUsed=[?7], cityId=[?8], tagIds=[?9],"
                    + " studyAts=[?10], workAts=[?11], isAppend=[false])\n"
                    + "                          DummyTableScan(tableConfig=[{isAll=false,"
                    + " tables=[dummy]}], alias=[_])",
                after.explain().trim());
    }

    @Test
    public void iu_2_test() {
        String query =
                "WITH $personId as personId, $postId as postId, $creationDate as date\n"
                        + "CREATE (person:PERSON {id: personId})-[:LIKES {creationDate:"
                        + " date}]->(post:POST {id: postId})";
        GraphBuilder builder =
                com.alibaba.graphscope.common.ir.Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before = com.alibaba.graphscope.cypher.antlr4.Utils.eval(query, builder).build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "Insert(operation=[INSERT], target=[Edge{table=[LIKES],"
                    + " mappings=FieldMappings{mappings=[Entry{source=date,"
                    + " target=_.creationDate}]}, srcVertex=Vertex{table=[PERSON],"
                    + " mappings=FieldMappings{mappings=[Entry{source=personId, target=_.id}]}},"
                    + " dstVertex=Vertex{table=[POST],"
                    + " mappings=FieldMappings{mappings=[Entry{source=postId, target=_.id}]}}}],"
                    + " alias=[_])\n"
                    + "  GraphLogicalProject(personId=[?0], postId=[?1], date=[?2],"
                    + " isAppend=[false])\n"
                    + "    DummyTableScan(tableConfig=[{isAll=false, tables=[dummy]}], alias=[_])",
                after.explain().trim());
    }

    @Test
    public void iu_3_test() {
        String query =
                "WITH $personId as personId, $commentId as commentId, $creationDate as date\n"
                        + "CREATE (person:PERSON {id: personId})-[:LIKES {creationDate:"
                        + " date}]->(comment:COMMENT {id: commentId})";
        GraphBuilder builder =
                com.alibaba.graphscope.common.ir.Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before = com.alibaba.graphscope.cypher.antlr4.Utils.eval(query, builder).build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "Insert(operation=[INSERT], target=[Edge{table=[LIKES],"
                    + " mappings=FieldMappings{mappings=[Entry{source=date,"
                    + " target=_.creationDate}]}, srcVertex=Vertex{table=[PERSON],"
                    + " mappings=FieldMappings{mappings=[Entry{source=personId, target=_.id}]}},"
                    + " dstVertex=Vertex{table=[COMMENT],"
                    + " mappings=FieldMappings{mappings=[Entry{source=commentId, target=_.id}]}}}],"
                    + " alias=[_])\n"
                    + "  GraphLogicalProject(personId=[?0], commentId=[?1], date=[?2],"
                    + " isAppend=[false])\n"
                    + "    DummyTableScan(tableConfig=[{isAll=false, tables=[dummy]}], alias=[_])",
                after.explain().trim());
    }

    @Test
    public void iu_4_test() {
        String query =
                "WITH \n"
                    + "  $moderatorPersonId as personId,\n"
                    + "  $forumId as forumId,\n"
                    + "  $forumTitle as forumTitle,\n"
                    + "  $creationDate as creationDate,\n"
                    + "  $tagIds as tagIds\n"
                    + "CREATE (f:FORUM {id: forumId, title: forumTitle, creationDate:"
                    + " creationDate})\n"
                    + "CREATE (f:FORUM {id: forumId})-[:HASMODERATOR]->(p:PERSON {id: personId})\n"
                    + "UNWIND tagIds AS tagId\n"
                    + "CREATE (f:FORUM {id: forumId})-[:HASTAG]->(t:TAG {id: tagId})";
        GraphBuilder builder =
                com.alibaba.graphscope.common.ir.Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before = com.alibaba.graphscope.cypher.antlr4.Utils.eval(query, builder).build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "Insert(operation=[INSERT], target=[Edge{table=[HASTAG],"
                    + " mappings=FieldMappings{mappings=[]}, srcVertex=Vertex{table=[FORUM],"
                    + " mappings=FieldMappings{mappings=[Entry{source=forumId, target=_.id}]}},"
                    + " dstVertex=Vertex{table=[TAG],"
                    + " mappings=FieldMappings{mappings=[Entry{source=tagId, target=_.id}]}}}],"
                    + " alias=[_])\n"
                    + "  GraphLogicalUnfold(key=[tagIds], alias=[tagId])\n"
                    + "    Insert(operation=[INSERT], target=[Edge{table=[HASMODERATOR],"
                    + " mappings=FieldMappings{mappings=[]}, srcVertex=Vertex{table=[FORUM],"
                    + " mappings=FieldMappings{mappings=[Entry{source=forumId, target=_.id}]}},"
                    + " dstVertex=Vertex{table=[PERSON],"
                    + " mappings=FieldMappings{mappings=[Entry{source=personId, target=_.id}]}}}],"
                    + " alias=[_])\n"
                    + "      Insert(operation=[INSERT], target=[Vertex{table=[FORUM],"
                    + " mappings=FieldMappings{mappings=[Entry{source=forumId, target=_.id},"
                    + " Entry{source=forumTitle, target=_.title}, Entry{source=creationDate,"
                    + " target=_.creationDate}]}}], alias=[f])\n"
                    + "        GraphLogicalProject(personId=[?0], forumId=[?1], forumTitle=[?2],"
                    + " creationDate=[?3], tagIds=[?4], isAppend=[false])\n"
                    + "          DummyTableScan(tableConfig=[{isAll=false, tables=[dummy]}],"
                    + " alias=[_])",
                after.explain().trim());
    }

    @Test
    public void iu_5_test() {
        String query =
                "WITH $forumId as forumId, $personId as personId, $joinDate as joinDate\n"
                    + "CREATE (f:FORUM {id: forumId})-[:HASMEMBER {joinDate: joinDate}]->(p:PERSON"
                    + " {id: personId})";
        GraphBuilder builder =
                com.alibaba.graphscope.common.ir.Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before = com.alibaba.graphscope.cypher.antlr4.Utils.eval(query, builder).build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "Insert(operation=[INSERT], target=[Edge{table=[HASMEMBER],"
                    + " mappings=FieldMappings{mappings=[Entry{source=joinDate,"
                    + " target=_.joinDate}]}, srcVertex=Vertex{table=[FORUM],"
                    + " mappings=FieldMappings{mappings=[Entry{source=forumId, target=_.id}]}},"
                    + " dstVertex=Vertex{table=[PERSON],"
                    + " mappings=FieldMappings{mappings=[Entry{source=personId, target=_.id}]}}}],"
                    + " alias=[_])\n"
                    + "  GraphLogicalProject(forumId=[?0], personId=[?1], joinDate=[?2],"
                    + " isAppend=[false])\n"
                    + "    DummyTableScan(tableConfig=[{isAll=false, tables=[dummy]}], alias=[_])",
                after.explain().trim());
    }

    @Test
    public void iu_6_test() {
        String query =
                "WITH\n"
                    + "  $authorPersonId as personId,\n"
                    + "  $countryId as countryId,\n"
                    + "  $forumId as forumId,\n"
                    + "  $postId as postId,\n"
                    + "  $creationDate as creationDate,\n"
                    + "  $locationIP as locationIP,\n"
                    + "  $browserUsed as browserUsed,\n"
                    + "  $language as language,\n"
                    + "  CASE $content WHEN '' THEN NULL ELSE $content END as content,\n"
                    + "  CASE $imageFile WHEN '' THEN NULL ELSE $imageFile END as image,\n"
                    + "  $length as length,\n"
                    + "  $tagIds as tagIds\n"
                    + "CREATE (p:POST {\n"
                    + "    id: postId,\n"
                    + "    creationDate: creationDate,\n"
                    + "    locationIP: locationIP,\n"
                    + "    browserUsed: browserUsed,\n"
                    + "    language: language,\n"
                    + "    content: content,\n"
                    + "    imageFile: image,\n"
                    + "    length: length\n"
                    + "  })\n"
                    + "CREATE (author:PERSON {id: personId})<-[:HASCREATOR]-(p:POST {id: postId})\n"
                    + "CREATE (p: POST {id: postId})<-[:CONTAINEROF]-(forum:FORUM {id: forumId})\n"
                    + "CREATE (p: POST {id: postId})-[:ISLOCATEDIN]->(country:COUNTRY {id:"
                    + " countryId})\n"
                    + "UNWIND tagIds AS tagId\n"
                    + "CREATE (p:POST {id: postId})-[:HASTAG]->(t:TAG {id: tagId})";
        GraphBuilder builder =
                com.alibaba.graphscope.common.ir.Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before = com.alibaba.graphscope.cypher.antlr4.Utils.eval(query, builder).build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "Insert(operation=[INSERT], target=[Edge{table=[HASTAG],"
                    + " mappings=FieldMappings{mappings=[]}, srcVertex=Vertex{table=[POST],"
                    + " mappings=FieldMappings{mappings=[Entry{source=postId, target=_.id}]}},"
                    + " dstVertex=Vertex{table=[TAG],"
                    + " mappings=FieldMappings{mappings=[Entry{source=tagId, target=_.id}]}}}],"
                    + " alias=[_])\n"
                    + "  GraphLogicalUnfold(key=[tagIds], alias=[tagId])\n"
                    + "    Insert(operation=[INSERT], target=[Edge{table=[ISLOCATEDIN],"
                    + " mappings=FieldMappings{mappings=[]}, srcVertex=Vertex{table=[POST],"
                    + " mappings=FieldMappings{mappings=[Entry{source=postId, target=_.id}]}},"
                    + " dstVertex=Vertex{table=[COUNTRY],"
                    + " mappings=FieldMappings{mappings=[Entry{source=countryId, target=_.id}]}}}],"
                    + " alias=[_])\n"
                    + "      Insert(operation=[INSERT], target=[Edge{table=[CONTAINEROF],"
                    + " mappings=FieldMappings{mappings=[]}, srcVertex=Vertex{table=[FORUM],"
                    + " mappings=FieldMappings{mappings=[Entry{source=forumId, target=_.id}]}},"
                    + " dstVertex=Vertex{table=[POST],"
                    + " mappings=FieldMappings{mappings=[Entry{source=postId, target=_.id}]}}}],"
                    + " alias=[_])\n"
                    + "        Insert(operation=[INSERT], target=[Edge{table=[HASCREATOR],"
                    + " mappings=FieldMappings{mappings=[]}, srcVertex=Vertex{table=[POST],"
                    + " mappings=FieldMappings{mappings=[Entry{source=postId, target=_.id}]}},"
                    + " dstVertex=Vertex{table=[PERSON],"
                    + " mappings=FieldMappings{mappings=[Entry{source=personId, target=_.id}]}}}],"
                    + " alias=[_])\n"
                    + "          Insert(operation=[INSERT], target=[Vertex{table=[POST],"
                    + " mappings=FieldMappings{mappings=[Entry{source=postId, target=_.id},"
                    + " Entry{source=creationDate, target=_.creationDate}, Entry{source=locationIP,"
                    + " target=_.locationIP}, Entry{source=browserUsed, target=_.browserUsed},"
                    + " Entry{source=language, target=_.language}, Entry{source=content,"
                    + " target=_.content}, Entry{source=image, target=_.imageFile},"
                    + " Entry{source=length, target=_.length}]}}], alias=[p])\n"
                    + "            GraphLogicalProject(personId=[?0], countryId=[?1], forumId=[?2],"
                    + " postId=[?3], creationDate=[?4], locationIP=[?5], browserUsed=[?6],"
                    + " language=[?7], content=[CASE(=(?8, _UTF-8''), null:NULL, ?8)],"
                    + " image=[CASE(=(?9, _UTF-8''), null:NULL, ?9)], length=[?10], tagIds=[?11],"
                    + " isAppend=[false])\n"
                    + "              DummyTableScan(tableConfig=[{isAll=false, tables=[dummy]}],"
                    + " alias=[_])",
                after.explain().trim());
    }

    @Test
    public void iu_7_test() {
        String query =
                "WITH\n"
                    + "  $authorPersonId as personId,\n"
                    + "  $countryId as countryId,\n"
                    + "  $replyToPostId  + 1 + $replyToCommentId as messageId,\n"
                    + "  $commentId as commentId,\n"
                    + "  $creationDate as creationDate,\n"
                    + "  $locationIP as locationIP,\n"
                    + "  $browserUsed as browserUsed,\n"
                    + "  $content as content,\n"
                    + "  $length as length,\n"
                    + "  $tagIds as tagIds\n"
                    + "CREATE (c:COMMENT {\n"
                    + "    id: commentId,\n"
                    + "    creationDate: creationDate,\n"
                    + "    locationIP: locationIP,\n"
                    + "    browserUsed: browserUsed,\n"
                    + "    content: content,\n"
                    + "    length: length\n"
                    + "  })\n"
                    + "CREATE (author:PERSON {id: personId})<-[:HASCREATOR]-(c:COMMENT {id:"
                    + " commentId})\n"
                    + "CREATE (c:COMMENT {id: commentId})-[:REPLYOF]->(message:POST|COMMENT {id:"
                    + " messageId})\n"
                    + "CREATE (c:COMMENT {id: commentId})-[:ISLOCATEDIN]->(country:COUNTRY {id:"
                    + " countryId})\n"
                    + "UNWIND tagIds AS tagId\n"
                    + "CREATE (c:COMMENT {id: commentId})-[:HASTAG]->(t:TAG {id: tagId})";
        GraphBuilder builder =
                com.alibaba.graphscope.common.ir.Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before = com.alibaba.graphscope.cypher.antlr4.Utils.eval(query, builder).build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        Assert.assertEquals(
                "Insert(operation=[INSERT], target=[Edge{table=[HASTAG],"
                    + " mappings=FieldMappings{mappings=[]}, srcVertex=Vertex{table=[COMMENT],"
                    + " mappings=FieldMappings{mappings=[Entry{source=commentId, target=_.id}]}},"
                    + " dstVertex=Vertex{table=[TAG],"
                    + " mappings=FieldMappings{mappings=[Entry{source=tagId, target=_.id}]}}}],"
                    + " alias=[_])\n"
                    + "  GraphLogicalUnfold(key=[tagIds], alias=[tagId])\n"
                    + "    Insert(operation=[INSERT], target=[Edge{table=[ISLOCATEDIN],"
                    + " mappings=FieldMappings{mappings=[]}, srcVertex=Vertex{table=[COMMENT],"
                    + " mappings=FieldMappings{mappings=[Entry{source=commentId, target=_.id}]}},"
                    + " dstVertex=Vertex{table=[COUNTRY],"
                    + " mappings=FieldMappings{mappings=[Entry{source=countryId, target=_.id}]}}}],"
                    + " alias=[_])\n"
                    + "      Insert(operation=[INSERT], target=[Edge{table=[REPLYOF],"
                    + " mappings=FieldMappings{mappings=[]}, srcVertex=Vertex{table=[COMMENT],"
                    + " mappings=FieldMappings{mappings=[Entry{source=commentId, target=_.id}]}},"
                    + " dstVertex=Vertex{table=[COMMENT],"
                    + " mappings=FieldMappings{mappings=[Entry{source=messageId, target=_.id}]}}}],"
                    + " alias=[_])\n"
                    + "        Insert(operation=[INSERT], target=[Edge{table=[REPLYOF],"
                    + " mappings=FieldMappings{mappings=[]}, srcVertex=Vertex{table=[COMMENT],"
                    + " mappings=FieldMappings{mappings=[Entry{source=commentId, target=_.id}]}},"
                    + " dstVertex=Vertex{table=[POST],"
                    + " mappings=FieldMappings{mappings=[Entry{source=messageId, target=_.id}]}}}],"
                    + " alias=[_])\n"
                    + "          Insert(operation=[INSERT], target=[Edge{table=[HASCREATOR],"
                    + " mappings=FieldMappings{mappings=[]}, srcVertex=Vertex{table=[COMMENT],"
                    + " mappings=FieldMappings{mappings=[Entry{source=commentId, target=_.id}]}},"
                    + " dstVertex=Vertex{table=[PERSON],"
                    + " mappings=FieldMappings{mappings=[Entry{source=personId, target=_.id}]}}}],"
                    + " alias=[_])\n"
                    + "            Insert(operation=[INSERT], target=[Vertex{table=[COMMENT],"
                    + " mappings=FieldMappings{mappings=[Entry{source=commentId, target=_.id},"
                    + " Entry{source=creationDate, target=_.creationDate}, Entry{source=locationIP,"
                    + " target=_.locationIP}, Entry{source=browserUsed, target=_.browserUsed},"
                    + " Entry{source=content, target=_.content}, Entry{source=length,"
                    + " target=_.length}]}}], alias=[c])\n"
                    + "              GraphLogicalProject(personId=[?0], countryId=[?1],"
                    + " messageId=[+(+(?2, 1), ?3)], commentId=[?4], creationDate=[?5],"
                    + " locationIP=[?6], browserUsed=[?7], content=[?8], length=[?9], tagIds=[?10],"
                    + " isAppend=[false])\n"
                    + "                DummyTableScan(tableConfig=[{isAll=false, tables=[dummy]}],"
                    + " alias=[_])",
                after.explain().trim());
    }
}

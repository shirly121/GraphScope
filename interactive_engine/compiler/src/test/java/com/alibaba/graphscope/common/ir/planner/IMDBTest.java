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

package com.alibaba.graphscope.common.ir.planner;

import com.alibaba.graphscope.common.config.Configs;
import com.alibaba.graphscope.common.ir.Utils;
import com.alibaba.graphscope.common.ir.meta.IrMeta;
import com.alibaba.graphscope.common.ir.tools.GraphBuilder;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.rel.RelNode;
import org.junit.BeforeClass;
import org.junit.Test;

public class IMDBTest {
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
                Utils.mockIrMeta(
                        "schema/imdb_schema.yaml",
                        "statistics/imdb_statistics.json",
                        optimizer.getGlogueHolder());
    }

    @Test
    public void imdb_test() {
        GraphBuilder builder = Utils.mockGraphBuilder(optimizer, irMeta);
        RelNode before =
                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
                                "MATCH\n" +
                                        "(t:TITLE)<-[:COMPLETE_CAST_TITLE]-(cc:COMPLETE_CAST)-[:COMPLETE_CAST_SUBJECT]->(cct1:COMP_CAST_TYPE),\n" +
                                        "(cc)-[:COMPLETE_CAST_STATUS]->(cct2:COMP_CAST_TYPE),\n" +
                                        "(t)-[mk:MOVIE_KEYWORD]->(k:KEYWORD),\n" +
                                        "(t)<-[:MOVIE_COMPANIES_TITLE]-(mc:MOVIE_COMPANIES)-[:MOVIE_COMPANIES_COMPANY_NAME]->(cn:COMPANY_NAME),\n" +
                                        "(t)-[mi:MOVIE_INFO]->(it:INFO_TYPE),\n" +
                                        "(t)<-[:CAST_INFO_TITLE]-(ci:CAST_INFO)-[:CAST_INFO_CHAR]->(chn:CHAR_NAME),\n" +
                                        "(ci)-[:CAST_INFO_NAME]->(n:NAME),\n" +
                                        "(ci)-[:CAST_INFO_ROLE]->(rt:ROLE_TYPE),\n" +
                                        "(n)<-[:ALSO_KNOWN_AS_NAME]-(an:AKA_NAME),\n" +
                                        "(n)-[pi:PERSON_INFO]->(it3:INFO_TYPE)\n" +
                                        "WHERE cct1.kind = 'cast'\n" +
                                        "  AND cct2.kind = 'complete+verified'\n" +
                                        "  AND chn.name = 'Queen'\n" +
                                        "  AND ci.note IN ['(voice)', '(voice) (uncredited)', '(voice: English version)']\n" +
                                        "  AND cn.country_code = '[us]'\n" +
                                        "  AND it.info = 'release dates'\n" +
                                        "  AND it3.info = 'trivia'\n" +
                                        "  AND k.keyword = 'computer-animation'\n" +
                                        "  AND mi.info IS NOT NULL\n" +
                                        "  AND (mi.info CONTAINS 'Japan:.*200' OR mi.info CONTAINS 'USA:.*200')\n" +
                                        "  AND n.gender = 'f'\n" +
                                        "  AND n.name CONTAINS 'An'\n" +
                                        "  AND rt.role = 'actress'\n" +
                                        "  AND t.title = 'Shrek 2'\n" +
                                        "  AND t.production_year >= 2000\n" +
                                        "  AND t.production_year <= 2010\n" +
                                        "RETURN\n" +
                                        "  MIN(chn.name) AS voiced_char,\n" +
                                        "  MIN(n.name) AS voicing_actress,\n" +
                                        "  MIN(t.title) AS voiced_animation;",
                                builder)
                        .build();
//        RelNode before1 =
//                com.alibaba.graphscope.cypher.antlr4.Utils.eval(
//                                "MATCH\n" +
////                                        "(t:TITLE)<-[:COMPLETE_CAST_TITLE]-(cc:COMPLETE_CAST)-[:COMPLETE_CAST_SUBJECT]->(cct1:COMP_CAST_TYPE),\n" +
////                                        "(cc)-[:COMPLETE_CAST_STATUS]->(cct2:COMP_CAST_TYPE),\n" +
////                                        "(t)-[mk:MOVIE_KEYWORD]->(k:KEYWORD),\n" +
////                                        "(t)<-[:MOVIE_COMPANIES_TITLE]-(mc:MOVIE_COMPANIES)-[:MOVIE_COMPANIES_COMPANY_NAME]->(cn:COMPANY_NAME),\n" +
////                                        "(t)-[mi:MOVIE_INFO]->(it:INFO_TYPE),\n" +
//                                        "(t)<-[:CAST_INFO_TITLE]-(ci:CAST_INFO)-[:CAST_INFO_CHAR]->(chn:CHAR_NAME),\n" +
//                                        "(ci)-[:CAST_INFO_NAME]->(n:NAME),\n" +
//                                        "(ci)-[:CAST_INFO_ROLE]->(rt:ROLE_TYPE),\n" +
//                                        "(n)<-[:ALSO_KNOWN_AS_NAME]-(an:AKA_NAME),\n" +
//                                        "(n)-[pi:PERSON_INFO]->(it3:INFO_TYPE)\n" +
////                                        "WHERE cct1.kind = 'cast'\n" +
////                                        "  AND cct2.kind = 'complete+verified'\n" +
//                                        "  Where chn.name = 'Queen'\n" +
//                                        "  AND ci.note IN ['(voice)', '(voice) (uncredited)', '(voice: English version)']\n" +
////                                        "  AND cn.country_code = '[us]'\n" +
////                                        "  AND it.info = 'release dates'\n" +
//                                        "  AND it3.info = 'trivia'\n" +
////                                        "  AND k.keyword = 'computer-animation'\n" +
////                                        "  AND mi.info IS NOT NULL\n" +
////                                        "  AND (mi.info CONTAINS 'Japan:.*200' OR mi.info CONTAINS 'USA:.*200')\n" +
//                                        "  AND n.gender = 'f'\n" +
//                                        "  AND n.name CONTAINS 'An'\n" +
//                                        "  AND rt.role = 'actress'\n" +
//                                        "  AND t.title = 'Shrek 2'\n" +
//                                        "  AND t.production_year >= 2000\n" +
//                                        "  AND t.production_year <= 2010\n" +
//                                        "RETURN\n" +
//                                        "  MIN(chn.name) AS voiced_char,\n" +
//                                        "  MIN(n.name) AS voicing_actress,\n" +
//                                        "  MIN(t.title) AS voiced_animation;",
//                                builder)
//                        .build();
        RelNode after = optimizer.optimize(before, new GraphIOProcessor(builder, irMeta));
        System.out.println(optimizer.getCoreElapsedTime());
        System.out.println(after.explain());
    }
}

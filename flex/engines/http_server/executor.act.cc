/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "flex/engines/http_server/executor.act.h"

#include "flex/engines/graph_db/database/graph_db.h"
#include "flex/engines/graph_db/database/graph_db_session.h"
#include "flex/engines/http_server/codegen_proxy.h"
#include "flex/engines/http_server/stored_procedure.h"

#include <seastar/core/print.hh>

namespace server {

executor::~executor() {
  // finalization
  // ...
}

executor::executor(hiactor::actor_base* exec_ctx, const hiactor::byte_t* addr)
    : hiactor::actor(exec_ctx, addr) {
  set_max_concurrency(1);  // set max concurrency for task reentrancy (stateful)
  // initialization
  // ...
}

seastar::future<query_result> executor::run_graph_db_query(
    query_param&& param) {
  auto ret = gs::GraphDB::get()
                 .GetSession(hiactor::local_shard_id())
                 .Eval(param.content);
  seastar::sstring content(ret.data(), ret.size());
  return seastar::make_ready_future<query_result>(std::move(content));
}

// run_query_for stored_procedure
seastar::future<query_result> executor::run_hqps_procedure_query(
    query_param&& param) {
  auto& str = param.content;
  const char* str_data = str.data();
  size_t str_length = str.size();
  LOG(INFO) << "Receive pay load: " << str_length << " bytes";

  query::Query cur_query;
  {
    CHECK(cur_query.ParseFromArray(str.data(), str.size()));
    LOG(INFO) << "Parse query: " << cur_query.DebugString();
  }
  auto& store_procedure_manager = server::StoredProcedureManager::get();
  return store_procedure_manager.Query(cur_query).then(
      [&cur_query](results::CollectiveResults&& hqps_result) {
        LOG(INFO) << "Finish running query: " << cur_query.DebugString();
        LOG(INFO) << "Query results" << hqps_result.DebugString();

        auto tem_str = hqps_result.SerializeAsString();

        seastar::sstring content(tem_str.data(), tem_str.size());
        return seastar::make_ready_future<query_result>(std::move(content));
      });
}

seastar::future<query_result> executor::run_hqps_adhoc_query(
    adhoc_result&& param) {
  LOG(INFO) << "Run adhoc query";
  // The received query's pay load shoud be able to deserialze to physical plan
  // 1. load and run.
  auto& content = param.content;
  LOG(INFO) << "Okay, try to run the query of lib path: " << content.second
            << ", job id: " << content.first
            << "local shard id: " << hiactor::local_shard_id();
  seastar::sstring result = server::load_and_run(content.first, content.second);
  return seastar::make_ready_future<query_result>(std::move(result));
}

}  // namespace server

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
#include "flex/engines/http_server/service/hqps_service.h"
#include "flex/engines/http_server/options.h"
namespace server {

ServiceConfig::ServiceConfig()
    : bolt_port(DEFAULT_BOLT_PORT),
      admin_port(DEFAULT_ADMIN_PORT),
      query_port(DEFAULT_QUERY_PORT),
      shard_num(DEFAULT_SHARD_NUM),
      dpdk_mode(false),
      enable_thread_resource_pool(true),
      external_thread_num(2),
      start_admin_service(true) {}

const std::string HQPSService::DEFAULT_GRAPH_NAME = "modern_graph";

HQPSService& HQPSService::get() {
  static HQPSService instance;
  return instance;
}

void HQPSService::init(const ServiceConfig& config) {
  if (initialized_.load(std::memory_order_relaxed)) {
    std::cerr << "High QPS service has been already initialized!" << std::endl;
    return;
  }
  actor_sys_ = std::make_unique<actor_system>(
      config.shard_num, config.dpdk_mode, config.enable_thread_resource_pool,
      config.external_thread_num);
  query_hdl_ = std::make_unique<hqps_http_handler>(config.query_port);
  if (config.start_admin_service) {
    admin_hdl_ = std::make_unique<admin_http_handler>(config.admin_port);
  }
  initialized_.store(true);
  service_config_ = config;
  gs::init_cpu_usage_watch();
}

HQPSService::~HQPSService() {
  if (actor_sys_) {
    actor_sys_->terminate();
  }
}

const ServiceConfig& HQPSService::get_service_config() const {
  return service_config_;
}

bool HQPSService::is_initialized() const {
  return initialized_.load(std::memory_order_relaxed);
}

bool HQPSService::is_running() const {
  return running_.load(std::memory_order_relaxed);
}

uint16_t HQPSService::get_query_port() const {
  if (query_hdl_) {
    return query_hdl_->get_port();
  }
  return 0;
}

gs::Result<seastar::sstring> HQPSService::service_status() {
  if (!is_initialized()) {
    return gs::Result<seastar::sstring>(
        gs::StatusCode::OK, "High QPS service has not been inited!", "");
  }
  if (!is_running()) {
    return gs::Result<seastar::sstring>(
        gs::StatusCode::OK, "High QPS service has not been started!", "");
  }
  return gs::Result<seastar::sstring>(
      seastar::sstring("High QPS service is running ..."));
}

void HQPSService::run_and_wait_for_exit() {
  if (!is_initialized()) {
    std::cerr << "High QPS service has not been inited!" << std::endl;
    return;
  }
  actor_sys_->launch();
  query_hdl_->start();
  if (admin_hdl_) {
    admin_hdl_->start();
  }
  running_.store(true);
  while (running_.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  query_hdl_->stop();
  if (admin_hdl_) {
    admin_hdl_->stop();
  }
  actor_sys_->terminate();
}

void HQPSService::set_exit_state() { running_.store(false); }

seastar::future<> HQPSService::stop_query_actors() {
  if (query_hdl_) {
    return query_hdl_->stop_query_actors();
  } else {
    std::cerr << "Query handler has not been inited!" << std::endl;
    return seastar::make_exception_future<>(
        std::runtime_error("Query handler has not been inited!"));
  }
}

void HQPSService::start_query_actors() {
  if (query_hdl_) {
    query_hdl_->start_query_actors();
  } else {
    std::cerr << "Query handler has not been inited!" << std::endl;
    return;
  }
}
}  // namespace server

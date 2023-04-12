/** Copyright 2020 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#ifndef ANALYTICAL_ENGINE_APPS_FLASH_CONNECTIVITY_SCC_2_H_
#define ANALYTICAL_ENGINE_APPS_FLASH_CONNECTIVITY_SCC_2_H_

#include <algorithm>
#include <memory>
#include <utility>

#include "grape/grape.h"

#include "apps/flash/api.h"
#include "apps/flash/flash_app_base.h"
#include "apps/flash/flash_context.h"
#include "apps/flash/flash_worker.h"
#include "apps/flash/value_type.h"

namespace gs {

template <typename FRAG_T>
class SCC2Flash : public FlashAppBase<FRAG_T, SCC_TYPE> {
 public:
  INSTALL_FLASH_WORKER(SCC2Flash<FRAG_T>, SCC_TYPE, FRAG_T)
  using context_t = FlashVertexDataContext<FRAG_T, SCC_TYPE, int>;

  bool sync_all_ = false;

  int* Res(value_t* v) { return &(v->scc); }

  void Run(const fragment_t& graph, const std::shared_ptr<fw_t> fw) {
    int n_vertex = graph.GetTotalVerticesNum();
    LOG(INFO) << "Run SCC with Flash, total vertices: " << n_vertex
              << std::endl;

    DefineMapV(init) {
      v.scc = -1;
      v.fid = -1;
    };
    vset_t A = VertexMap(All, CTrueV, init);

    std::pair<int, int> v_loc = std::make_pair(0, 0), v_glb;
    TraverseLocal(
        if (Deg(id) > v_loc.first) { v_loc = std::make_pair(Deg(id), id); });
    GetMax(v_loc, v_glb);
    int sccid = v_glb.second;

    DefineFV(filter0) { return id == sccid; };
    DefineMapV(local0) {
      v.fid = id;
      v.scc = id;
    };
    vset_t B = VertexMap(All, filter0, local0);

    for (int nb = VSize(B), j = 1; nb > 0; nb = VSize(B), ++j) {
      DefineFV(cond) { return v.fid == -1; };
      DefineMapE(update) { d.fid = sccid; };
      B = EdgeMap(B, ED, CTrueE, update, cond);
    }

    B = VertexMap(All, filter0);
    for (int nb = VSize(B), j = 1; nb > 0; nb = VSize(B), ++j) {
      DefineFV(cond) { return v.scc == -1 && v.fid != -1; };
      DefineFE(check) { return s.scc == sccid; };
      DefineMapE(update) { d.scc = sccid; };
      B = EdgeMap(B, ER, check, update, cond);
    }

    DefineFV(filter) { return v.scc == -1; };
    A = VertexMap(All, filter);

    DefineMapV(local1) { v.fid = id; };

    DefineFV(filter2) { return v.fid == id; };
    DefineMapV(local2) { v.scc = id; };

    for (int i = 1, vsa = VSize(A); vsa > 0; vsa = VSize(A), ++i) {
      vset_t B = VertexMap(A, CTrueV, local1);
      for (int vsb = VSize(B), j = 1; vsb > 0; vsb = VSize(B), ++j) {
        DefineFE(check1) { return s.fid < d.fid; };
        DefineMapE(update1) { d.fid = std::min(d.fid, s.fid); };
        DefineFV(cond1) { return v.scc == -1; };

        B = EdgeMap(B, EjoinV(ED, A), check1, update1, cond1);
      }

      B = VertexMap(A, filter2, local2);

      for (int vsb = VSize(B), j = 1; vsb > 0; vsb = VSize(B), ++j) {
        DefineFE(check2) { return s.scc == d.fid; };
        DefineMapE(update2) { d.scc = d.fid; };
        DefineFV(cond2) { return v.scc == -1; };

        B = EdgeMap(B, EjoinV(ER, A), check2, update2, cond2);
      }

      A = VertexMap(A, filter);
    }
  }
};

}  // namespace gs

#endif  // ANALYTICAL_ENGINE_APPS_FLASH_CONNECTIVITY_SCC_2_H_

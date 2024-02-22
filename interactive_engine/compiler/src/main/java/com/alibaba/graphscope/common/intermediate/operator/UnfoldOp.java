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

package com.alibaba.graphscope.common.intermediate.operator;

import java.util.Optional;

public class UnfoldOp extends InterOpBase {
    private Optional<OpArg> tag;
    private Optional<OpArg> alias;

    public UnfoldOp() {
        super();
        this.tag = Optional.empty();
        this.alias = Optional.empty();
    }

    public Optional<OpArg> getUnfoldTag() {
        return tag;
    }

    public Optional<OpArg> getUnfoldAlias() {
        return alias;
    }

    public void setUnfoldTag(OpArg tag) {
        this.tag = Optional.of(tag);
    }

    public void setUnfoldAlias(OpArg alias) {
        this.alias = Optional.of(alias);
    }
}

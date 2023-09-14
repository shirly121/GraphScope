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

package com.alibaba.graphscope.common.ir.rex.operator;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.fun.SqlMultisetValueConstructor;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;

import java.util.List;

public class SqlArrayValueConstructor extends SqlMultisetValueConstructor {
    public SqlArrayValueConstructor() {
        super("ARRAY", SqlKind.ARRAY_VALUE_CONSTRUCTOR);
    }

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
        RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
        List<RelDataType> argTypes = opBinding.collectOperandTypes();
        RelDataType componentType;
        if (argTypes.isEmpty()) {
            componentType = typeFactory.createSqlType(SqlTypeName.ANY);
        } else {
            componentType = getComponentType(typeFactory, argTypes);
            if (componentType == null) {
                componentType = typeFactory.createSqlType(SqlTypeName.ANY);
            }
        }
        return SqlTypeUtil.createArrayType(typeFactory, componentType, false);
    }

    // operands of array value constructor can be any, even if empty, i.e []
    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        return true;
    }
}

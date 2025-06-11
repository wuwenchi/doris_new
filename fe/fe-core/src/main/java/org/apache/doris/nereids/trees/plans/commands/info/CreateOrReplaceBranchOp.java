// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.alter.AlterOpType;
import org.apache.doris.analysis.AlterTableClause;

import java.util.Map;

public class CreateOrReplaceBranchOp extends AlterTableOp {

    private final String branchName;
    private final BranchOptions branchOptions;
    private final boolean create;
    private final boolean replace;
    private final boolean ifNotExists;

    public CreateOrReplaceBranchOp(String branchName,
                                   BranchOptions branchOptions,
                                   boolean create,
                                   boolean replace,
                                   boolean ifNotExists) {
        super(AlterOpType.ALTER_BRANCH);
        this.branchName = branchName;
        this.branchOptions = branchOptions;
        this.create = create;
        this.replace = replace;
        this.ifNotExists = ifNotExists;
    }

    @Override
    public boolean allowOpMTMV() {
        return false;
    }

    @Override
    public boolean needChangeMTMVState() {
        return false;
    }

    @Override
    public String toSql() {
        return "";
    }

    @Override
    public Map<String, String> getProperties() {
        return Map.of();
    }

    @Override
    public AlterTableClause translateToLegacyAlterClause() {
        return null;
    }
}

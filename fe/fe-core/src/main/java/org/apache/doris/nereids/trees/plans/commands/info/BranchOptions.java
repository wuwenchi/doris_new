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

import java.util.Optional;

public class BranchOptions {
    static final BranchOptions EMPTY = new BranchOptions(Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    private final Optional<Long> snapshotId;
    private final Optional<Integer> numSnapshots;
    private final Optional<Long> retain;
    private final Optional<Long> retention;

    public BranchOptions(Optional<Long> snapshotId, Optional<Integer> numSnapshots,
                         Optional<Long> retain, Optional<Long> retention) {
        this.snapshotId = snapshotId;
        this.numSnapshots = numSnapshots;
        this.retain = retain;
        this.retention = retention;
    }

    public Optional<Long> getSnapshotId() {
        return snapshotId;
    }

    public Optional<Integer> getNumSnapshots() {
        return numSnapshots;
    }

    public Optional<Long> getRetain() {
        return retain;
    }

    public Optional<Long> getRetention() {
        return retention;
    }

}

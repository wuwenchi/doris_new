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

package org.apache.doris.datasource.iceberg.source;

import lombok.Data;

import java.util.List;
import java.util.OptionalLong;

@Data
public class IcebergDeleteFileFilter {
    private String deleteFilePath;
    private long filesize;

    public IcebergDeleteFileFilter(String deleteFilePath, long filesize) {
        this.deleteFilePath = deleteFilePath;
        this.filesize = filesize;
    }

    public static PositionDelete createPositionDelete(String deleteFilePath, Long positionLowerBound,
                                                      Long positionUpperBound, long filesize) {
        return new PositionDelete(deleteFilePath, positionLowerBound, positionUpperBound, filesize);
    }

    public static EqualityDelete createEqualityDelete(String deleteFilePath, List<Integer> fieldIds, long fileSize) {
        // todo:
        // Schema deleteSchema = TypeUtil.select(scan.schema(), new HashSet<>(fieldIds));
        // StructLikeSet deleteSet = StructLikeSet.create(deleteSchema.asStruct());
        // pass deleteSet to BE
        // compare two StructLike value, if equals, filtered
        return new EqualityDelete(deleteFilePath, fieldIds, fileSize);
    }

    static class PositionDelete extends IcebergDeleteFileFilter {
        private final Long positionLowerBound;
        private final Long positionUpperBound;

        public PositionDelete(String deleteFilePath, Long positionLowerBound,
                              Long positionUpperBound, long fileSize) {
            super(deleteFilePath, fileSize);
            this.positionLowerBound = positionLowerBound;
            this.positionUpperBound = positionUpperBound;
        }

        public OptionalLong getPositionLowerBound() {
            return positionLowerBound == -1L ? OptionalLong.empty() : OptionalLong.of(positionLowerBound);
        }

        public OptionalLong getPositionUpperBound() {
            return positionUpperBound == -1L ? OptionalLong.empty() : OptionalLong.of(positionUpperBound);
        }
    }

    static class EqualityDelete extends IcebergDeleteFileFilter {
        private List<Integer> fieldIds;

        public EqualityDelete(String deleteFilePath, List<Integer> fieldIds, long fileSize) {
            super(deleteFilePath, fileSize);
            this.fieldIds = fieldIds;
        }

        public List<Integer> getFieldIds() {
            return fieldIds;
        }

        public void setFieldIds(List<Integer> fieldIds) {
            this.fieldIds = fieldIds;
        }
    }
}

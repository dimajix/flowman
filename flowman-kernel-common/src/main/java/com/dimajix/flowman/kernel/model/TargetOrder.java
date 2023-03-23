/*
 * Copyright (C) 2023 The Flowman Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dimajix.flowman.kernel.model;


public enum TargetOrder {
    BY_DATETIME,
    BY_PROJECT,
    BY_NAME,
    BY_ID,
    BY_STATUS,
    BY_PHASE,
    BY_PARENT_NAME,
    BY_PARENT_ID;


    public com.dimajix.flowman.kernel.proto.history.TargetOrder toProto() {
        switch(this) {
            case BY_DATETIME:
                return com.dimajix.flowman.kernel.proto.history.TargetOrder.TARGET_BY_DATETIME;
            case BY_PROJECT:
                return com.dimajix.flowman.kernel.proto.history.TargetOrder.TARGET_BY_PROJECT;
            case BY_NAME:
                return com.dimajix.flowman.kernel.proto.history.TargetOrder.TARGET_BY_NAME;
            case BY_ID:
                return com.dimajix.flowman.kernel.proto.history.TargetOrder.TARGET_BY_ID;
            case BY_STATUS:
                return com.dimajix.flowman.kernel.proto.history.TargetOrder.TARGET_BY_STATUS;
            case BY_PHASE:
                return com.dimajix.flowman.kernel.proto.history.TargetOrder.TARGET_BY_PHASE;
            case BY_PARENT_NAME:
                return com.dimajix.flowman.kernel.proto.history.TargetOrder.TARGET_BY_PARENT_NAME;
            case BY_PARENT_ID:
                return com.dimajix.flowman.kernel.proto.history.TargetOrder.TARGET_BY_PARENT_ID;
        }

        return com.dimajix.flowman.kernel.proto.history.TargetOrder.TARGET_BY_ID;
    }
    public static TargetOrder ofProto(com.dimajix.flowman.kernel.proto.history.TargetOrder order) {
        switch(order) {
            case TARGET_BY_DATETIME:
                return BY_DATETIME;
            case TARGET_BY_PROJECT:
                return BY_PROJECT;
            case TARGET_BY_NAME:
                return BY_NAME;
            case TARGET_BY_ID:
                return BY_ID;
            case TARGET_BY_STATUS:
                return BY_STATUS;
            case TARGET_BY_PHASE:
                return BY_PHASE;
            case TARGET_BY_PARENT_NAME:
                return BY_PARENT_NAME;
            case TARGET_BY_PARENT_ID:
                return BY_PARENT_ID;
        }

        return BY_ID;
    }
}

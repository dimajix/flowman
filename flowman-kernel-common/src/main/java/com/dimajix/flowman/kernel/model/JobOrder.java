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


public enum JobOrder {
    BY_DATETIME,
    BY_PROJECT,
    BY_NAME,
    BY_ID,
    BY_STATUS,
    BY_PHASE;


    public com.dimajix.flowman.kernel.proto.history.JobOrder toProto() {
        switch(this) {
            case BY_DATETIME:
                return com.dimajix.flowman.kernel.proto.history.JobOrder.JOB_BY_DATETIME;
            case BY_PROJECT:
                return com.dimajix.flowman.kernel.proto.history.JobOrder.JOB_BY_PROJECT;
            case BY_NAME:
                return com.dimajix.flowman.kernel.proto.history.JobOrder.JOB_BY_NAME;
            case BY_ID:
                return com.dimajix.flowman.kernel.proto.history.JobOrder.JOB_BY_ID;
            case BY_STATUS:
                return com.dimajix.flowman.kernel.proto.history.JobOrder.JOB_BY_STATUS;
            case BY_PHASE:
                return com.dimajix.flowman.kernel.proto.history.JobOrder.JOB_BY_PHASE;
        }

        return com.dimajix.flowman.kernel.proto.history.JobOrder.JOB_BY_ID;
    }
    public static JobOrder ofProto(com.dimajix.flowman.kernel.proto.history.JobOrder order) {
        switch(order) {
            case JOB_BY_DATETIME:
                return BY_DATETIME;
            case JOB_BY_PROJECT:
                return BY_PROJECT;
            case JOB_BY_NAME:
                return BY_NAME;
            case JOB_BY_ID:
                return BY_ID;
            case JOB_BY_STATUS:
                return BY_STATUS;
            case JOB_BY_PHASE:
                return BY_PHASE;
        }

        return BY_ID;
    }
}

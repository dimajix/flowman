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


import java.util.Locale;

import lombok.val;

import com.dimajix.flowman.kernel.proto.ExecutionStatus;

public enum Status {
    UNKNOWN,
    RUNNING,
    SUCCESS,
    SUCCESS_WITH_ERRORS,
    FAILED,
    ABORTED,
    SKIPPED;


    public com.dimajix.flowman.kernel.proto.ExecutionStatus toProto() {
        switch(this) {
            case UNKNOWN:
                return ExecutionStatus.UNKNOWN_STATUS;
            case RUNNING:
                return ExecutionStatus.RUNNING;
            case SUCCESS:
                return ExecutionStatus.SUCCESS;
            case SUCCESS_WITH_ERRORS:
                return ExecutionStatus.SUCCESS_WITH_ERRORS;
            case FAILED:
                return ExecutionStatus.FAILED;
            case ABORTED:
                return ExecutionStatus.ABORTED;
            case SKIPPED:
                return ExecutionStatus.SKIPPED;
        }

        return ExecutionStatus.UNKNOWN_STATUS;
    }

    public static Status ofProto(com.dimajix.flowman.kernel.proto.ExecutionStatus status) {
        switch(status) {
            case UNKNOWN_STATUS:
                return UNKNOWN;
            case RUNNING:
                return RUNNING;
            case SUCCESS:
                return SUCCESS;
            case SUCCESS_WITH_ERRORS:
                return SUCCESS_WITH_ERRORS;
            case FAILED:
                return FAILED;
            case ABORTED:
                return ABORTED;
            case SKIPPED:
                return SKIPPED;
        }

        throw new IllegalArgumentException("Unknown status " + status);
    }

    public static Status ofString(String status) {
        val upr = status.toUpperCase(Locale.ROOT);
        if (upr.equals("UNKNOWN"))
            return UNKNOWN;
        if (upr.equals("RUNNING"))
            return RUNNING;
        if (upr.equals("SUCCESS"))
            return SUCCESS;
        if (upr.equals("SUCCESS_WITH_ERRORS"))
            return SUCCESS_WITH_ERRORS;
        if (upr.equals("FAILED"))
            return FAILED;
        if (upr.equals("ABORTED"))
            return ABORTED;
        if (upr.equals("SKIPPED"))
            return SKIPPED;
        throw new IllegalArgumentException("Unknown status " + status);
    }
}

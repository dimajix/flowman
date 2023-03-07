/*
 * Copyright 2018-2023 Kaya Kupferschmidt
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


import com.dimajix.flowman.kernel.proto.ExecutionPhase;

public enum Phase {
    VALIDATE,
    CREATE,
    BUILD,
    VERIFY,
    TRUNCATE,
    DESTROY;

    public com.dimajix.flowman.kernel.proto.ExecutionPhase toProto() {
        switch (this) {
            case VALIDATE:
                return ExecutionPhase.VALIDATE;
            case CREATE:
                return ExecutionPhase.CREATE;
            case BUILD:
                return ExecutionPhase.BUILD;
            case VERIFY:
                return ExecutionPhase.VERIFY;
            case TRUNCATE:
                return ExecutionPhase.TRUNCATE;
            case DESTROY:
                return ExecutionPhase.DESTROY;
        }

        throw new IllegalArgumentException("Unknown phase " + name());
    }

    public static Phase ofProto(com.dimajix.flowman.kernel.proto.ExecutionPhase phase) {
        switch(phase) {
            case VALIDATE:
                return VALIDATE;
            case CREATE:
                return CREATE;
            case BUILD:
                return BUILD;
            case VERIFY:
                return VERIFY;
            case TRUNCATE:
                return TRUNCATE;
            case DESTROY:
                return DESTROY;
        }

        throw new IllegalArgumentException("Unknown phase " + phase.name());
    }
}

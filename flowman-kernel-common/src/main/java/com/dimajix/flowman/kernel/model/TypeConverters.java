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


import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import lombok.val;

import com.dimajix.flowman.kernel.proto.Timestamp;

public class TypeConverters {
    public static Timestamp toProto(ZonedDateTime dt) {
        val instant = dt.toInstant();
        val millis = instant.toEpochMilli();
        val seconds = millis / 1000;
        val nanos = millis % 1000;
        return Timestamp.newBuilder()
            .setSeconds(seconds)
            .setNanos((int)nanos)
            .build();
    }

    public static ZonedDateTime toModel(Timestamp ts) {
        val secs = ts.getSeconds();
        val nanos = ts.getNanos();
        return ZonedDateTime.ofInstant(Instant.ofEpochSecond(secs, nanos), ZoneId.systemDefault());
    }
}

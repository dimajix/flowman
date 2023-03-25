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

package com.dimajix.flowman.templating;

import java.time.Duration;

import com.dimajix.flowman.util.UtcTimestamp;


public class TimestampWrapper {
    public UtcTimestamp now() { return UtcTimestamp.now(); }
    public UtcTimestamp parse(String value) { return UtcTimestamp.parse(value); }
    public UtcTimestamp valueOf(String value) { return UtcTimestamp.parse(value); }
    public long toEpochSeconds(String value) { return UtcTimestamp.parse(value).toEpochSeconds(); }
    public String format(String value, String format) { return UtcTimestamp.parse(value).format(format); }
    public String format(UtcTimestamp value, String format) { return value.format(format); }
    public UtcTimestamp add(String value, String duration) { return UtcTimestamp.parse(value).plus(Duration.parse(duration)); }
    public UtcTimestamp add(String value, Duration duration) { return UtcTimestamp.parse(value).plus(duration); }
    public UtcTimestamp add(UtcTimestamp value, String duration) { return value.plus(Duration.parse(duration)); }
    public UtcTimestamp add(UtcTimestamp value, Duration duration) { return value.plus(duration); }
    public UtcTimestamp subtract(String value, String duration) { return UtcTimestamp.parse(value).minus(Duration.parse(duration)); }
    public UtcTimestamp subtract(String value, Duration duration) { return UtcTimestamp.parse(value).minus(duration); }
    public UtcTimestamp subtract(UtcTimestamp value, String duration) { return value.minus(Duration.parse(duration)); }
    public UtcTimestamp subtract(UtcTimestamp value, Duration duration) { return value.minus(duration); }
}

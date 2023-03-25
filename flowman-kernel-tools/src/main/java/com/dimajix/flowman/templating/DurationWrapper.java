/*
 * Copyright (C) 2023 The Flowman Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); }
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
import java.time.temporal.Temporal;


public class DurationWrapper {
    public Duration ofDays(String days) { return Duration.ofDays(Long.parseLong(days)); }
    public Duration ofDays(Long days) { return Duration.ofDays(days); }
    public Duration ofHours(String hours) { return Duration.ofHours(Long.parseLong(hours)); }
    public Duration ofHours(Long hours) { return Duration.ofHours(hours); }
    public Duration ofMinutes(String minutes) { return Duration.ofMinutes(Long.parseLong(minutes)); }
    public Duration ofMinutes(Long minutes) { return Duration.ofMinutes(minutes); }
    public Duration ofSeconds(String seconds) { return Duration.ofSeconds(Long.parseLong(seconds)); }
    public Duration ofSeconds(Long seconds) { return Duration.ofSeconds(seconds); }
    public Duration ofMillis(String millis) { return Duration.ofMillis(Long.parseLong(millis)); }
    public Duration ofMillis(Long millis) { return Duration.ofMillis(millis); }
    public Duration between(Temporal startInclusive, Temporal endExclusive) { return Duration.between(startInclusive, endExclusive); }
    public Duration parse(String value) { return Duration.parse(value); }
    public Duration valueOf(String value) { return Duration.parse(value); }
}

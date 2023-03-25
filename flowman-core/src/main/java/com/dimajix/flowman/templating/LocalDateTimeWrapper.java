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
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.Temporal;

import com.dimajix.flowman.util.UtcTimestamp;


public class LocalDateTimeWrapper {
    public LocalDateTime now() { return LocalDateTime.now(ZoneOffset.UTC); }
    public LocalDateTime parse(String value) { return LocalDateTime.parse(value); }
    public LocalDateTime valueOf(String value) { return LocalDateTime.parse(value); }
    public LocalDateTime ofEpochSeconds(String epoch) { return LocalDateTime.ofEpochSecond(Long.parseLong(epoch), 0, ZoneOffset.UTC); }
    public LocalDateTime ofEpochSeconds(long epoch) { return LocalDateTime.ofEpochSecond(epoch, 0, ZoneOffset.UTC); }
    public String format(String value, String format) { return DateTimeFormatter.ofPattern(format).format(LocalDateTime.parse(value)); }
    public String format(Temporal value, String format) { return DateTimeFormatter.ofPattern(format).format(value); }
    public String format(UtcTimestamp value, String format) { return DateTimeFormatter.ofPattern(format).format(value.toLocalDateTime()); }
    public LocalDateTime add(String value, String duration) { return LocalDateTime.parse(value).plus(Duration.parse(duration)); }
    public LocalDateTime add(String value, Duration duration) { return LocalDateTime.parse(value).plus(duration); }
    public LocalDateTime add(LocalDateTime value, String duration) { return value.plus(Duration.parse(duration)); }
    public LocalDateTime add(LocalDateTime value, Duration duration) { return value.plus(duration); }
    public LocalDateTime subtract(String value, String duration) { return LocalDateTime.parse(value).minus(Duration.parse(duration)); }
    public LocalDateTime subtract(String value, Duration duration) { return LocalDateTime.parse(value).minus(duration); }
    public LocalDateTime subtract(LocalDateTime value, String duration) { return value.minus(Duration.parse(duration)); }
    public LocalDateTime subtract(LocalDateTime value, Duration duration) { return value.minus(duration); }
    public LocalDateTime addSeconds(String value, int seconds) { return LocalDateTime.parse(value).plusSeconds(seconds); }
    public LocalDateTime addSeconds(LocalDateTime value, int seconds) { return value.plusSeconds(seconds); }
    public LocalDateTime addMinutes(String value, int minutes) { return LocalDateTime.parse(value).plusMinutes(minutes); }
    public LocalDateTime addMinutes(LocalDateTime value, int minutes) { return value.plusMinutes(minutes); }
    public LocalDateTime addHours(String value, int hours) { return LocalDateTime.parse(value).plusHours(hours); }
    public LocalDateTime addHours(LocalDateTime value, int hours) { return value.plusHours(hours); }
    public LocalDateTime addDays(String value, int days) { return LocalDateTime.parse(value).plusDays(days); }
    public LocalDateTime addDays(LocalDateTime value, int days) { return value.plusDays(days); }
    public LocalDateTime addWeeks(String value, int weeks) { return LocalDateTime.parse(value).plusWeeks(weeks); }
    public LocalDateTime addWeeks(LocalDateTime value, int weeks) { return value.plusWeeks(weeks); }
    public LocalDateTime addMonths(String value, int months) { return LocalDateTime.parse(value).plusMonths(months); }
    public LocalDateTime addMonths(LocalDateTime value, int months) { return value.plusMonths(months); }
    public LocalDateTime addYears(String value, int days) { return LocalDateTime.parse(value).plusYears(days); }
    public LocalDateTime addYears(LocalDateTime value, int days) { return value.plusYears(days); }
}

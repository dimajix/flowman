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


import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.Temporal;


public class LocalDateWrapper {
    public LocalDate now() { return LocalDate.now(ZoneOffset.UTC); }
    public LocalDate today() { return LocalDate.now(ZoneOffset.UTC); }
    public LocalDate parse(String value) { return LocalDate.parse(value); }
    public LocalDate valueOf(String value) { return LocalDate.parse(value); }
    public String format(String value, String format) { return DateTimeFormatter.ofPattern(format).format(LocalDate.parse(value)); }
    public String format(Temporal value, String format) { return DateTimeFormatter.ofPattern(format).format(value); }
    public LocalDate addDays(String value, int days) { return LocalDate.parse(value).plusDays(days); }
    public LocalDate addDays(LocalDate value, int days) { return value.plusDays(days); }
    public LocalDate addWeeks(String value, int weeks) { return LocalDate.parse(value).plusWeeks(weeks); }
    public LocalDate addWeeks(LocalDate value, int weeks) { return value.plusWeeks(weeks); }
    public LocalDate addMonths(String value, int months) { return LocalDate.parse(value).plusMonths(months); }
    public LocalDate addMonths(LocalDate value, int months) { return value.plusMonths(months); }
    public LocalDate addYears(String value, int days) { return LocalDate.parse(value).plusYears(days); }
    public LocalDate addYears(LocalDate value, int days) { return value.plusYears(days); }
}

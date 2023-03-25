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

import java.time.Period;


public class PeriodWrapper {
    public Period ofYears(String years) { return Period.ofYears(Integer.valueOf(years)); }
    public Period ofYears(int years) { return Period.ofYears(years); }
    public Period ofMonths(String months) { return Period.ofMonths(Integer.valueOf(months)); }
    public Period ofMonths(int months) { return Period.ofMonths(months); }
    public Period ofWeeks(String weeks) { return Period.ofWeeks(Integer.valueOf(weeks)); }
    public Period ofWeeks(int weeks) { return Period.ofWeeks(weeks); }
    public Period ofDays(String days) { return Period.ofDays(Integer.valueOf(days)); }
    public Period ofDays(int days) { return Period.ofDays(days); }
    public Period parse(String value) { return Period.parse(value); }
    public Period valueOf(String value) { return Period.parse(value); }
}

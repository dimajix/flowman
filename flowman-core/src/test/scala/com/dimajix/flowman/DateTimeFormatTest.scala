/*
 * Copyright 2018 Kaya Kupferschmidt
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

package com.dimajix.flowman

import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * Created by kaya on 11.10.16.
  */
class DateTimeFormatTest extends AnyFlatSpec with Matchers {
    "The instant" should "be formattable in UTC" in {
        val instant = Instant.ofEpochSecond(123456789l).atZone(ZoneId.of("UTC"))
        val formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd/HH")
        formatter.format(instant) should be ("1973/11/29/21")
    }
}

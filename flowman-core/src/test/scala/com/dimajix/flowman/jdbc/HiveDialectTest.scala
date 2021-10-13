/*
 * Copyright 2018-2021 Kaya Kupferschmidt
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

package com.dimajix.flowman.jdbc

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.util.UtcTimestamp


class HiveDialectTest extends AnyFlatSpec with Matchers {
    "The HiveDialect" should "correctly transform literals" in {
        HiveDialect.literal(true) should be ("true")
        HiveDialect.literal("some literal") should be ("'some literal'")
        HiveDialect.literal(123) should be ("123")
        HiveDialect.literal(java.sql.Date.valueOf("2021-08-03")) should be ("date('2021-08-03')")
        HiveDialect.literal(UtcTimestamp.parse("2021-08-03T02:03:44")) should be ("timestamp(1627956224)")
    }
}

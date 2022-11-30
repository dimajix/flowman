/*
 * Copyright 2022 Kaya Kupferschmidt
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


package com.dimajix.spark.sql

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.spark.testing.Logging


class ExpressionParserTest extends AnyFlatSpec with Matchers with Logging {
    "The ExpressionParser" should "retrieve all dependencies" in {
        val deps1 = ExpressionParser.resolveDependencies("x != y")
        deps1 should be (Set.empty)

        val deps2 = ExpressionParser.resolveDependencies("x NOT IN (SELECT id FROM duplicates)")
        deps2 should be (Set("duplicates"))
    }
}

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

package com.dimajix.flowman.spec.schema

import org.scalatest.FlatSpec
import org.scalatest.Matchers


class DoubleTypeTest  extends FlatSpec with Matchers {
    "A DoubleType" should "parse strings" in {
        DoubleType.parse("1.0") should be (1.0)
    }

    it should "support interpolation of SingleValues" in {
        DoubleType.interpolate(SingleValue("1.0"), null).head should be (1.0)
    }

    it should "support interpolation of ArrayValues" in {
        val result = DoubleType.interpolate(ArrayValue("1.0", "2.0"), null).toSeq
        result(0) should be (1.0)
        result(1) should be (2.0)
    }

    it should "provide the correct SQL type" in {
        val ftype = DoubleType
        ftype.sqlType should be ("double")
    }
}

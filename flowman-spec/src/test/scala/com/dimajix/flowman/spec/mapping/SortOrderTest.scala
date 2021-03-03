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

package com.dimajix.flowman.spec.mapping

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class SortOrderTest extends AnyFlatSpec with Matchers {
    "The SortOrder" should "be parsable" in {
        SortOrder.of("desc") should be (SortOrder(Descending, NullsLast))
        SortOrder.of(" desc ") should be (SortOrder(Descending, NullsLast))
        SortOrder.of("desc  nulls  last") should be (SortOrder(Descending, NullsLast))
        SortOrder.of("desc  nulls  first  ") should be (SortOrder(Descending, NullsFirst))
        SortOrder.of("DESC  NULLS  FIRST  ") should be (SortOrder(Descending, NullsFirst))

        SortOrder.of("asc") should be (SortOrder(Ascending, NullsFirst))
        SortOrder.of(" asc ") should be (SortOrder(Ascending, NullsFirst))
        SortOrder.of("asc nulls last") should be (SortOrder(Ascending, NullsLast))
        SortOrder.of("  asc  nulls  first  ") should be (SortOrder(Ascending, NullsFirst))
        SortOrder.of("ASC  NULLS  FIRST  ") should be (SortOrder(Ascending, NullsFirst))

        an[IllegalArgumentException] should be thrownBy(SortOrder.of("lala"))
        an[IllegalArgumentException] should be thrownBy(SortOrder.of("desc nulls"))
        an[IllegalArgumentException] should be thrownBy(SortOrder.of("desc nulls lala"))
        an[IllegalArgumentException] should be thrownBy(SortOrder.of("desc lala lala"))
    }
}

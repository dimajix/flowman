/*
 * Copyright 2021 Kaya Kupferschmidt
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

package com.dimajix.flowman.types

import java.time.Duration

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.util.ObjectMapper


class DurationTypeTest extends AnyFlatSpec with Matchers {
    "A DurationType" should "be deserializable" in {
        ObjectMapper.parse[FieldType]("duration") should be(DurationType)
    }

    it should "parse strings" in {
        DurationType.parse("P2D") should be (Duration.ofDays(2))
        DurationType.parse("PT3H") should be (Duration.ofHours(3))
    }

    it should "support interpolation of SingleValues" in {
        DurationType.interpolate(SingleValue("P2D")).head should be (Duration.ofDays(2))
    }

    it should "support interpolation of SingleValues with granularity" in {
        DurationType.interpolate(SingleValue("P2DT3H"), Some("PT1H")).head should be (Duration.ofHours(2*24+3))
        DurationType.interpolate(SingleValue("P2DT3H"), Some("P1D")).head should be (Duration.ofDays(2))
    }

    it should "support interpolation of ArrayValues" in {
        DurationType.interpolate(ArrayValue(Array("P2DT3H","P3D"))).toSeq should
            be (Seq(Duration.ofHours(2*24+3), Duration.ofHours(3*24)))
    }

    it should "support interpolation of ArrayValues with granularity" in {
        DurationType.interpolate(ArrayValue(Array("P2DT3H","P3D")), Some("P1D")).toSeq should
            be (Seq(Duration.ofDays(2), Duration.ofDays(3)))
        DurationType.interpolate(ArrayValue(Array("P2DT3H","P3D")), Some("PT1H")).toSeq should
            be (Seq(Duration.ofHours(2*24+3), Duration.ofHours(3*24)))
    }

    it should "support interpolation of Ranges" in {
        DurationType.interpolate(RangeValue("P1D","P1DT6H", Some("PT1H")), None).toSeq should
            be(Seq(
                Duration.ofHours(1*24),
                Duration.ofHours(1*24+1),
                Duration.ofHours(1*24+2),
                Duration.ofHours(1*24+3),
                Duration.ofHours(1*24+4),
                Duration.ofHours(1*24+5)
            ))
    }

    it should "support interpolation of Ranges with granularity" in {
        DurationType.interpolate(RangeValue("P1D","P1DT6H", Some("PT1H")), Some("PT2H")).toSeq should
            be(Seq(
                Duration.ofHours(1*24),
                Duration.ofHours(1*24+2),
                Duration.ofHours(1*24+4)
            ))
    }

    it should "provide the correct Spark type" in {
        an[NotImplementedError] should be thrownBy(DurationType.sparkType)
    }

    it should "provide the correct SQL type" in {
        DurationType.sqlType should be ("duration")
        DurationType.typeName should be ("duration")
    }
}

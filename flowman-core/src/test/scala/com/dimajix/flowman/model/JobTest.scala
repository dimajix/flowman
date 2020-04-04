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

package com.dimajix.flowman.model

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatestplus.mockito.MockitoSugar

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.types.ArrayValue
import com.dimajix.flowman.types.IntegerType
import com.dimajix.flowman.types.RangeValue
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.types.StringType


class JobTest extends FlatSpec with Matchers with MockitoSugar {
    "Job.parseArguments" should "parse arguments" in {
        val session = Session.builder().build()
        val context = session.context
        val job = Job.builder(context)
            .addParameter("p1", IntegerType, Some("2"))
            .addParameter("p2", StringType)
            .build()

        val arguments = job.parseArguments(Map(
            "p1:start" -> "17",
            "p1:end" -> "27",
            "p2" -> "lala"
        ))

        arguments("p1") should be (RangeValue("17", "27"))
        arguments("p2") should be (SingleValue("lala"))
    }

    it should "throw an exception for unknown parameters" in {
        val session = Session.builder().build()
        val context = session.context
        val job = Job.builder(context)
            .addParameter("p1", IntegerType, Some("2"))
            .addParameter("p2", StringType)
            .build()

        an[IllegalArgumentException] should be thrownBy (job.parseArguments(Map(
            "p3" -> "lala"
        )))
    }

    "Job.interpolate" should "interpolate arguments" in {
        val session = Session.builder().build()
        val context = session.context
        val job = Job.builder(context)
            .addParameter("p1", IntegerType, Some("2"))
            .addParameter("p2", StringType)
            .build()

        val args = job.interpolate(Map(
            "p1"-> RangeValue("2", "8"),
            "p2" -> ArrayValue("x", "y", "z")
        ))

        args.toSet should be (Set(
            Map("p1" -> 2, "p2" -> "x"),
            Map("p1" -> 4, "p2" -> "x"),
            Map("p1" -> 6, "p2" -> "x"),
            Map("p1" -> 2, "p2" -> "y"),
            Map("p1" -> 4, "p2" -> "y"),
            Map("p1" -> 6, "p2" -> "y"),
            Map("p1" -> 2, "p2" -> "z"),
            Map("p1" -> 4, "p2" -> "z"),
            Map("p1" -> 6, "p2" -> "z")
        ))
    }

    it should "work with simple arguments" in {
        val session = Session.builder().build()
        val context = session.context
        val job = Job.builder(context)
            .addParameter("p1", IntegerType, Some("2"))
            .addParameter("p2", StringType)
            .build()

        val args = job.interpolate(Map(
            "p1"-> SingleValue("2"),
            "p2" -> SingleValue("x")
        ))

        args.toSet should be (Set(
            Map("p1" -> 2, "p2" -> "x")
        ))
    }

    it should "support granularity" in {
        val session = Session.builder().build()
        val context = session.context
        val job = Job.builder(context)
            .addParameter("p1", IntegerType, Some("2"))
            .addParameter("p2", StringType)
            .build()

        job.interpolate(Map(
            "p1"-> RangeValue("1", "7")
        )).toSet  should be (Set(
            Map("p1" -> 0),
            Map("p1" -> 2),
            Map("p1" -> 4)
        ))

        job.interpolate(Map(
            "p1"-> RangeValue("7", "7")
        )).toSet  should be (Set())

        job.interpolate(Map(
            "p1"-> RangeValue("7", "8")
        )).toSet  should be (Set(
            Map("p1" -> 6)
        ))
    }
}

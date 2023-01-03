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

package com.dimajix.flowman.model

import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.types.ArrayValue
import com.dimajix.flowman.types.IntegerType
import com.dimajix.flowman.types.RangeValue
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.types.StringType


class JobTest extends AnyFlatSpec with Matchers with MockFactory {
    "Job.Builder" should "work" in {
        val session = Session.builder().disableSpark().build()
        val context = session.context
        val job = Job.builder(context)
            .setProperties(Job.Properties(context, "some_job"))
            .setDescription("Some job")
            .setParameters(Seq(Job.Parameter("p1", IntegerType)))
            .addParameter(Job.Parameter("p2", IntegerType))
            .addParameter("p3", StringType)
            .setEnvironment(Map("env1" -> "eval_1"))
            .addEnvironment("env2", "eval_2")
            .build()

        job.name should be ("some_job")
        job.description should be (Some("Some job"))
        job.parameters should be (Seq(
            Job.Parameter("p1", IntegerType),
            Job.Parameter("p2", IntegerType),
            Job.Parameter("p3", StringType)
        ))
        job.environment should be (Map(
            "env1" -> "eval_1",
            "env2" -> "eval_2"
        ))

        val instance = job.digest(Phase.BUILD, Map("p1" -> "val1", "p2" -> "val2", "p3" -> "val3"))
        instance.job should be ("some_job")
        instance.namespace should be ("")
        instance.project should be ("")
        instance.args should be(Map("p1" -> "val1", "p2" -> "val2", "p3" -> "val3"))
        instance.asMap should be (Map(
            "job" -> "some_job",
            "name" -> "some_job",
            "namespace" -> "",
            "project" -> "",
            "phase" -> "BUILD",
            "p1" -> "val1", "p2" -> "val2", "p3" -> "val3"
        ))

        job.metadata should be (Metadata(
            None,
            None,
            "some_job",
            None,
            "job",
            "job",
            Map()
        ))

        session.shutdown()
    }

    "Job.arguments" should "parse arguments" in {
        val session = Session.builder().disableSpark().build()
        val context = session.context
        val job = Job.builder(context)
            .addParameter("p1", IntegerType, Some("2"))
            .addParameter("p2", StringType)
            .addParameter("p3", StringType, value=Some("default"))
            .build()

        job.arguments(Map(
            "p1" -> "17",
            "p2" -> "lala"
        )) should be (Map(
            "p1" -> 17,
            "p2" -> "lala",
            "p3" -> "default"
        ))

        session.shutdown()
    }

    it should "throw an error on missing arguments" in {
        val session = Session.builder().disableSpark().build()
        val context = session.context
        val job = Job.builder(context)
            .addParameter("p1", IntegerType, Some("2"))
            .addParameter("p2", StringType)
            .build()

        an[IllegalArgumentException] should be thrownBy(job.arguments(Map(
            "p1" -> "17"
        )))

        session.shutdown()
    }

    it should "throw an error on unknown arguments" in {
        val session = Session.builder().disableSpark().build()
        val context = session.context
        val job = Job.builder(context)
            .addParameter("p1", IntegerType, Some("2"))
            .addParameter("p2", StringType)
            .build()

        an[IllegalArgumentException] should be thrownBy(job.arguments(Map(
            "p1" -> "17",
            "p3" -> "28"
        )))

        session.shutdown()
    }

    "Job.parseArguments" should "parse arguments" in {
        val session = Session.builder().disableSpark().build()
        val context = session.context
        val job = Job.builder(context)
            .addParameter("p1", IntegerType, Some("2"))
            .addParameter("p2", StringType)
            .build()

        job.parseArguments(Map(
            "p1:start" -> "17",
            "p1:end" -> "27",
            "p2" -> "lala"
        )) should be (Map(
            "p1" -> RangeValue("17", "27"),
            "p2" -> SingleValue("lala")
        ))

        session.shutdown()
    }

    it should "throw an exception for unknown parameters" in {
        val session = Session.builder().disableSpark().build()
        val context = session.context
        val job = Job.builder(context)
            .addParameter("p1", IntegerType, Some("2"))
            .addParameter("p2", StringType)
            .build()

        an[IllegalArgumentException] should be thrownBy (job.parseArguments(Map(
            "p3" -> "lala"
        )))

        session.shutdown()
    }

    "Job.interpolate" should "interpolate arguments" in {
        val session = Session.builder().disableSpark().build()
        val context = session.context
        val job = Job.builder(context)
            .addParameter("p1", IntegerType, Some("2"))
            .addParameter("p2", StringType)
            .build()

        job.interpolate(Map(
            "p1"-> RangeValue("2", "8"),
            "p2" -> ArrayValue("x", "y", "z")
        )).toSet should be (Set(
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

        session.shutdown()
    }

    it should "work with simple arguments" in {
        val session = Session.builder().disableSpark().build()
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

        session.shutdown()
    }

    it should "support granularity" in {
        val session = Session.builder().disableSpark().build()
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

        session.shutdown()
    }

    "Job.merge" should "correctly merge Jobs" in {
        val session = Session.builder().disableSpark().build()
        val context = session.context

        val job = Job.builder(context)
            .setProperties(Job.Properties(context, "some_job"))
            .setDescription("Some job")
            .setParameters(Seq(Job.Parameter("p1", IntegerType)))
            .addParameter("p3", StringType)
            .setEnvironment(Map("env1" -> "eval_1", "env2" -> "eval_2", "p2" -> "17"))
            .setTargets(Seq(TargetIdentifier("t2"),TargetIdentifier("t7"),TargetIdentifier("t1"),TargetIdentifier("t3")))
            .build()
        val parent = Job.builder(context)
            .setProperties(Job.Properties(context, "parent_job"))
            .setDescription("Some parent job")
            .setParameters(Seq(Job.Parameter("p1", IntegerType), Job.Parameter("p2", IntegerType), Job.Parameter("p4", IntegerType)))
            .setEnvironment(Map("env1" -> "parent_val_1", "env4" -> "parent_val_4"))
            .setTargets(Seq(TargetIdentifier("t3"),TargetIdentifier("t4"),TargetIdentifier("t6"),TargetIdentifier("t5")))
            .build()

        val result = Job.merge(job, Seq(parent))
        result.name should be ("some_job")
        result.description should be (Some("Some job"))
        result.parameters should be (Seq(
            Job.Parameter("p1", IntegerType),
            Job.Parameter("p4", IntegerType),
            Job.Parameter("p3", StringType)
        ))
        result.targets should be (Seq(
            TargetIdentifier("t3"),
            TargetIdentifier("t4"),
            TargetIdentifier("t6"),
            TargetIdentifier("t5"),
            TargetIdentifier("t2"),
            TargetIdentifier("t7"),
            TargetIdentifier("t1")
        ))
        result.environment should be (Map(
            "env1" -> "eval_1",
            "env2" -> "eval_2",
            "env4" -> "parent_val_4",
            "p2" -> "17"
        ))

        session.shutdown()
    }
}

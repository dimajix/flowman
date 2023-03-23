/*
 * Copyright (C) 2021 The Flowman Authors
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

import java.time.Instant

import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Lifecycle
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.types.IntegerType
import com.dimajix.flowman.types.StringType


class TemplatingTest extends AnyFlatSpec with Matchers with MockFactory {
  "The JobWrapper" should "work" in {
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

    val wrapper = JobWrapper(job)
    context.evaluate("${job.name}", Map("job" -> wrapper)) should be("some_job")
    context.evaluate("${job.description}", Map("job" -> wrapper)) should be("Some job")
    context.evaluate("${job.identifier}", Map("job" -> wrapper)) should be("some_job")
    context.evaluate("${job.project}", Map("job" -> wrapper)) should be("")
    context.evaluate("${job.namespace}", Map("job" -> wrapper)) should be("")
    context.evaluate("${job.parameters}", Map("job" -> wrapper)) should be("[p1, p2, p3]")
    context.evaluate("${job.targets}", Map("job" -> wrapper)) should be("[]")
    context.evaluate("${job.environment}", Map("job" -> wrapper)) should be("{env1=eval_1, env2=eval_2}")

    session.shutdown()
  }

  "The JobResultWrapper" should "work" in {
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

    val instant = Instant.now()
    val result = JobResult(
      job,
      job.digest(Phase.VALIDATE, Map("p1" -> "1", "p2" -> "2", "p3" -> "str")),
      Status.SUCCESS,
      instant
    )

    val wrapper = JobResultWrapper(result)
    context.evaluate("${result.job.name}", Map("result" -> wrapper)) should be("some_job")
    context.evaluate("${result.job.description}", Map("result" -> wrapper)) should be("Some job")
    context.evaluate("${result.status}", Map("result" -> wrapper)) should be("SUCCESS")
    context.evaluate("${result.children}", Map("result" -> wrapper)) should be("[]")
    context.evaluate("${result.success}", Map("result" -> wrapper)) should be("true")
    context.evaluate("${result.failure}", Map("result" -> wrapper)) should be("false")
    context.evaluate("${result.skipped}", Map("result" -> wrapper)) should be("false")
    context.evaluate("${result.numFailures}", Map("result" -> wrapper)) should be("0")
    context.evaluate("${result.numSuccesses}", Map("result" -> wrapper)) should be("0")
    context.evaluate("${result.numExceptions}", Map("result" -> wrapper)) should be("0")
    context.evaluate("${result.startTime}", Map("result" -> wrapper)) should be(result.startTime.toString)
    context.evaluate("${result.endTime}", Map("result" -> wrapper)) should be(result.endTime.toString)
    context.evaluate("#foreach ($child in $result.children)${child.phase}:${child.status} #end", Map("result" -> wrapper)) should be("")

    session.shutdown()
  }

  "The LifecycleResultWrapper" should "work" in {
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

    val instant = Instant.now()
    val result = LifecycleResult(
      job,
      job.lifecycle(Lifecycle.BUILD, Map("p1" -> "1", "p2" -> "2", "p3" -> "str")),
      Seq(
        JobResult(job, job.digest(Phase.VALIDATE, Map("p1" -> "1", "p2" -> "2", "p3" -> "str")), Status.SUCCESS, instant),
        JobResult(job, job.digest(Phase.CREATE, Map("p1" -> "1", "p2" -> "2", "p3" -> "str")), Status.SUCCESS, instant),
        JobResult(job, job.digest(Phase.BUILD, Map("p1" -> "1", "p2" -> "2", "p3" -> "str")), Status.SUCCESS, instant)
      ),
      instant)

    val wrapper = LifecycleResultWrapper(result)
    context.evaluate("${result.job.name}", Map("result" -> wrapper)) should be("some_job")
    context.evaluate("${result.job.description}", Map("result" -> wrapper)) should be("Some job")
    context.evaluate("${result.status}", Map("result" -> wrapper)) should be("SUCCESS")
    context.evaluate("${result.children}", Map("result" -> wrapper)) should be("[SUCCESS, SUCCESS, SUCCESS]")
    context.evaluate("${result.success}", Map("result" -> wrapper)) should be("true")
    context.evaluate("${result.failure}", Map("result" -> wrapper)) should be("false")
    context.evaluate("${result.skipped}", Map("result" -> wrapper)) should be("false")
    context.evaluate("${result.numFailures}", Map("result" -> wrapper)) should be("0")
    context.evaluate("${result.numSuccesses}", Map("result" -> wrapper)) should be("3")
    context.evaluate("${result.numExceptions}", Map("result" -> wrapper)) should be("0")
    context.evaluate("${result.startTime}", Map("result" -> wrapper)) should be(result.startTime.toString)
    context.evaluate("${result.endTime}", Map("result" -> wrapper)) should be(result.endTime.toString)
    context.evaluate("#foreach ($child in $result.children)${child.phase}:${child.status} #end", Map("result" -> wrapper)) should be("VALIDATE:SUCCESS CREATE:SUCCESS BUILD:SUCCESS ")

    session.shutdown()
  }
}

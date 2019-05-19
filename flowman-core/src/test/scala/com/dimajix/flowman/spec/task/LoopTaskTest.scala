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

package com.dimajix.flowman.spec.task

import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.mockito.MockitoSugar

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.Module
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.state.Status
import com.dimajix.flowman.types.ArrayValue
import com.dimajix.flowman.types.IntegerType
import com.dimajix.flowman.types.RangeValue
import com.dimajix.flowman.types.StringType


class LoopTaskTest extends FlatSpec with Matchers with MockitoSugar {
    "The LoopTask" should "support simple loops" in {
        val spec =
            """
              |jobs:
              |  child:
              |    parameters:
              |      - name: p1
              |      - name: p2
              |        default: v2
              |      - name: p3
              |        type: Integer
              |        default: 7
              |  loop:
              |    tasks:
              |      - kind: loop
              |        job: child
              |        args:
              |          p1: xyz
              |          p2: [abc, xyz]
              |          p3:
              |            start: 1
              |            end: 3
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().build()
        val executor = session.getExecutor(project)

        val job = project.jobs("loop").instantiate(session.context)
        job.execute(executor, Map()) shouldBe (Status.SUCCESS)
    }

    it should "fail on undefined parameters" in {
        val spec =
            """
              |jobs:
              |  child:
              |    parameters:
              |      - name: p1
              |  loop:
              |    tasks:
              |      - kind: loop
              |        job: child
              |        args:
              |          p2: [abc, xyz]
            """.stripMargin

        val project = Module.read.string(spec).toProject("project")
        val session = Session.builder().build()
        val executor = session.getExecutor(project)

        val job = project.jobs("loop").instantiate(session.context)
        job.execute(executor, Map()) shouldBe (Status.FAILED)
    }

    it should "loop as expected with a single parameter" in {
        val loopJob = mock[Job]
        val loopTask = new LoopTask("job", Map("p1" -> ArrayValue("v1", "v2")))

        val session = Session.builder().build()
        val executor = session.executor

        when(loopJob.parameters).thenReturn(Seq(JobParameter("p1", StringType)))
        when(loopJob.execute(executor, Map("p1" -> "v1"))).thenReturn(Status.SUCCESS)
        when(loopJob.execute(executor, Map("p1" -> "v2"))).thenReturn(Status.SUCCESS)

        loopTask.execute(executor)

        verify(loopJob).execute(executor, Map("p1" -> "v1"))
        verify(loopJob).execute(executor, Map("p1" -> "v2"))
    }

    it should "loop as expected with a multiple parameter" in {
        val loopJob = mock[Job]
        val loopTask = new LoopTask("job", Map("p1" -> ArrayValue("v1", "v2"), "p2" -> RangeValue("2", "8")))

        val session = Session.builder().build()
        val executor = session.executor

        when(loopJob.parameters).thenReturn(Seq(JobParameter("p1", StringType), JobParameter("p2", IntegerType, "2")))
        when(loopJob.execute(executor, Map("p1" -> "v1", "p2" -> "2"))).thenReturn(Status.SUCCESS)
        when(loopJob.execute(executor, Map("p1" -> "v1", "p2" -> "4"))).thenReturn(Status.SUCCESS)
        when(loopJob.execute(executor, Map("p1" -> "v1", "p2" -> "6"))).thenReturn(Status.SUCCESS)
        when(loopJob.execute(executor, Map("p1" -> "v2", "p2" -> "2"))).thenReturn(Status.SUCCESS)
        when(loopJob.execute(executor, Map("p1" -> "v2", "p2" -> "4"))).thenReturn(Status.SUCCESS)
        when(loopJob.execute(executor, Map("p1" -> "v2", "p2" -> "6"))).thenReturn(Status.SUCCESS)

        loopTask.execute(executor)

        verify(loopJob).execute(executor, Map("p1" -> "v1", "p2" -> "2"))
        verify(loopJob).execute(executor, Map("p1" -> "v1", "p2" -> "4"))
        verify(loopJob).execute(executor, Map("p1" -> "v1", "p2" -> "6"))
        verify(loopJob).execute(executor, Map("p1" -> "v2", "p2" -> "2"))
        verify(loopJob).execute(executor, Map("p1" -> "v2", "p2" -> "4"))
        verify(loopJob).execute(executor, Map("p1" -> "v2", "p2" -> "6"))
    }

    it should "loop as expected with a single parameter and predefined value" in {
        val loopJob = mock[Job]
        val loopTask = new LoopTask("job", Map("p1" -> ArrayValue("v1", "v2")))

        val session = Session.builder().build()
        val executor = session.executor

        when(loopJob.parameters).thenReturn(Seq(JobParameter("p1", StringType), JobParameter("p2", IntegerType, "2", "4")))
        when(loopJob.execute(executor, Map("p1" -> "v1"))).thenReturn(Status.SUCCESS)
        when(loopJob.execute(executor, Map("p1" -> "v2"))).thenReturn(Status.SUCCESS)

        loopTask.execute(executor)

        verify(loopJob).execute(executor, Map("p1" -> "v1"))
        verify(loopJob).execute(executor, Map("p1" -> "v2"))
    }
}

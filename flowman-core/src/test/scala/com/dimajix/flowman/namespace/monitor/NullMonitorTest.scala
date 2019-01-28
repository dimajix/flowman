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

package com.dimajix.flowman.namespace.monitor

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.namespace.Namespace
import com.dimajix.flowman.spec.ObjectMapper
import com.dimajix.flowman.spec.task.Job


class NullMonitorTest extends FlatSpec with Matchers {
    "The NullMonitor" should "work" in {
        val job = Job.builder()
            .setName("job")
            .build()
        val ns = Namespace.builder()
            .build()
        val session = Session.builder()
            .withNamespace(ns)
            .build()

        val monitor = new NullMonitor
        monitor.check(session.context, job, Map()) should be(false)
        val token = monitor.start(session.context, job, Map())
        monitor.check(session.context, job, Map()) should be(false)
        monitor.success(session.context, token)
        monitor.check(session.context, job, Map()) should be(false)
    }

    it should "be parseable" in {
        val spec =
            """
              |kind: null
            """.stripMargin

        val monitor = ObjectMapper.parse[Monitor](spec)
        monitor shouldBe a[NullMonitor]
    }

}

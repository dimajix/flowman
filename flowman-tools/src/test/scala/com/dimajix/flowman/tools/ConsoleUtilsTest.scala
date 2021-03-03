/*
 * Copyright 2020 Kaya Kupferschmidt
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

package com.dimajix.flowman.tools

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.history.JobState


class ConsoleUtilsTest extends AnyFlatSpec with Matchers {
    "ConsoleUtils" should "correctly format a list of JobStates" in {
        val columns = Seq("id", "namespace", "project", "job", "phase", "args", "status", "start_dt", "end_dt")
        val records = Seq(JobState("123", "default", "p1", "some_job", Phase.BUILD, Map("arg1" -> "val1"), Status.SUCCESS, None, None))
        val result = ConsoleUtils.showTableString(records, columns)
        result should be ("""+---+---------+-------+--------+-----+---------+-------+--------+------+
                            || id|namespace|project|     job|phase|     args| status|start_dt|end_dt|
                            |+---+---------+-------+--------+-----+---------+-------+--------+------+
                            ||123|  default|     p1|some_job|build|arg1=val1|success|        |      |
                            |+---+---------+-------+--------+-----+---------+-------+--------+------+
                            |""".stripMargin)
    }
}

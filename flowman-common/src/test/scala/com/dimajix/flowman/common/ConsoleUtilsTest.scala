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

package com.dimajix.flowman.common

import java.time.ZonedDateTime

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


object ConsoleUtilsTest {
    final case class JobState(
        id:String,
        namespace:String,
        project:String,
        job:String,
        phase:String,
        args:Map[String,String],
        status:String,
        startDateTime:Option[ZonedDateTime] = None,
        endDateTime:Option[ZonedDateTime] = None
    )
}
class ConsoleUtilsTest extends AnyFlatSpec with Matchers {
    import ConsoleUtilsTest.JobState

    "ConsoleUtils" should "show Product types as tables" in {
        val records = Seq(JobState("123", "default", "p1", "some_job", "BUILD", Map("arg1" -> "val1"), "SUCCESS", None, None))
        val result = ConsoleUtils.showTableString(records)
        result should be ("""+---+---------+-------+--------+-----+---------+-------+-------------+-----------+
                            || id|namespace|project|     job|phase|     args| status|startDateTime|endDateTime|
                            |+---+---------+-------+--------+-----+---------+-------+-------------+-----------+
                            ||123|  default|     p1|some_job|BUILD|arg1=val1|SUCCESS|             |           |
                            |+---+---------+-------+--------+-----+---------+-------+-------------+-----------+
                            |""".stripMargin)
    }

    it should "show Product types as tables with explicit column names" in {
        val columns = Seq("id", "namespace", "project", "job", "phase", "args", "status", "start_dt", "end_dt")
        val records = Seq(JobState("123", "default", "p1", "some_job", "BUILD", Map("arg1" -> "val1"), "SUCCESS", None, None))
        val result = ConsoleUtils.showTableString(records, columns)
        result should be ("""+---+---------+-------+--------+-----+---------+-------+--------+------+
                            || id|namespace|project|     job|phase|     args| status|start_dt|end_dt|
                            |+---+---------+-------+--------+-----+---------+-------+--------+------+
                            ||123|  default|     p1|some_job|BUILD|arg1=val1|SUCCESS|        |      |
                            |+---+---------+-------+--------+-----+---------+-------+--------+------+
                            |""".stripMargin)
    }
}

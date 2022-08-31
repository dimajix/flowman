/*
 * Copyright 2020-2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.tools.exec.test

import scala.util.control.NonFatal

import org.kohsuke.args4j.Argument
import org.slf4j.LoggerFactory

import com.dimajix.common.ExceptionUtils.reasons
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.NoSuchTestException
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.TestIdentifier
import com.dimajix.flowman.tools.exec.Command


class InspectCommand extends Command {
    private val logger = LoggerFactory.getLogger(classOf[InspectCommand])

    @Argument(index=0, required=true, usage = "name of test to inspect", metaVar = "<test>")
    var test: String = ""

    override def execute(session: Session, project:Project, context:Context): Status = {
        try {
            val test = context.getTest(TestIdentifier(this.test))
            println(s"Name: ${test.name}")
            println(s"Description: ${test.description}")
            println("Environment:")
            test.environment
                .toSeq
                .sortBy(_._1)
                .foreach{ case(k,v) => println(s"    $k=$v") }
            println("Mapping Overrides:")
            test.overrideMappings.keySet
                .toSeq
                .sorted
                .foreach{ p => println(s"    $p") }
            println("Relation Overrides:")
            test.overrideRelations.keySet
                .toSeq
                .sorted
                .foreach{ p => println(s"    $p") }
            println("Fixture Targets:")
            test.fixtures.keySet
                .toSeq
                .sorted
                .foreach{ p => println(s"    $p") }
            println("Build Targets:")
            test.targets
                .foreach{ p => println(s"    $p") }
            println("Assertions:")
            test.assertions.keySet
                .toSeq
                .sorted
                .foreach{ p => println(s"    $p") }
            Status.SUCCESS
        }
        catch {
            case ex:NoSuchTestException =>
                logger.error(s"Cannot resolve test '${ex.test}'")
                Status.FAILED
            case NonFatal(e) =>
                logger.error(s"Error inspecting '$test':\n  ${reasons(e)}")
                Status.FAILED
        }
    }
}

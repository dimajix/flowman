/*
 * Copyright 2018-2020 Kaya Kupferschmidt
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

package com.dimajix.flowman.tools.exec.model

import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.control.NonFatal

import org.kohsuke.args4j.Argument
import org.kohsuke.args4j.Option
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.spec.target.ConsoleTarget
import com.dimajix.flowman.tools.ParserUtils
import com.dimajix.flowman.tools.exec.ActionCommand
import com.dimajix.flowman.types.SingleValue


class ShowCommand extends ActionCommand {
    private val logger = LoggerFactory.getLogger(classOf[ShowCommand])

    @Argument(index=0, usage="specifies the relation to show", metaVar="<relation>", required=true)
    var relation: String = ""
    @Argument(index=1, usage = "specifies the columns to show as a comma separated list", metaVar="<columns>", required=false)
    var columns: String = ""
    @Option(name="-n", aliases=Array("--limit"), usage="Specifies maximum number of rows to print", metaVar="<limit>", required=false)
    var limit: Int = 10
    @Option(name="-p", aliases=Array("--partition"), usage = "specify partition to work on, as partition1=value1,partition2=value2")
    var partition: String = ""

    override def executeInternal(session: Session, context:Context, project: Project) : Boolean = {
        val columns = ParserUtils.parseDelimitedList(this.columns)
        val partition = ParserUtils.parseDelimitedKeyValues(this.partition).map { case(k,v) => (k,SingleValue(v)) }
        val task = ConsoleTarget(context, RelationIdentifier(relation), limit, columns, partition)

        Try {
            task.execute(session.executor, Phase.BUILD)
        } match {
            case Success(_) =>
                logger.info("Successfully finished dumping relation")
                true
            case Failure(NonFatal(e)) =>
                logger.error(s"Caught exception while dumping relation '$relation'", e)
                false
        }
    }
}

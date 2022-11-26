/*
 * Copyright 2018-2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.tools.exec.relation

import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.control.NonFatal

import org.kohsuke.args4j.Argument
import org.kohsuke.args4j.Option
import org.slf4j.LoggerFactory

import com.dimajix.common.ExceptionUtils.reasons
import com.dimajix.flowman.common.ParserUtils
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.spec.target.ConsoleTarget
import com.dimajix.flowman.tools.exec.Command
import com.dimajix.flowman.types.SingleValue


class ShowCommand extends Command {
    private val logger = LoggerFactory.getLogger(classOf[ShowCommand])

    @Argument(index=0, usage="specifies the relation to show", metaVar="<relation>", required=true)
    var relation: String = ""
    @Argument(index=1, usage = "specifies the columns to show as a comma separated list", metaVar="<columns>", required=false)
    var columns: String = ""
    @Option(name="-n", aliases=Array("--limit"), usage="Specifies maximum number of rows to print", metaVar="<limit>", required=false)
    var limit: Int = 10
    @Option(name="-nh", aliases=Array("--no-header"), usage="Print header", required = false)
    var noHeader: Boolean = false
    @Option(name="-c", aliases=Array("--csv"), usage="Print data as csv", required = false)
    var csv: Boolean = false
    @Option(name="-p", aliases=Array("--partition"), usage = "specify partition to work on, as partition1=value1,partition2=value2")
    var partition: String = ""

    override def execute(session: Session, project: Project, context:Context) : Status = {
        val columns = ParserUtils.parseDelimitedList(this.columns)
        val partition = ParserUtils.parseDelimitedKeyValues(this.partition).map { case(k,v) => (k,SingleValue(v)) }
        val task = ConsoleTarget(context, RelationIdentifier(relation), limit, columns, partition, !noHeader, csv)

        task.execute(session.execution, Phase.BUILD).toTry match {
            case Success(s) =>
                s
            case Failure(e) =>
                logger.error(s"Caught exception while dumping relation '$relation':\n  ${reasons(e)}")
                Status.FAILED
        }
    }
}

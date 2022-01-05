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

package com.dimajix.flowman.tools.exec.model

import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.control.NonFatal

import org.kohsuke.args4j.Argument
import org.kohsuke.args4j.Option
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.NoSuchJobException
import com.dimajix.flowman.execution.NoSuchRelationException
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.tools.exec.Command


class DescribeCommand extends Command {
    private val logger = LoggerFactory.getLogger(classOf[DescribeCommand])

    @Option(name = "-s", aliases=Array("--spark"), usage = "use Spark to derive final schema")
    var useSpark: Boolean = false
    @Argument(usage = "specifies the relation to describe", metaVar = "<relation>", required = true)
    var relation: String = ""

    override def execute(session: Session, project: Project, context:Context) : Boolean = {
        try {
            val identifier = RelationIdentifier(this.relation)
            val relation = context.getRelation(identifier)

            if (useSpark) {
                val df = relation.read(session.execution, Map())
                df.printSchema()
            }
            else {
                val execution = session.execution
                val schema = relation.describe(execution)
                schema.printTree()
            }
            true
        } catch {
            case ex:NoSuchRelationException =>
                logger.error(s"Cannot resolve relation '${ex.relation}'")
                false
            case NonFatal(e) =>
                logger.error(s"Error describing relation '$relation':", e)
                false
        }
    }
}

/*
 * Copyright 2018-2019 Kaya Kupferschmidt
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

package com.dimajix.flowman.tools.exec.batch

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import org.kohsuke.args4j.Argument
import org.kohsuke.args4j.Option
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.execution.NoSuchBundleException
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.spec.JobIdentifier
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.spec.job.Job
import com.dimajix.flowman.spec.splitSettings
import com.dimajix.flowman.tools.exec.ActionCommand


class RunCommand extends ActionCommand {
    private val logger = LoggerFactory.getLogger(classOf[RunCommand])

    @Argument(index=0, required=true, usage = "specifies the lifecycle phase to execute", metaVar = "<phase>")
    var phase: String = "build"
    @Argument(index=1, required=false, usage = "specifies batch to run", metaVar = "<batch>")
    var batch: String = ""
    @Argument(index=2, required=false, usage = "specifies batch parameters", metaVar = "<param>=<value>")
    var args: Array[String] = Array()
    @Option(name = "-f", aliases=Array("--force"), usage = "forces execution, even if outputs are already created")
    var force: Boolean = false

    override def executeInternal(executor:Executor, context:Context, project: Project) : Boolean = {
        val args = splitSettings(this.args).toMap
        Try {
            context.getJob(BatchIdentifier(batch))
        }
        match {
            case Failure(_:NoSuchBundleException) =>
                logger.error(s"Cannot find batch '$batch'")
                false
            case Failure(_) =>
                logger.error(s"Error instantiating batch '$batch'")
                false
            case Success(batch) =>
                executeBatch(executor, batch, args)
        }
    }

    private def executeBatch(executor:Executor, batch:Job, args:Map[String,String]) : Boolean = {
        val batchDescription = batch.description.map("(" + _ + ")").getOrElse("")
        val batchArgs = args.map(kv => kv._1 + "=" + kv._2).mkString(", ")
        val batchPhase = Phase.ofString(phase)
        logger.info(s"Executing phase '$batchPhase' of batch '${batch.name}' $batchDescription with args $batchArgs")

        val runner = executor.runner
        val result = runner.executeBatch(executor, batch, batchPhase, args, force)
        result match {
            case Status.SUCCESS => true
            case Status.SKIPPED => true
            case _ => false
        }
    }
}

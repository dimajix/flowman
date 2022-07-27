/*
 * Copyright 2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.hook

import scala.util.control.NonFatal

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.JobToken
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.execution.TargetToken
import com.dimajix.flowman.execution.Token
import com.dimajix.flowman.jdbc.JdbcUtils.withConnection
import com.dimajix.flowman.jdbc.JdbcUtils.withStatement
import com.dimajix.flowman.model.BaseHook
import com.dimajix.flowman.model.Connection
import com.dimajix.flowman.model.Hook
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.JobDigest
import com.dimajix.flowman.model.JobResult
import com.dimajix.flowman.model.Reference
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetDigest
import com.dimajix.flowman.model.TargetResult
import com.dimajix.flowman.spec.connection.ConnectionReferenceSpec
import com.dimajix.flowman.spec.connection.JdbcConnection
import com.dimajix.flowman.spec.hook.JdbcHook.DummyJobToken
import com.dimajix.flowman.spec.hook.JdbcHook.DummyTargetToken
import com.dimajix.flowman.util.ConsoleColors.yellow


object JdbcHook {
    private case class DummyJobToken(env:Map[String,String]) extends JobToken
    private case class DummyTargetToken(env:Map[String,String]) extends TargetToken
}


case class JdbcHook(
    instanceProperties: Hook.Properties,
    connection:Reference[Connection],
    jobStart:Option[String] = None,
    jobFinish:Option[String] = None,
    jobSuccess:Option[String] = None,
    jobSkip:Option[String] = None,
    jobFailure:Option[String] = None,
    targetStart:Option[String] = None,
    targetFinish:Option[String] = None,
    targetSuccess:Option[String] = None,
    targetSkip:Option[String] = None,
    targetFailure:Option[String] = None
) extends BaseHook {
    private val logger = LoggerFactory.getLogger(classOf[JdbcHook])
    private val options = {
        val con = connection.value.asInstanceOf[JdbcConnection]
        val props = con.toConnectionProperties() + (JDBCOptions.JDBC_TABLE_NAME -> "dummy")
        new JDBCOptions(props)
    }

    /**
     * Starts the run and returns a token, which can be anything
     *
     * @param job
     * @return
     */
    override def startJob(execution:Execution, job:Job, instance: JobDigest, parent:Option[Token]): JobToken = {
        val env = instance.asMap -- context.environment.keys
        invoke(jobStart, env)
        DummyJobToken(env)
    }

    /**
     * Sets the status of a job after it has been started
     *
     * @param token The token returned by startJob
     * @param result
     */
    override def finishJob(execution:Execution, token: JobToken, result: JobResult): Unit = {
        val status = result.status
        val env = token.asInstanceOf[DummyJobToken].env + ("status" -> status)
        invoke(jobFinish, env)

        status match {
            case Status.FAILED | Status.ABORTED => invoke(jobFailure, env)
            case Status.SKIPPED  => invoke(jobSkip, env)
            case Status.SUCCESS  => invoke(jobSuccess, env)
            case _ =>
        }
    }

    /**
     * Starts the run and returns a token, which can be anything
     *
     * @param target
     * @return
     */
    override def startTarget(execution:Execution, target:Target, instance: TargetDigest, parent: Option[Token]): TargetToken =  {
        val parentEnv = parent.map {
            case t:DummyJobToken => t.env
            case _ => Map()
        }.getOrElse(Map())
        val env = parentEnv ++ instance.asMap -- context.environment.keys
        invoke(targetStart, env)
        DummyTargetToken(env)
    }

    /**
     * Sets the status of a job after it has been started
     *
     * @param token The token returned by startJob
     * @param result
     */
    override def finishTarget(execution:Execution, token: TargetToken, result: TargetResult): Unit = {
        val status = result.status
        val env = token.asInstanceOf[DummyTargetToken].env + ("status" -> status)
        invoke(targetFinish, env)

        status match {
            case Status.FAILED | Status.ABORTED => invoke(targetFailure, env)
            case Status.SKIPPED  => invoke(targetSkip, env)
            case Status.SUCCESS  => invoke(targetSuccess, env)
            case _ =>
        }
    }

    private def invoke(sqlTemplate:Option[String], args:Map[String,AnyRef]) : Unit = {
        sqlTemplate.foreach { v =>
            val sql = context.environment.evaluate(v, args)
            try {
                logger.info(s"Invoking external JDBC hook at ${options.url} with extra args $args")
                withConnection(options) { con =>
                    withStatement(con, options) { stmt =>
                        stmt.executeUpdate(sql)
                    }
                }
            }
            catch {
                case NonFatal(ex) =>
                    logger.warn(yellow(s"Could not post status to JDBC hook at '${options.url}': ${ex.toString}"))
            }
        }
    }
}


class JdbcHookSpec extends HookSpec {
    @JsonProperty(value="connection", required = true) private var connection:ConnectionReferenceSpec = _
    @JsonProperty(value="jobStart", required=false) private var jobStart:Option[String] = None
    @JsonProperty(value="jobFinish", required=false) private var jobFinish:Option[String] = None
    @JsonProperty(value="jobSuccess", required=false) private var jobSuccess:Option[String] = None
    @JsonProperty(value="jobSkip", required=false) private var jobSkip:Option[String] = None
    @JsonProperty(value="jobFailure", required=false) private var jobFailure:Option[String] = None
    @JsonProperty(value="targetStart", required=false) private var targetStart:Option[String] = None
    @JsonProperty(value="targetFinish", required=false) private var targetFinish:Option[String] = None
    @JsonProperty(value="targetSuccess", required=false) private var targetSuccess:Option[String] = None
    @JsonProperty(value="targetSkip", required=false) private var targetSkip:Option[String] = None
    @JsonProperty(value="targetFailure", required=false) private var targetFailure:Option[String] = None

    override def instantiate(context: Context, properties:Option[Hook.Properties] = None): JdbcHook = {
        new JdbcHook(
            instanceProperties(context, properties),
            connection.instantiate(context),
            jobStart,
            jobFinish,
            jobSuccess,
            jobSkip,
            jobFailure,
            targetStart,
            targetFinish,
            targetSuccess,
            targetSkip,
            targetFailure
        )
    }
}

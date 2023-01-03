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

package com.dimajix.flowman.spec.hook

import java.net.URL

import scala.util.control.NonFatal

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.JobToken
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.execution.TargetToken
import com.dimajix.flowman.execution.Token
import com.dimajix.flowman.model.BaseHook
import com.dimajix.flowman.model.Hook
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.JobDigest
import com.dimajix.flowman.model.JobResult
import com.dimajix.flowman.model.JobResultWrapper
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetDigest
import com.dimajix.flowman.model.TargetResult
import com.dimajix.flowman.model.TargetResultWrapper
import com.dimajix.flowman.spec.hook.WebHook.DummyJobToken
import com.dimajix.flowman.spec.hook.WebHook.DummyTargetToken
import com.dimajix.flowman.common.ConsoleColors.yellow


object WebHook {
    private case class DummyJobToken(env:Map[String,String]) extends JobToken
    private case class DummyTargetToken(env:Map[String,String]) extends TargetToken
}


case class WebHook(
    instanceProperties: Hook.Properties,
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
    private val logger = LoggerFactory.getLogger(classOf[WebHook])


    /**
     * Starts the run and returns a token, which can be anything
     *
     * @param job
     * @return
     */
    override def startJob(execution:Execution, job:Job, instance: JobDigest, parent:Option[Token]): JobToken = {
        val env = job.metadata.asMap ++ instance.asMap + ("status" -> Status.RUNNING.toString) -- context.environment.keys
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
        val env = token.asInstanceOf[DummyJobToken].env + ("result" -> JobResultWrapper(result)) + ("status" -> status)
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
        val parentEnv = parent.collect {
                case t:DummyJobToken => t.env
            }.getOrElse(Map())
        val env = parentEnv ++ target.metadata.asMap ++ instance.asMap + ("status" -> Status.RUNNING.toString) -- context.environment.keys
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
        val env = token.asInstanceOf[DummyTargetToken].env + ("result" -> TargetResultWrapper(result)) + ("status" -> status)
        invoke(targetFinish, env)

        status match {
            case Status.FAILED | Status.ABORTED => invoke(targetFailure, env)
            case Status.SKIPPED  => invoke(targetSkip, env)
            case Status.SUCCESS  => invoke(targetSuccess, env)
            case _ =>
        }
    }

    private def invoke(urlTemplate:Option[String], args:Map[String,AnyRef]) : Unit = {
        urlTemplate.foreach { v =>
            val url = context.environment.evaluate(v, args)
            try {
                val niceUrl = {
                    val u = new URL(url)
                    val result = new StringBuffer()
                    result.append(u.getProtocol)
                    result.append(":")
                    if (u.getAuthority != null && u.getAuthority.nonEmpty) {
                        result.append("//")
                        result.append(u.getAuthority)
                    }

                    if (u.getPath != null) {
                        result.append(u.getPath)
                    }
                    result
                }
                logger.info(s"Invoking external web-hook: $niceUrl with extra args $args")
                val httpClient = HttpClients.createDefault()
                val httpGet = new HttpGet(url)
                httpClient.execute(httpGet)
            }
            catch {
                case NonFatal(ex) => logger.warn(yellow(s"Could not post status to url '$url': ${ex.toString}"))
            }
        }
    }
}


class WebHookSpec extends HookSpec {
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

    override def instantiate(context: Context, properties:Option[Hook.Properties] = None): WebHook = {
        WebHook(
            instanceProperties(context, properties),
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

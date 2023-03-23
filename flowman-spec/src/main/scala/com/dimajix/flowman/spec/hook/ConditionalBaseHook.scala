/*
 * Copyright (C) 2022 The Flowman Authors
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

import scala.util.matching.Regex

import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.JobToken
import com.dimajix.flowman.execution.LifecycleToken
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.execution.TargetToken
import com.dimajix.flowman.execution.Token
import com.dimajix.flowman.model.BaseHook
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.JobDigest
import com.dimajix.flowman.model.JobLifecycle
import com.dimajix.flowman.model.JobResult
import com.dimajix.flowman.model.JobResultWrapper
import com.dimajix.flowman.model.LifecycleResult
import com.dimajix.flowman.model.LifecycleResultWrapper
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetDigest
import com.dimajix.flowman.model.TargetResult
import com.dimajix.flowman.model.TargetResultWrapper
import com.dimajix.flowman.spec.hook.ConditionalBaseHook.DummyJobToken
import com.dimajix.flowman.spec.hook.ConditionalBaseHook.DummyLifecycleToken
import com.dimajix.flowman.spec.hook.ConditionalBaseHook.DummyTargetToken


object ConditionalBaseHook {
    private case class DummyLifecycleToken(vars:Map[String,String], env:Map[String,AnyRef]) extends LifecycleToken
    private case class DummyJobToken(vars:Map[String,String], env:Map[String,AnyRef]) extends JobToken
    private case class DummyTargetToken(vars:Map[String,String], env:Map[String,AnyRef]) extends TargetToken
}


abstract class ConditionalBaseHook(
    condition: Map[String,Seq[Regex]]
) extends BaseHook {
    protected val logger = LoggerFactory.getLogger(getClass)

    /**
     * Starts the run and returns a token, which can be anything
     *
     * @param job
     * @return
     */
    override def startLifecycle(execution: Execution, job:Job, instance:JobLifecycle): LifecycleToken = {
        val vars = job.metadata.asMap ++ instance.asMap + ("category" -> "lifecycle") +  ("status" -> Status.RUNNING.toString)
        val env = vars -- context.environment.keys

        if (matchConditions(vars)) {
            invoke(vars, env)
        }

        DummyLifecycleToken(vars, env)
    }

    /**
     * Sets the status of a job after it has been started
     *
     * @param token The token returned by startJob
     * @param result
     */
    override def finishLifecycle(execution: Execution, token: LifecycleToken, result: LifecycleResult): Unit = {
        val status = result.status
        val vars = token.asInstanceOf[DummyLifecycleToken].vars + ("status" -> status.toString)
        val env = token.asInstanceOf[DummyLifecycleToken].env + ("result" -> LifecycleResultWrapper(result)) + ("status" -> status.toString)

        if (matchConditions(vars)) {
            invoke(vars, env)
        }
    }

    /**
     * Starts the run and returns a token, which can be anything
     *
     * @param job
     * @return
     */
    override def startJob(execution: Execution, job: Job, instance: JobDigest, parent: Option[Token]): JobToken = {
        val (parentVars, parentEnv) = parent.collect {
            case t: DummyLifecycleToken => (t.vars, t.env)
        }.getOrElse((Map.empty[String,String], Map.empty[String,AnyRef]))

        val vars = parentVars ++ job.metadata.asMap ++ instance.asMap + ("status" -> Status.RUNNING.toString)
        val env = parentEnv ++ vars -- context.environment.keys

        if (matchConditions(vars)) {
            invoke(vars, env)
        }

        DummyJobToken(vars, env)
    }

    /**
     * Sets the status of a job after it has been started
     *
     * @param token The token returned by startJob
     * @param result
     */
    override def finishJob(execution: Execution, token: JobToken, result: JobResult): Unit = {
        val status = result.status
        val vars = token.asInstanceOf[DummyJobToken].vars + ("status" -> status.toString)
        val env = token.asInstanceOf[DummyJobToken].env + ("result" -> JobResultWrapper(result)) + ("status" -> status.toString)

        if (matchConditions(vars)) {
            invoke(vars, env)
        }
    }

    /**
     * Starts the run and returns a token, which can be anything
     *
     * @param target
     * @return
     */
    override def startTarget(execution: Execution, target: Target, instance: TargetDigest, parent: Option[Token]): TargetToken = {
        val (parentVars, parentEnv) = parent.collect {
            case t: DummyJobToken => (t.vars, t.env)
        }.getOrElse((Map.empty[String,String], Map.empty[String,AnyRef]))

        val vars = parentVars ++ target.metadata.asMap ++ instance.asMap + ("status" -> Status.RUNNING.toString)
        val env = parentEnv ++ vars -- context.environment.keys

        if (matchConditions(vars)) {
            invoke(vars, env)
        }

        DummyTargetToken(vars, env)
    }

    /**
     * Sets the status of a job after it has been started
     *
     * @param token The token returned by startJob
     * @param result
     */
    override def finishTarget(execution: Execution, token: TargetToken, result: TargetResult): Unit = {
        val status = result.status
        val vars = token.asInstanceOf[DummyTargetToken].vars + ("status" -> status.toString)
        val env = token.asInstanceOf[DummyTargetToken].env + ("result" -> TargetResultWrapper(result)) + ("status" -> status.toString)

        if (matchConditions(vars)) {
            invoke(vars, env)
        }
    }


    protected def invoke(vars:Map[String,String], env:Map[String,AnyRef]) : Unit

    private def matchConditions(vars:Map[String,String]) : Boolean = {
        condition.isEmpty ||
        condition.forall { case(key,values) =>
            vars.get(key).exists(v => values.exists(_.unapplySeq(v).nonEmpty))
        }
    }
}

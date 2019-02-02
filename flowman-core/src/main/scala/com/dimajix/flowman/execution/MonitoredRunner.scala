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

package com.dimajix.flowman.execution

import org.slf4j.LoggerFactory

import com.dimajix.flowman.namespace.monitor.Monitor
import com.dimajix.flowman.spec.task.Job


/**
  * This implementation of the Runner interface provides monitoring via calling appropriate methods in
  * a Monitor
  * @param monitor
  */
class MonitoredRunner(monitor:Monitor) extends AbstractRunner {
    override protected val logger = LoggerFactory.getLogger(classOf[MonitoredRunner])

    /**
      * Performs some check, if the run is required
      * @param context
      * @return
      */
    protected override def check(context:Context, job:Job, args:Map[String,String]) : Boolean = {
        monitor.check(context, job, args)
    }

    /**
      * Starts the run and returns a token, which can be anything
      *
      * @param context
      * @return
      */
    protected override def start(context:Context, job:Job, args:Map[String,String]) : Object = {
        monitor.start(context, job, args)
    }

    /**
      * Marks a run as a success
      *
      * @param context
      * @param token
      */
    protected override def success(context: Context, token:Object) : Unit = {
        monitor.success(context, token)
    }

    /**
      * Marks a run as a failure
      *
      * @param context
      * @param token
      */
    protected override def failure(context: Context, token:Object) : Unit = {
        monitor.failure(context, token)
    }

    /**
      * Marks a run as a failure
      *
      * @param context
      * @param token
      */
    protected override def aborted(context: Context, token:Object) : Unit = {
        monitor.aborted(context, token)
    }

    /**
      * Marks a run as being skipped
      *
      * @param context
      * @param token
      */
    protected override def skipped(context: Context, token:Object) : Unit = {
        monitor.skipped(context, token)
    }
}

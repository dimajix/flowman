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


class MonitoredRunner(monitor:Monitor) extends AbstractRunner {
    override protected val logger = LoggerFactory.getLogger(classOf[MonitoredRunner])

    protected override def check(context:Context, job:Job, args:Map[String,String]) : Boolean = {
        monitor.check(context, job, args)
    }

    protected override def start(context:Context, job:Job, args:Map[String,String]) : Object = {
        monitor.start(context, job, args)
    }

    protected override def success(context: Context, token:Object) : Unit = {
        monitor.success(context, token)
    }

    protected override def failure(context: Context, token:Object) : Unit = {
        monitor.failure(context, token)
    }

    protected override def aborted(context: Context, token:Object) : Unit = {
        monitor.aborted(context, token)
    }

    protected override def skipped(context: Context, token:Object) : Unit = {
        monitor.skipped(context, token)
    }
}

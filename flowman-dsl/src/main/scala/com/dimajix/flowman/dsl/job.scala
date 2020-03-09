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

package com.dimajix.flowman.dsl

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.JobIdentifier
import com.dimajix.flowman.model.Template


case class JobWrapper(
    job:Job.Properties => Job,
    name:String = "",
    labels:Map[String,String] = Map(),
    description:Option[String] = None
) extends Template[Job] {
    def label(kv:(String,String)) : JobWrapper = copy(labels=labels + kv)
    def description(desc:String) : JobWrapper = copy(description=Some(desc))
    def named(name:String) : JobWrapper = copy(name=name)
    def as(name:String) : JobWrapper = named(name)

    def extend(job:String) : JobWrapper = ???
    def extend(job:JobIdentifier) : JobWrapper = ???
    def extend(job:JobWrapper) : JobWrapper = ???

    def identifier : JobIdentifier = ???

    override def instantiate(context: Context): Job = {
        val props = Job.Properties(context, context.namespace, context.project, name, labels, description)
        job(props)
    }
}

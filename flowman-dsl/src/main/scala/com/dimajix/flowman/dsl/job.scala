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
import com.dimajix.flowman.execution.Environment
import com.dimajix.flowman.model
import com.dimajix.flowman.model.JobIdentifier
import com.dimajix.flowman.model.TargetIdentifier


case class Job(
    parameters:Environment => Seq[model.Job.Parameter] = _ => Seq(),
    environment:Environment => Map[String,String] = _ => Map(),
    targets:Environment => Seq[TargetIdentifier] = _ => Seq(),
    extend:Environment => Seq[String] = _ => Seq()
) extends JobGen {
    override def apply(props:model.Job.Properties): model.Job = {
        val env = props.context.environment
        model.Job(
            props
        )
    }
}


case class JobWrapper(
    job:model.Job.Properties => model.Job,
    name:String = "",
    labels:Environment => Map[String,String] = _ => Map(),
    description:Environment => Option[String] = _ => None
) extends Wrapper[model.Job] {
    override def identifier : JobIdentifier = ???

    def label(kv:(String,String)) : JobWrapper = copy(labels=env => labels(env) + kv)
    def description(desc:String) : JobWrapper = copy(description=_ => Some(desc))
    def named(name:String) : JobWrapper = copy(name=name)
    def as(name:String) : JobWrapper = named(name)

    override def instantiate(context: Context): model.Job = {
        val env = context.environment
        val props = model.Job.Properties(context, context.namespace, context.project, name, labels(env), description(env))
        model.Job(props)
    }
}

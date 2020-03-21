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


object Job {
    type Parameter = model.Job.Parameter
}

case class Job(
    parameters:Seq[Job.Parameter] = Seq(),
    environment:Map[String,String] = Map(),
    targets:Seq[TargetIdentifier] = Seq(),
    parents:Seq[JobIdentifier] = Seq()
) extends JobGen {
    override def apply(props:model.Job.Properties): model.Job = {
        val context = props.context
        val parents = this.parents.map(job => context.getJob(job))

        val job = model.Job(
            props,
            parameters,
            environment,
            targets
        )

        model.Job.merge(job, parents)
    }
}


class JobWrapperFunctions(wrapper:Wrapper[model.Job, model.Job.Properties]) {
    def label(kv:(String,String)) : JobWrapper = new JobWrapper {
        override def gen: model.Job.Properties => model.Job = wrapper.gen
        override def props: Context => model.Job.Properties = ctx => {
            val props = wrapper.props(ctx)
            props.copy(labels = props.labels + kv)
        }
    }
    def description(desc:String) : JobWrapper = new JobWrapper {
        override def gen: model.Job.Properties => model.Job = wrapper.gen
        override def props: Context => model.Job.Properties = ctx =>
            wrapper.props(ctx).copy(description = Some(desc))
    }
}

case class JobGenHolder(r:JobGen) extends JobWrapper {
    override def gen: model.Job.Properties => model.Job = r
    override def props: Context => model.Job.Properties = c => model.Job.Properties(c)
}

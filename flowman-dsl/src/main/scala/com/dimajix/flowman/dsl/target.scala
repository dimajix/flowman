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
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetIdentifier


case class TargetWrapper(
    target:Target.Properties => Target,
    name:String = "",
    labels:Map[String,String] = Map(),
    before:Seq[TargetIdentifier] = Seq(),
    after:Seq[TargetIdentifier] = Seq()
) extends Wrapper[Target] {
    override def identifier : TargetIdentifier = ???

    def label(kv:(String,String)) : TargetWrapper = copy(labels=labels + kv)
    def named(name:String) : TargetWrapper = copy(name=name)
    def as(name:String) : TargetWrapper = named(name)

    override def instantiate(context: Context): Target = {
        val props = Target.Properties(context, context.namespace, context.project, name, "", labels, before, after)
        target(props)
    }
}

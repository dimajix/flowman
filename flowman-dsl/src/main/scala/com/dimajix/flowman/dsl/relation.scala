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
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.Template


case class RelationWrapper(
          rel:Relation.Properties => Relation,
          name:String = "",
          labels:Map[String,String] = Map(),
          description:Option[String] = None,
          options:Map[String,String] = Map()
) extends Template[Relation] {
    def label(kv:(String,String)) : RelationWrapper = copy(labels=labels + kv)
    def description(desc:String) : RelationWrapper = copy(description=Some(desc))
    def option(kv:(String,String)): RelationWrapper = copy(options=options + kv)
    def named(name:String) : RelationWrapper = copy(name=name)
    def as(name:String) : RelationWrapper = named(name)
    def identifier : RelationIdentifier = ???

    override def instantiate(context: Context): Relation = {
        val props = Relation.Properties(context, context.namespace, context.project, name, "", labels, description, options)
        rel(props)
    }
}

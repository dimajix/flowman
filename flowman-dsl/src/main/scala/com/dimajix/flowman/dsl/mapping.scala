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

import org.apache.spark.storage.StorageLevel

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.MappingOutputIdentifier


case class MappingWrapper(
    rel:Mapping.Properties => Mapping,
    name:String = "",
    labels:Map[String,String] = Map(),
    broadcast:Boolean = false,
    checkpoint:Boolean = false,
    cache:StorageLevel = StorageLevel.NONE
) extends Wrapper[Mapping] {
    override def identifier : MappingIdentifier = ???

    def label(kv:(String,String)) : MappingWrapper = copy(labels=labels + kv)
    def named(name:String) : MappingWrapper = copy(name=name)
    def as(name:String) : MappingWrapper = named(name)

    def output : MappingOutputIdentifier = output("main")
    def output(name:String) : MappingOutputIdentifier = MappingOutputIdentifier(identifier, name)

    override def instantiate(context: Context): Mapping = {
        val props = Mapping.Properties(context, context.namespace, context.project, name, "", labels, broadcast, checkpoint, cache)
        rel(props)
    }
}

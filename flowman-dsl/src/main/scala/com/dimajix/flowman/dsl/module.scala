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
import com.dimajix.flowman.model.Identifier
import com.dimajix.flowman.model.JobIdentifier
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.model.TargetIdentifier
import com.dimajix.flowman.model.Template


class Project {
    val name = new Field
    val version = new Field
    val description = new Field

    val plugins = new FieldList

    val modules = new ModuleList
}


trait WithWrapper {
    implicit class NamedRelation(name:String) {
        def :=(rel:RelationWrapper) : RelationWrapper = rel as name
        def :=(mapping:MappingWrapper) : MappingWrapper = mapping as name
        def :=(rel:TargetWrapper) : TargetWrapper = rel as name
        def :=(job:JobWrapper) : JobWrapper = job as name
    }
    implicit def wrapRelation(relation:model.Relation.Properties => model.Relation) : RelationWrapper = RelationWrapper(relation)
    implicit def wrapMapping(mapping:model.Mapping.Properties => model.Mapping) : MappingWrapper = MappingWrapper(mapping)
    implicit def wrapTarget(target:model.Target.Properties => model.Target) : TargetWrapper = TargetWrapper(target)
    implicit def wrapJob(job:model.Job.Properties => model.Job) : JobWrapper = JobWrapper(job)

    implicit def toFactory[T](v:T) : Environment => T = _ => v
    implicit def toOption[T](v:T) : Option[T] = Some(v)
    implicit def toOptionFactory[T](v:T) : Environment => Option[T] = _ => Some(v)

    implicit def toTemplate(schema:SchemaGen) : Template[Schema] = new Template[Schema] {
        override def instantiate(context: Context): Schema = schema(Schema.Properties(context))
    }
    implicit def toOptionTemplate(schema:SchemaGen) : Option[Template[Schema]] = Some(toTemplate(schema))

    implicit def toIdentifierList[S,T <: Wrapper[S]](wrappers:WrapperList[S,T]) : Seq[Identifier[S]] = wrappers.identifiers
    implicit def toIdentifierListFactory[S,T <: Wrapper[S]](wrappers:WrapperList[S,T]) : Environment => Seq[Identifier[S]] = _ => wrappers.identifiers

    def relation(name:String) : RelationIdentifier = ???
    def mapping(name:String) : MappingIdentifier = ???
    def output(name:String) : MappingOutputIdentifier = ???
    def target(name:String) : TargetIdentifier = ???
    def job(name:String) : JobIdentifier = ???
}


class Module extends WithWrapper {
    val relations : RelationList = new RelationList()
    val mappings : MappingList = new MappingList()
    val targets : TargetList = new TargetList()
    val jobs : JobList = new JobList()
    val modules : ModuleList = new ModuleList()
    val environment = new FieldMap()
    val config = new FieldMap()

    def instantiate() : model.Module = ???
}


class ModuleList extends Seq[Module] {
    override def length: Int = ???
    override def apply(idx: Int): Module = ???
    override def iterator: Iterator[Module] = ???

    def +=(m:Module*) = ???
}

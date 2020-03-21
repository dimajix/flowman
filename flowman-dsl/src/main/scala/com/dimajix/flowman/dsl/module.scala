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

import scala.collection.SeqLike
import scala.collection.mutable

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Environment
import com.dimajix.flowman.model
import com.dimajix.flowman.model.Identifier
import com.dimajix.flowman.model.Instance
import com.dimajix.flowman.model.JobIdentifier
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.model.TargetIdentifier
import com.dimajix.flowman.model.Template
import com.dimajix.flowman.transforms.schema.Path


class Project {
    val name = new Field
    val version = new Field
    val description = new Field

    val plugins = new FieldList

    val modules = new ModuleList

    def instantiate() : model.Project = {
        val modules = this.modules.map(_.instantiate())

        model.Project(
            name = name.toString,
            version = version.toOption,
            description = description.toOption,

            config = modules.flatMap(_.config.toSeq).toMap,
            environment = modules.flatMap(_.environment.toSeq).toMap,
            profiles = Map(),
            relations = modules.flatMap(_.relations).toMap,
            connections = Map(),
            mappings = modules.flatMap(_.mappings).toMap,
            targets = modules.flatMap(_.targets).toMap,
            jobs = modules.flatMap(_.jobs).toMap
        )
    }
}


trait Converters {
    implicit class NamedRelation(name:String) {
        def :=[T <: Instance,P <: Instance.Properties[P]](rel:Wrapper[T, P]) : NamedWrapper[T, P] = NamedWrapper[T,P](name, rel)
    }
    implicit def wrapRelation(relation:model.Relation.Properties => model.Relation) : RelationWrapper = RelationGenHolder(relation)
    implicit def wrapMapping(mapping:model.Mapping.Properties => model.Mapping) : MappingWrapper = MappingGenHolder(mapping)
    implicit def wrapTarget(target:model.Target.Properties => model.Target) : TargetWrapper = TargetGenHolder(target)
    implicit def wrapJob(job:model.Job.Properties => model.Job) : JobWrapper = JobGenHolder(job)

    implicit def relationFunctions(gen:RelationGen) : RelationWrapperFunctions = new RelationWrapperFunctions(wrapRelation(gen))
    implicit def relationFunctions(wrapper:RelationWrapper) : RelationWrapperFunctions = new RelationWrapperFunctions(wrapper)
    implicit def mappingFunctions(gen:MappingGen) : MappingWrapperFunctions = new MappingWrapperFunctions(wrapMapping(gen))
    implicit def mappingFunctions(wrapper:MappingWrapper) :MappingWrapperFunctions = new MappingWrapperFunctions(wrapper)
    implicit def targetFunctions(gen:TargetGen) : TargetWrapperFunctions = new TargetWrapperFunctions(wrapTarget(gen))
    implicit def targetFunctions(wrapper:TargetWrapper) : TargetWrapperFunctions = new TargetWrapperFunctions(wrapper)

    implicit def toOption[T](v:T) : Option[T] = Some(v)

    implicit def toSeq[T](v:T) : Seq[T] = Seq(v)
    implicit def toMap[K,V](kv:(K,V)) : Map[K,V] = Map(kv)

    implicit def toTemplate(schema:SchemaGen) : Template[Schema] = new Template[Schema] {
        override def instantiate(context: Context): Schema = schema(Schema.Properties(context))
    }
    implicit def toOptionTemplate(schema:SchemaGen) : Option[Template[Schema]] = Some(toTemplate(schema))

    implicit def toIdentifierList[S <: Instance,P <: Instance.Properties[P]](wrappers:WrapperList[S,P]) : Seq[Identifier[S]] = wrappers.identifiers
}


trait ContextAware {
    def withContext[S <: Instance,P <: Instance.Properties[P]](fn:Context => Wrapper[S, P]): Wrapper[S, P] = new Wrapper[S, P] {
        override def gen: P => S = p => fn(p.context).gen(p)
        override def props: Context => P = c => fn(c).props(c)
    }

    def withEnvironment[S <: Instance,P <: Instance.Properties[P]](fn:Environment => Wrapper[S, P]): Wrapper[S, P] = new Wrapper[S, P] {
        override def gen: P => S = p => fn(p.context.environment).gen(p)
        override def props: Context => P = c => fn(c.environment).props(c)
    }
}


trait Functions {
    def path(str:String) : Path = Path(str)
}


trait Identifiers {
    /**
     * Returns a RelationIdentifier for the specified relation of the current project
     * @param name
     * @return
     */
    def relation(name:String) : RelationIdentifier = RelationIdentifier(name, None)

    /**
     * Returns a MappingIdentifier for the specified mapping of the current project
     * @param name
     * @return
     */
    def mapping(name:String) : MappingIdentifier = MappingIdentifier(name, None)

    /**
     * Returns a MappingOutputIdentifier for the specified mapping output  of the current project
     * @param name
     * @return
     */
    def output(name:String, output:String="main") : MappingOutputIdentifier = MappingOutputIdentifier(name, output, None)

    /**
     * Returns a TargetIdentifier for the specified build target of the current project
     * @param name
     * @return
     */
    def target(name:String) : TargetIdentifier = TargetIdentifier(name, None)

    /**
     * Returns a JobIdentifier for the specified job of the current project
     * @param name
     * @return
     */
    def job(name:String) : JobIdentifier = JobIdentifier(name, None)
}


class Module extends Converters with Identifiers with Functions with ContextAware {
    val relations : RelationList = new RelationList()
    val mappings : MappingList = new MappingList()
    val targets : TargetList = new TargetList()
    val jobs : JobList = new JobList()
    val modules : ModuleList = new ModuleList()
    val environment = new FieldMap()
    val config = new FieldMap()

    def instantiate() : model.Module = {
        val modules = this.modules.map(_.instantiate())
        model.Module(
            config = modules.flatMap(_.config.toSeq).toMap ++ config.toMap,
            environment = modules.flatMap(_.environment.toSeq).toMap ++ environment.toMap,
            profiles = Map(),
            relations = modules.flatMap(_.relations.toSeq).toMap ++ relations.toMap,
            connections = Map(),
            mappings = modules.flatMap(_.mappings.toSeq).toMap ++ mappings.toMap,
            targets = modules.flatMap(_.targets.toSeq).toMap ++ targets.toMap,
            jobs = modules.flatMap(_.jobs.toSeq).toMap ++ jobs.toMap
        )
    }
}


class ModuleList(private var modules:Seq[Module] = Seq()) extends Seq[Module] {
    def +=(m:Module*) : Unit = this.modules = this.modules ++ m

    def relations : RelationList = new RelationList(modules.flatMap(m => m.relations ++ m.modules.relations))
    def mappings : MappingList = new MappingList(modules.flatMap(m => m.mappings ++ m.modules.mappings))
    def targets : TargetList = new TargetList(modules.flatMap(m => m.targets ++ m.modules.targets))
    def jobs : JobList = new JobList(modules.flatMap(m => m.jobs ++ m.modules.jobs))
    def environment : Map[String,String] = modules.flatMap(m => m.environment.toSeq ++ m.modules.environment.toSeq).toMap
    def config : Map[String,String] = modules.flatMap(m => m.config.toSeq ++ m.modules.config.toSeq).toMap

    override def length: Int = modules.length
    override def apply(idx: Int): Module = modules(idx)
    override def iterator: Iterator[Module] = modules.iterator
}

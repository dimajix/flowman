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

import com.dimajix.flowman.model
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.Relation


class Project {
    val name = new Field
    val version = new Field
    val description = new Field

    val plugins = new FieldList

    val modules = new ModuleList
}


class Module {
    implicit class NamedRelation(name:String) {
        //def :=(rel:Relation.Properties => Relation) : RelationWrapper = RelationWrapper(rel)
        def :=(rel:RelationWrapper) : RelationWrapper = rel as name
    }
    implicit def wrapRelation(_rel:Relation.Properties => Relation) : RelationWrapper = RelationWrapper(_rel)
    implicit def wrapJob(_job:Job.Properties => Job) : JobWrapper = JobWrapper(_job)

    def relation(name:String) : RelationWrapper = ???
    val relations : RelationList = ???
    val jobs : JobList = ???
    val modules : ModuleList = ???

    def instantiate() : model.Module = ???
}


class ModuleList extends Seq[Module] {
    override def length: Int = ???
    override def apply(idx: Int): Module = ???
    override def iterator: Iterator[Module] = ???

    def +=(m:Module*) = ???
}

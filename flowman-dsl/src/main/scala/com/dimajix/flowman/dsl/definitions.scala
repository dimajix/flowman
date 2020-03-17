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
import com.dimajix.flowman.model.Identifier
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.Template


class Field {
    private var name:String = _

    def :=(value:String) : Unit = ???
}
class FieldList {
    private var values:Seq[String] = Seq()

    def :=(values:String*) : Unit = ???
    def +=(values:String*) : Unit = ???

    def toSeq : Seq[String] = ???
}
class FieldMap {
    def :=(values:(String,String)*) : Unit = ???
    def +=(values:(String,String)*) : Unit = ???

    def toMap : Map[String,String] = ???
}


class WrapperList[S,T <: Wrapper[S]] extends Map[String,T] {
    override def +[B1 >: T](kv: (String, B1)): Map[String, B1] = ???
    override def get(key: String): Option[T] = ???
    override def iterator: Iterator[(String, T)] = ???
    override def -(key: String): Map[String, T] = ???

    def :=(seq: T*) = ???
    def +=(rel: T) = ???
    def +=(seq: T*) = ???
    def +=(fn: => Unit) = ???

    def identifiers : Seq[Identifier[S]] = map { case(k,v) => v.identifier }.toSeq
}


trait Wrapper[T] extends Template[T] {
    def identifier : Identifier[T] = ???
}


trait TargetGen extends (model.Target.Properties => model.Target)
trait RelationGen extends (model.Relation.Properties => model.Relation)
trait MappingGen extends (model.Mapping.Properties => model.Mapping)
trait JobGen extends (model.Job.Properties => model.Job)

trait SchemaGen extends (model.Schema.Properties => model.Schema)
trait DatasetGen extends (model.Dataset.Properties => model.Dataset)

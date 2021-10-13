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
import com.dimajix.flowman.model.Identifier
import com.dimajix.flowman.model.Instance
import com.dimajix.flowman.model.Prototype


class Field {
    private var value:Option[String] = None

    override def toString: String = value.getOrElse("")

    def :=(value:String) : Unit = this.value = Some(value)

    def toOption : Option[String] = value
}
class FieldList {
    private var values:Seq[String] = Seq()

    override def toString: String = values.toString()

    def :=(values:String*) : Unit = this.values = values
    def +=(values:String*) : Unit = this.values = this.values ++ values

    def toSeq : Seq[String] = values
}
class FieldMap {
    private var values:Map[String,String] = Map()

    def :=(values:(String,String)*) : Unit = this.values = values.toMap
    def +=(values:(String,String)*) : Unit = this.values = this.values ++ values.toMap

    def toSeq : Seq[(String,String)] = values.toSeq
    def toMap : Map[String,String] = values
}



trait Wrapper[T <: Instance, P <: Instance.Properties[P]] {
    def gen:P => T
    def props:Context => P
}

case class NamedWrapper[T <: Instance, P <: Instance.Properties[P]](name:String, wrapper:Wrapper[T,P]) extends Prototype[T] {
    def identifier : Identifier[T] = Identifier[T](name, None)

    override def instantiate(context: Context): T = {
        val props = wrapper.props(context).withName(name)
        wrapper.gen(props)
    }
}


final class WrapperList[S <: Instance,P <: Instance.Properties[P]](private var wrappers : Seq[NamedWrapper[S,P]] = Seq())
    extends Seq[NamedWrapper[S,P]] {
    override def length: Int = wrappers.length
    override def apply(idx: Int): NamedWrapper[S, P] = wrappers(idx)
    override def iterator: Iterator[NamedWrapper[S, P]] = wrappers.iterator

    def :=(seq: NamedWrapper[S,P]*) : Unit = wrappers = seq
    def +=(seq: NamedWrapper[S,P]) : Unit = wrappers = wrappers :+ seq
    def +=(seq: NamedWrapper[S,P]*) : Unit = wrappers = wrappers ++ seq

    def identifiers : Seq[Identifier[S]] = wrappers.map(_.identifier)

    def toMap : Map[String,NamedWrapper[S,P]] = wrappers.map(w => w.name -> w).toMap
}

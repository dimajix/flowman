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


class Field {
    private var name:String = _

    def :=(value:String) : Unit = ???
}
class FieldList {
    private var values:Seq[String] = Seq()

    def :=(values:String*) : Unit = ???
    def +=(values:String*) : Unit = ???
}


class WrapperList[T] extends Map[String,T] {
    override def +[B1 >: T](kv: (String, B1)): Map[String, B1] = ???
    override def get(key: String): Option[T] = ???
    override def iterator: Iterator[(String, T)] = ???
    override def -(key: String): Map[String, T] = ???

    def :=(seq: Seq[T]) = ???
    def +=(rel: T) = ???
    def +=(seq: Seq[T]) = ???
    def +=(fn: => Unit) = ???
}

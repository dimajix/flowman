/*
 * Copyright (C) 2018 The Flowman Authors
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

package com.dimajix.common

import java.util.Locale

import scala.collection.Iterator
import scala.collection.Map
import scala.collection.Seq
import scala.collection.SetLike


object SetIgnoreCase {
    def apply() : SetIgnoreCase = new SetIgnoreCase(Map.empty)
    def apply(set:Iterable[String]) : SetIgnoreCase = {
        new SetIgnoreCase(set.map(kv => kv.toLowerCase(Locale.ROOT) -> kv).toMap)
    }
    def apply(head:String) : SetIgnoreCase = {
        apply(Seq(head))
    }
    def apply(head:String, tail:String*) : SetIgnoreCase = {
        apply(head +: tail.toSeq)
    }

}
class SetIgnoreCase private(impl:Map[String,String] = Map()) extends Set[String] with SetLike[String, SetIgnoreCase] {
    override def empty: SetIgnoreCase = SetIgnoreCase(Set.empty)

    /** Creates a new iterator over all key/value pairs of this map
      *
      *  @return the new iterator
      */
    override def iterator: Iterator[String] = impl.valuesIterator

    override def + (v: String): SetIgnoreCase = new SetIgnoreCase(impl + (v.toLowerCase(Locale.ROOT) -> v))

    /** Removes a key from this map, returning a new map.
      *  @param    key the key to be removed
      *  @return   a new map without a binding for `key`
      *
      *  @usecase  def - (key: A): Map[A, B]
      *    @inheritdoc
      */
    override def - (key: String): SetIgnoreCase = new SetIgnoreCase(impl - key.toLowerCase(Locale.ROOT))

    override def contains(elem: String): Boolean = impl.contains(elem.toLowerCase(Locale.ROOT))

    def get(v: String) : Option[String] = impl.get(v.toLowerCase(Locale.ROOT))
}

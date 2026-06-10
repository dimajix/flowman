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
import scala.collection.Seq
import scala.collection.immutable.Map
import scala.collection.immutable.Set
import scala.collection.immutable.MapOps
import scala.collection.mutable.Builder


object MapIgnoreCase {
    def apply[T]() : MapIgnoreCase[T] = new MapIgnoreCase[T](Map.empty)
    def apply[T](map:Map[String,T]) : MapIgnoreCase[T] = {
        new MapIgnoreCase[T](map.map(kv => kv._1.toLowerCase(Locale.ROOT) -> ((kv._1, kv._2))).toMap)
    }

    def apply[T](seq:Seq[(String,T)]) : MapIgnoreCase[T] = {
        new MapIgnoreCase[T](seq.map(kv => kv._1.toLowerCase(Locale.ROOT) -> ((kv._1, kv._2))).toMap)
    }
    def apply[T](head:(String,T)) : MapIgnoreCase[T] = {
        apply(Seq(head))
    }
    def apply[T](head:(String,T), tail:(String,T)*) : MapIgnoreCase[T] = {
        apply(head +: tail.toSeq)
    }
}


class MapIgnoreCase[B] private(impl:Map[String,(String,B)] = Map()) extends Map[String,B] with MapOps[String,B,Map,MapIgnoreCase[B]] {
    override def empty: MapIgnoreCase[B] = MapIgnoreCase[B]()

    override def get(key: String): Option[B] = {
        impl.get(key.toLowerCase(Locale.ROOT)).map(_._2)
    }

    /**
      * This returns the original key in its original spelling together with the value
      * @param key
      * @return
      */
    def getKeyValue(key: String): (String,B) = {
        impl(key.toLowerCase(Locale.ROOT))
    }

    /** Creates a new iterator over all key/value pairs of this map
      *
      *  @return the new iterator
      */
    override def iterator: Iterator[(String, B)] = impl.valuesIterator

    override def keySet: scala.collection.immutable.Set[String] = impl.keySet

    override protected def fromSpecific(coll: IterableOnce[(String,B)]): MapIgnoreCase[B] = coll.foldLeft(MapIgnoreCase[B]())((m,kv) => m.updated(kv._1, kv._2))
    override protected def newSpecificBuilder: Builder[(String,B), MapIgnoreCase[B]] =
        new Builder[(String,B), MapIgnoreCase[B]] {
            var data: scala.collection.mutable.ListBuffer[(String,B)] = scala.collection.mutable.ListBuffer()
            def clear(): Unit = data.clear()
            def result(): MapIgnoreCase[B] = MapIgnoreCase(data)
            def addOne(v: (String,B)): this.type = {
                data.addOne(v)
                this
            }
        }

    override def keysIterator : Iterator[String] = impl.keysIterator

    /** Adds a key/value pair to this map, returning a new map.
      *  @param    kv the key/value pair
      *  @tparam   B1 the type of the value in the key/value pair.
      *  @return   a new map with the new binding added to this map
      *
      *  @usecase  def + (kv: (String, B)): Map[A, B]
      *    @inheritdoc
      */
    def updated[B1 >: B] (key: String, value: B1): MapIgnoreCase[B1] = new MapIgnoreCase[B1](impl + (key.toLowerCase(Locale.ROOT) -> ((key, value))))

    /** Removes a key from this map, returning a new map.
      *  @param    key the key to be removed
      *  @return   a new map without a binding for `key`
      *
      *  @usecase  def - (key: A): Map[A, B]
      *    @inheritdoc
      */
    def removed(key: String): MapIgnoreCase[B] = new MapIgnoreCase[B](impl - key.toLowerCase(Locale.ROOT))
}

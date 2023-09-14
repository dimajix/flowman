/*
 * Copyright (C) 2021 The Flowman Authors
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

import scala.collection.mutable


object SynchronizedMap {
    def apply[K,V]() : SynchronizedMap[K,V] = SynchronizedMap(mutable.Map[K,V]())
}

/**
 * This is a thin wrapper around a [[Map]] which provides synchronized Access.
 * @param impl
 * @tparam A
 */
case class SynchronizedMap[K,V](impl:mutable.Map[K,V]) {

    /** Tests whether this map contains a binding for a key.
     *
     *  @param key the key
     *  @return    `true` if there is a binding for `key` in this map, `false` otherwise.
     */
    def contains(key: K): Boolean = {
        synchronized {
            impl.contains(key)
        }
    }

    /** Optionally returns the value associated with a key.
     *
     *  @param  key    the key value
     *  @return an option value containing the value associated with `key` in this map,
     *          or `None` if none exists.
     */
    def get(key: K): Option[V] = {
        synchronized {
            impl.get(key)
        }
    }

    /**  Returns the value associated with a key, or a default value if the key is not contained in the map.
     *   @param   key      the key.
     *   @param   default  a computation that yields a default value in case no binding for `key` is
     *                     found in the map.
     *   @tparam  V1       the result type of the default computation.
     *   @return  the value associated with `key` if it exists,
     *            otherwise the result of the `default` computation.
     *
     *   @usecase def getOrElse(key: K, default: => V): V
     *     @inheritdoc
     */
    def getOrElse[V1 >: V](key: K, default: => V1): V1 = {
        synchronized {
            impl.get(key)
        }
        match {
            case Some(result) => result
            case None => default
        }
    }

    /** Adds a new key/value pair to this map and optionally returns previously bound value.
     *  If the map already contains a
     *  mapping for the key, it will be overridden by the new value.
     *
     * @param key    the key to update
     * @param value  the new value
     * @return an option value containing the value associated with the key
     *         before the `put` operation was executed, or `None` if `key`
     *         was not defined in the map before.
     */
    def put(key: K, value: V) : Unit = {
        synchronized {
            impl.put(key, value)
        }
    }

    /**
     * Remove a value from the map
     * @param key
     */
    def remove(key: K) : Unit = {
        synchronized {
            impl.remove(key)
        }
    }

    /** Retrieves the value which is associated with the given key. This
     *  method invokes the `default` method of the map if there is no mapping
     *  from the given key to a value. Unless overridden, the `default` method throws a
     *  `NoSuchElementException`.
     *
     *  @param  key the key
     *  @return     the value associated with the given key, or the result of the
     *              map's `default` method, if none exists.
     */
    def apply(key: K) : V = {
        synchronized {
            impl(key)
        }
    }

    /** If given key is already in this map, returns associated value.
     *
     *  Otherwise, computes value from given expression `op`, stores with key
     *  in map and returns that value.
     *
     *  @param  key the key to test
     *  @param  op  the computation yielding the value to associate with `key`, if
     *              `key` is previously unbound.
     *  @return     the value associated with key (either previously or as a result
     *              of executing the method).
     */
    def getOrElseUpdate(key: K, op: => V): V = {
        synchronized(
            impl.get(key)
        )
        match {
            case Some(result) => result
            case None =>
                val result = op
                synchronized(impl.getOrElseUpdate(key, result))
        }
    }

    /** Converts this $coll to a sequence.
     *
     * ```Note```: assumes a fast `size` method.  Subclasses should override if this is not true.
     */
    def toSeq : collection.Seq[(K,V)] = {
        synchronized {
            impl.toSeq
        }
    }

    /** Creates a new iterator over all elements contained in this iterable object.
     *
     *  @return the new iterator
     */
    def iterator : Iterator[(K,V)] = {
        toSeq.iterator
    }

    /** Collects all keys of this map in an Set.
     *
     *  @return the keys of this map as a Set.
     */
    def keys : Set[K] = {
        synchronized {
            impl.keySet.toSet
        }
    }

    /** Collects all values of this map in an iterable collection.
     *
     *  @return the values of this map as a Sequence.
     */
    def values: Seq[V] = {
        synchronized {
            Seq(impl.values.toSeq:_*)
        }
    }

    /** Applies a function `f` to all values produced by this iterator.
     *
     *  @param  f   the function that is applied for its side-effect to every element.
     *              The result of function `f` is discarded.
     *
     *  @tparam  U  the type parameter describing the result of function `f`.
     *              This result will always be ignored. Typically `U` is `Unit`,
     *              but this is not necessary.
     *
     *  @usecase def foreach(f: A => Unit): Unit
     *    @inheritdoc
     */
    def foreach[U](f: ((K,V)) => U) : Unit = {
        iterator.foreach(f)
    }

    /** Removes all bindings from the map. After this operation has completed,
     *  the map will be empty.
     */
    def clear() : Unit = {
        synchronized {
            impl.clear()
        }
    }
}

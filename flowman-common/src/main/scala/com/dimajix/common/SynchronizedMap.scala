/*
 * Copyright 2021 Kaya Kupferschmidt
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

import scala.collection.Iterable
import scala.collection.mutable


object SynchronizedMap {
    def apply[K,V]() : SynchronizedMap[K,V] = SynchronizedMap(mutable.Map[K,V]())
}
case class SynchronizedMap[K,V](impl:mutable.Map[K,V]) {
    def contains(key: K): Boolean = {
        synchronized {
            impl.contains(key)
        }
    }
    def get(key: K): Option[V] = {
        synchronized {
            impl.get(key)
        }
    }
    def getOrElse[V1 >: V](key: K, default: => V1): V1 = {
        synchronized {
            impl.get(key)
        }
        match {
            case Some(result) => result
            case None => default
        }
    }
    def put(key: K, value: V) : Unit = {
        synchronized {
            impl.put(key, value)
        }
    }
    def apply(key: K) : V = {
        synchronized {
            impl(key)
        }
    }

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

    def toSeq : collection.Seq[(K,V)] = {
        synchronized {
            impl.toSeq
        }
    }

    def iterator : Iterator[(K,V)] = {
        toSeq.iterator
    }

    def values: Iterable[V] = {
        synchronized {
            Seq(impl.values.toSeq:_*)
        }
    }

    def foreach[U](f: ((K,V)) => U) : Unit = {
        iterator.foreach(f)
    }

    def clear() : Unit = {
        synchronized {
            impl.clear()
        }
    }

}

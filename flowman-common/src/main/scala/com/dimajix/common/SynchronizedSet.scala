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

import scala.collection.Iterator
import scala.collection.mutable


object SynchronizedSet {
    def apply[A]() : SynchronizedSet[A] = SynchronizedSet(mutable.Set[A]())
}
case class SynchronizedSet[A](impl:mutable.Set[A]) {
    def add(elem: A): Boolean = {
        impl.synchronized(impl.add(elem))
    }

    def remove(elem: A): Boolean = {
        impl.synchronized(impl.remove(elem))
    }

    def contains(elem: A): Boolean = {
        impl.synchronized(impl.contains(elem))
    }

    def find(p: A => Boolean): Option[A] = {
        impl.synchronized(impl.find(p))
    }

    def toSeq : collection.Seq[A] = {
        impl.synchronized(impl.toSeq)
    }

    def iterator : Iterator[A] = toSeq.iterator

    def foreach[U](f: A => U) : Unit = {
        toSeq.foreach(f)
    }

    def clear() : Unit = {
        impl.synchronized(impl.clear())
    }
}

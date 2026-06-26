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

import scala.collection.mutable
import scala.collection.mutable.Map
import scala.jdk.CollectionConverters._


class IdentityHashMap[A, B] private(underlying: java.util.IdentityHashMap[A, B]) extends Map[A, B] {
    def this() = this(new java.util.IdentityHashMap[A, B]())

    def updated(key: A, value: B): IdentityHashMap[A, B] = {
        val result = new IdentityHashMap[A, B](new java.util.IdentityHashMap[A, B](underlying))
        result.put(key, value)
        result
    }

    def removed(key: A): IdentityHashMap[A, B] = {
        val result = new IdentityHashMap[A, B](new java.util.IdentityHashMap[A, B](underlying))
        result.remove(key)
        result
    }

    override def get(key: A): Option[B] = {
        val value = underlying.get(key)
        if (value == null) None else Some(value)
    }

    override def iterator: Iterator[(A, B)] = underlying.asScala.iterator

    override def addOne(kv: (A, B)): this.type = {
        underlying.put(kv._1, kv._2)
        this
    }

    override def subtractOne(key: A): this.type = {
        underlying.remove(key)
        this
    }

    override def clear(): Unit = underlying.clear()

    override def size: Int = underlying.size()

    override def put(key: A, value: B): Option[B] = {
        val result = underlying.get(key)
        underlying.put(key, value)
        if (result == null) None else Some(result)
    }

    override def remove(key: A): Option[B] = {
        val result = underlying.get(key)
        underlying.remove(key)
        if (result == null) None else Some(result)
    }

    override def clone(): IdentityHashMap[A, B] = new IdentityHashMap[A, B](new java.util.IdentityHashMap[A, B](underlying))
}


object IdentityHashMap {
    def empty[A, B]: IdentityHashMap[A, B] = new IdentityHashMap[A, B]()

    def apply[A, B](): IdentityHashMap[A, B] = empty[A, B]
}

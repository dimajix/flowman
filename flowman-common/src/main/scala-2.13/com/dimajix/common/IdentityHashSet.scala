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

import java.util
import java.util.Collections

import scala.collection.mutable
import scala.collection.mutable.Set
import scala.jdk.CollectionConverters._


class IdentityHashSet[A] private(underlying: java.util.Set[A]) extends Set[A] {
    def this() = this(Collections.newSetFromMap(new util.IdentityHashMap[A, java.lang.Boolean]()))
    
    def incl(elem: A): IdentityHashSet[A] = {
        val result = new IdentityHashSet[A](new util.HashSet[A](underlying))
        result.addOne(elem)
        result
    }
    
    def excl(elem: A): IdentityHashSet[A] = {
        val result = new IdentityHashSet[A](new util.HashSet[A](underlying))
        result.subtractOne(elem)
        result
    }
    
    override def contains(elem: A): Boolean = underlying.contains(elem)
    
    override def iterator: Iterator[A] = underlying.asScala.iterator
    
    override def addOne(elem: A): this.type = {
        underlying.add(elem)
        this
    }
    
    override def subtractOne(elem: A): this.type = {
        underlying.remove(elem)
        this
    }
    
    override def clear(): Unit = underlying.clear()
    
    override def size: Int = underlying.size()
    
    def diff(that: Set[A]): Set[A] = {
        val result = new IdentityHashSet[A](new util.HashSet[A](underlying))
        that.foreach(result.subtractOne)
        result
    }
    
    override def clone(): IdentityHashSet[A] = new IdentityHashSet[A](new util.HashSet[A](underlying))
}


object IdentityHashSet {
    def empty[A]: IdentityHashSet[A] = new IdentityHashSet[A]()
    
    def apply[A](): IdentityHashSet[A] = empty[A]
}

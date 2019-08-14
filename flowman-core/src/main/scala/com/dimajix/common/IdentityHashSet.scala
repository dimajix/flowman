/*
 * Copyright 2018-2019 Kaya Kupferschmidt
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

import scala.collection.convert.Wrappers.JSetWrapper
import scala.collection.generic.MutableSetFactory
import scala.collection.mutable


class IdentityHashSet[A] extends JSetWrapper[A](Collections.newSetFromMap(new util.IdentityHashMap()))
    with mutable.SetLike[A, IdentityHashSet[A]]
{
    override def empty = new IdentityHashSet[A]
}

object IdentityHashSet extends MutableSetFactory[IdentityHashSet] {
    override def empty[A]: IdentityHashSet[A] = new IdentityHashSet[A]
}

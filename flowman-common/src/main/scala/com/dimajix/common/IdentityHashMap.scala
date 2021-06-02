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

import scala.collection.generic._
import scala.collection.convert.Wrappers._


class IdentityHashMap[A, B] extends JMapWrapper[A, B](new java.util.IdentityHashMap)
    with JMapWrapperLike[A, B, IdentityHashMap[A, B]] {
    override def empty = new IdentityHashMap[A, B]
}


object IdentityHashMap extends MutableMapFactory[IdentityHashMap] {
    implicit def canBuildFrom[A, B]: CanBuildFrom[Coll, (A, B), IdentityHashMap[A, B]] =
        new MapCanBuildFrom[A, B]

    def empty[A, B]: IdentityHashMap[A, B] = new IdentityHashMap[A, B]
}

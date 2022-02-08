/*
 * Copyright 2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.documentation


/**
 * A [[Fragment]] represents a piece of documentation. The full documentation then is a tree structure build from many
 * fragments.
 */
abstract class Fragment {
    /**
     * Optional textual description of the fragment to be shown in the documentation
     * @return
     */
    def description : Option[String]

    /**
     * A resolvable reference to the fragment itself
     * @return
     */
    def reference : Reference

    /**
     * A reference to the parent of this [[Fragment]]
     * @return
     */
    def parent : Option[Reference]

    /**
     * List of child fragments
     * @return
     */
    def fragments : Seq[Fragment]

    def reparent(parent:Reference) : Fragment

    def resolve(path:Seq[Reference]) : Option[Fragment] = {
        path match {
            case head :: tail =>
                fragments.find(_.reference == head).flatMap(_.resolve(tail))
            case Nil => Some(this)
        }
    }
}

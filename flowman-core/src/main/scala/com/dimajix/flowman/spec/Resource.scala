/*
 * Copyright 2018 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec

import com.dimajix.flowman.execution.Context


abstract class Resource {
    /**
      * Returns the name of the resource
      * @return
      */
    def name : String

    /**
      * Returns the kind of the resource
      * @return
      */
    def kind : String

    /**
      * Returns the category of the resource
      * @return
      */
    def category : String

    /**
      * Returns the map of generic meta data labels
      * @param context
      * @return
      */
    def labels(implicit context:Context) : Map[String,String]

    /**
      * Returns the full set of meta data of this resource
      * @param context
      * @return
      */
    def metadata(implicit context:Context) : Metadata = {
        Metadata(
            Option(context.namespace).map(_.name),
            Option(context.project).map(_.name),
            name,
            Option(context.project).map(_.version),
            category,
            kind,
            labels
        )
    }
}

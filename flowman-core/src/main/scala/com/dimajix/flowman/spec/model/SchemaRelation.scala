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

package com.dimajix.flowman.spec.model

import com.fasterxml.jackson.annotation.JsonProperty

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.spec.schema.Schema


/**
  * Common base implementation for the Relation interface class. It contains a couple of common properties.
  */
trait SchemaRelation { this:Relation =>
    @JsonProperty(value="schema", required=false) private var _schema: Schema = _

    /**
      * Returns the schema of the relation
      * @param context
      * @return
      */
    override def schema(implicit context: Context) : Schema = _schema
}

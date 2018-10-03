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

package com.dimajix.flowman.spec.schema

import org.apache.spark.sql.types.DataType

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.SparkSchemaUtils


class SparkSchema extends ExternalSchema {
    /**
      * Returns the description of the schema
      * @param context
      * @return
      */
    protected override def loadDescription(implicit context: Context): String = {
        ""
    }

    /**
      * Returns the list of all fields of the schema
      * @param context
      * @return
      */
    protected override def loadFields(implicit context: Context): Seq[Field] = {
        SparkSchemaUtils.fromSpark(sprkSchema)
    }

    /**
      * Load and cache Spark schema from external source
      * @param context
      * @return
      */
    private def sprkSchema(implicit context: Context) : org.apache.spark.sql.types.StructType = {
        if (cachedSparkSchema == null) {
            val json = loadSchemaSpec(context)
            cachedSparkSchema = DataType.fromJson(json).asInstanceOf[org.apache.spark.sql.types.StructType]
        }
        cachedSparkSchema
    }
    private var cachedSparkSchema : org.apache.spark.sql.types.StructType = _
}

/*
 * Copyright 2018-2021 Kaya Kupferschmidt
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

import java.net.URL

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.DataType
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.spec.schema.ExternalSchema.CachedSchema
import com.dimajix.flowman.types.Field


case class SparkSchema(
    instanceProperties:Schema.Properties,
    override val file: Option[Path],
    override val url: Option[URL],
    override val spec: Option[String]
) extends ExternalSchema {
    protected override val logger = LoggerFactory.getLogger(classOf[SparkSchema])

    /**
      * Returns the list of all fields of the schema
      * @return
      */
    protected override def loadSchema: CachedSchema = {
        val json = loadSchemaSpec
        val sparkSchema = DataType.fromJson(json).asInstanceOf[org.apache.spark.sql.types.StructType]

        CachedSchema(
            Field.of(sparkSchema),
            None
        )
    }
}



class SparkSchemaSpec extends ExternalSchemaSpec {
    /**
      * Creates the instance of the specified Schema with all variable interpolation being performed
      * @param context
      * @return
      */
    override def instantiate(context: Context): SparkSchema = {
        SparkSchema(
            Schema.Properties(context),
            file.map(context.evaluate).filter(_.nonEmpty).map(p => new Path(p)),
            url.map(context.evaluate).filter(_.nonEmpty).map(u => new URL(u)),
            context.evaluate(spec)
        )
    }
}

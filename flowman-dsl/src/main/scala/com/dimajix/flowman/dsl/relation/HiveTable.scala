/*
 * Copyright 2018-2020 Kaya Kupferschmidt
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

package com.dimajix.flowman.dsl.relation

import org.apache.hadoop.fs.Path

import com.dimajix.flowman.dsl.RelationGen
import com.dimajix.flowman.model.PartitionField
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.model.Template
import com.dimajix.flowman.spec.relation.HiveTableRelation


case class HiveTable(
    database: Option[String] = None,
    schema:Option[Template[Schema]] = None,
    partitions: Seq[PartitionField] = Seq(),
    table: String,
    external: Boolean = false,
    location: Option[Path] = None,
    format: String = "parquet",
    rowFormat: Option[String] = None,
    inputFormat: Option[String] = None,
    outputFormat: Option[String] = None,
    properties: Map[String, String] = Map(),
    serdeProperties: Map[String, String] = Map(),
    writer: String = "hive"
) extends RelationGen {
    override def apply(props:Relation.Properties) : HiveTableRelation = {
        HiveTableRelation(
            props,
            database = database,
            schema = schema.map(s => s.instantiate(props.context)),
            table = table,
            external = external,
            location = location,
            format = format,
            rowFormat = rowFormat,
            inputFormat = inputFormat,
            outputFormat = outputFormat,
            properties = properties,
            serdeProperties = serdeProperties,
            writer = writer
        )
    }
}

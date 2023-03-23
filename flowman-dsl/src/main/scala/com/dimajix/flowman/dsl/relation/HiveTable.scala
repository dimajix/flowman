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

package com.dimajix.flowman.dsl.relation

import org.apache.hadoop.fs.Path

import com.dimajix.flowman.catalog.TableIdentifier
import com.dimajix.flowman.dsl.RelationGen
import com.dimajix.flowman.model.PartitionField
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.model.Prototype
import com.dimajix.flowman.spec.relation.HiveTableRelation


case class HiveTable(
    database: Option[String] = None,
    schema:Option[Prototype[Schema]] = None,
    partitions: Seq[PartitionField] = Seq(),
    table: String,
    external: Boolean = false,
    location: Option[Path] = None,
    format: Option[String] = None,
    options: Map[String,String] = Map(),
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
            schema = schema.map(s => s.instantiate(props.context)),
            partitions = partitions,
            table = TableIdentifier(table, database.toSeq),
            external = external,
            location = location,
            format = format,
            options = options,
            rowFormat = rowFormat,
            inputFormat = inputFormat,
            outputFormat = outputFormat,
            properties = properties,
            serdeProperties = serdeProperties,
            writer = writer
        )
    }
}

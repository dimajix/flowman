/*
 * Copyright 2018-2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.catalog

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable

import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.StructType


object TableDefinition {
    def ofTable(table:CatalogTable) : TableDefinition = {
        val sourceSchema = com.dimajix.flowman.types.StructType.of(table.dataSchema)
        TableDefinition(table.identifier, sourceSchema.fields)
    }
}
case class TableDefinition(
    identifier: TableIdentifier,
    fields: Seq[Field],
    comment: Option[String] = None,
    primaryKey: Seq[String] = Seq(),
    indexes: Seq[TableIndex] = Seq()
) {
    def schema : StructType = StructType(fields)
}


case class TableIndex(
    name: String,
    columns: Seq[String]
)

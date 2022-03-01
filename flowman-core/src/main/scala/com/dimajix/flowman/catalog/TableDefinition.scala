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

import java.util.Locale

import org.apache.spark.sql.catalyst.catalog.CatalogTable

import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.StructType


object TableDefinition {
    def ofTable(table:CatalogTable) : TableDefinition = {
        val id = table.identifier
        val schema = com.dimajix.flowman.types.StructType.of(table.dataSchema)
        TableDefinition(TableIdentifier(id.table, id.database.toSeq), schema.fields)
    }
}
final case class TableDefinition(
    identifier: TableIdentifier,
    columns: Seq[Field] = Seq.empty,
    comment: Option[String] = None,
    primaryKey: Seq[String] = Seq.empty,
    indexes: Seq[TableIndex] = Seq.empty
) {
    def schema : StructType = StructType(columns)

    def normalize() : TableDefinition = copy(
        columns = columns.map(f => f.copy(name = f.name.toLowerCase(Locale.ROOT))),
        primaryKey = primaryKey.map(_.toLowerCase(Locale.ROOT)).sorted,
        indexes = indexes.map(_.normalize())
    )

}


final case class TableIndex(
    name: String,
    columns: Seq[String],
    unique:Boolean = false
) {
    def normalize() : TableIndex = copy(
        name = name.toLowerCase(Locale.ROOT),
        columns = columns.map(_.toLowerCase(Locale.ROOT)).sorted
    )
}

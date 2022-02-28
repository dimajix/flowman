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

import org.apache.spark.sql.catalyst.catalog.CatalogTable


object TableIdentifier {
    def apply(table: String, db:Option[String]) : TableIdentifier = TableIdentifier(table, db.toSeq)

    def empty : TableIdentifier = TableIdentifier("", Seq.empty)

    def of(table:CatalogTable) : TableIdentifier = {
        of(table.identifier)
    }
    def of(id: org.apache.spark.sql.catalyst.TableIdentifier) : TableIdentifier = {
        TableIdentifier(id.table, id.database.toSeq)
    }
}
final case class TableIdentifier(
    table: String,
    space: Seq[String] = Seq.empty
) {
    private def quoteIdentifier(name: String): String = s"`${name.replace("`", "``")}`"

    def quotedString: String = {
        val replacedId = quoteIdentifier(table)
        val replacedSpace = space.map(quoteIdentifier)

        if (replacedSpace.nonEmpty) s"${replacedSpace.mkString(".")}.$replacedId" else replacedId
    }

    def unquotedString: String = {
        if (space.nonEmpty) s"${space.mkString(".")}.$table" else table
    }

    def toSpark : org.apache.spark.sql.catalyst.TableIdentifier = {
        org.apache.spark.sql.catalyst.TableIdentifier(table, database)
    }

    def quotedDatabase : Option[String] = if (space.nonEmpty) Some(space.map(quoteIdentifier).mkString(".")) else None
    def unquotedDatabase : Option[String] = if (space.nonEmpty) Some(space.mkString(".")) else None
    def database : Option[String] = unquotedDatabase

    override def toString: String = quotedString
}

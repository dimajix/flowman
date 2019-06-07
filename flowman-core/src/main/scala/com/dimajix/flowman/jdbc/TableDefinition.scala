package com.dimajix.flowman.jdbc

import org.apache.spark.sql.catalyst.TableIdentifier

import com.dimajix.flowman.types.Field

case class TableDefinition(
    identifier: TableIdentifier,
    fields: Seq[Field],
    comment: Option[String],
    primaryKey: Seq[String]
) {
}

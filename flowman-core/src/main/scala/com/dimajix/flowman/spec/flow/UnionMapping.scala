package com.dimajix.flowman.spec.flow

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.NullType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.TableIdentifier
import com.dimajix.flowman.util.SchemaUtils


class UnionMapping extends BaseMapping {
    @JsonProperty(value="inputs", required=true) private[spec] var _inputs:Seq[String] = _
    @JsonProperty(value="fields", required=false) private[spec] var _fields:Map[String,String] = _

    def inputs(implicit context: Context) : Seq[TableIdentifier] = _inputs.map(i => TableIdentifier.parse(context.evaluate(i)))
    def fields(implicit context: Context) : Map[String,String] = if (_fields != null) _fields.mapValues(context.evaluate) else null

    /**
      * Executes this MappingType and returns a corresponding DataFrame
      *
      * @param executor
      * @param input
      * @return
      */
    override def execute(executor:Executor, input:Map[TableIdentifier,DataFrame]) : DataFrame = {
        implicit val context = executor.context
        val tables = inputs.map(input(_))

        // Create a common schema from collected columns
        val fields = this.fields
        val schema = if (fields != null) SchemaUtils.createSchema(fields.toSeq) else getCommonSchema(tables)

        // Project all tables onto common schema
        val projectedTables = tables.map(table =>
            projectTable(table, schema)
        )

        // Now create a union of all tables
        projectedTables.reduce((l,r) => l.union(r))
    }

    /**
      * Creates the list of required dependencies
      *
      * @param context
      * @return
      */
    override def dependencies(implicit context: Context) : Array[TableIdentifier] = {
        inputs.toArray
    }

    private def getCommonSchema(tables:Seq[DataFrame])(implicit context: Context) = {
        def commonField(newField:StructField, fields:Map[String,StructField]) = {
            val existingField = fields.getOrElse(newField.name, newField)
            val nullable = existingField.nullable || newField.nullable
            val dataType = if (existingField.dataType == NullType) newField.dataType else existingField.dataType
            StructField(newField.name, dataType, nullable)
        }
        val allColumns = tables.foldLeft(Map[String,StructField]())((columns, table) => {
            val tableColumns = table
                .schema
                .map(field => field.name -> commonField(field, columns)).toMap
            columns ++ tableColumns
        })

        // Create a common schema from collected columns
        StructType(allColumns.values.toSeq.sortBy(_.name))
    }

    private def projectTable(table:DataFrame, schema:StructType) = {
        val tableColumnNames = table.schema.map(_.name).toSet
        table.select(schema.fields.map(column =>
            if (tableColumnNames.contains(column.name))
                table(column.name).cast(column.dataType)
            else
                lit(null).cast(column.dataType).as(column.name)
        ):_*)
    }
}

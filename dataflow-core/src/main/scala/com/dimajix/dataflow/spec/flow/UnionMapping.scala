package com.dimajix.dataflow.spec.flow

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.NullType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

import com.dimajix.dataflow.execution.Context
import com.dimajix.dataflow.util.SchemaUtils


class UnionMapping extends Mapping {
    @JsonProperty(value="inputs", required=true) private[spec] var _inputs:Seq[String] = _
    @JsonProperty(value="fields", required=false) private[spec] var _fields:Map[String,String] = _

    def inputs(implicit context: Context) : Seq[String] = _inputs.map(context.evaluate)
    def fields(implicit context: Context) : Map[String,String] = if (_fields != null) _fields.mapValues(context.evaluate) else null

    /**
      * Creates a new DataFrame from the specification
      *
      * @param context
      * @return
      */
    override def execute(implicit context:Context) : DataFrame = {
        val tables = inputs

        // Create a common schema from collected columns
        val fields = this.fields
        val schema = if (fields != null) SchemaUtils.createSchema(fields.toSeq) else getCommonSchema(tables)

        // Project all tables onto common schema
        val projectedTables = tables.map(tableName =>
            projectTable(context.session.table(tableName), schema)
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
    override def dependencies(implicit context: Context) : Array[String] = {
        inputs.toArray
    }

    private def getCommonSchema(tables:Seq[String])(implicit context: Context) = {
        def commonField(newField:StructField, fields:Map[String,StructField]) = {
            val existingField = fields.getOrElse(newField.name, newField)
            val nullable = existingField.nullable || newField.nullable
            val dataType = if (existingField.dataType == NullType) newField.dataType else existingField.dataType
            StructField(newField.name, dataType, nullable)
        }
        val allColumns = tables.foldLeft(Map[String,StructField]())((columns, tableName) => {
            val tableColumns = context.session.table(tableName)
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

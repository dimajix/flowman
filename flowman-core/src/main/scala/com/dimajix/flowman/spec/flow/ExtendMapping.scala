package com.dimajix.flowman.spec.flow

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.functions.expr

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.TableIdentifier

class ExtendMapping extends BaseMapping {
    @JsonProperty(value = "input", required = true) private var _input:String = _
    @JsonProperty(value = "columns", required = true) private var _columns:Map[String,String] = _

    def input(implicit context: Context) : TableIdentifier = TableIdentifier.parse(context.evaluate(_input))
    def columns(implicit context: Context) : Map[String,String] = _columns.mapValues(context.evaluate)

    /**
      * Executes this Transform by reading from the specified source and returns a corresponding DataFrame
      *
      * @param executor
      * @param input
      * @return
      */
    override def execute(executor:Executor, input:Map[TableIdentifier,DataFrame]) : DataFrame = {
        implicit val context = executor.context
        val allColumns = columns
        val columnNames = allColumns.keys.toSet

        // First we need to create an ordering of all fields, such that dependencies are resolved correctly
        val parser = CatalystSqlParser
        def getRequiredColumns(column:String) = {
            val expression = allColumns(column)
            val result = parser.parseExpression(expression)
            result.references.map(_.name).toSet
        }
        def addField(column:String, orderedFields:Seq[String], usedFields:Set[String]) : (Seq[String], Set[String]) = {
            if (usedFields.contains(column))
                throw new RuntimeException("Cycling dependency between fields.")
            val deps = getRequiredColumns(column)
            val start = (orderedFields, usedFields + column)
            val result = deps.foldLeft(start) { case ((ordered, used), field) =>
                if (columnNames.contains(field) && !ordered.contains(field))
                    addField(field, ordered, used)
                else
                    (ordered, used)
            }

            (result._1 :+ column, result._2)
        }

        val start = (Seq[String](), Set[String]())
        val orderedFields = columnNames.foldLeft(start) { case ((ordered, used), field) =>
            if (!ordered.contains(field))
                addField(field, ordered, used)
            else
                (ordered, used)
        }

        // Now that we have a field order, we can transform the DataFrame
        val table = input(this.input)
        orderedFields._1.foldLeft(table)((df,field) => df.withColumn(field, expr(allColumns(field))))
    }

    /**
      * Returns the dependencies of this mapping, which is exactly one input table
      *
      * @param context
      * @return
      */
    override def dependencies(implicit context: Context) : Array[TableIdentifier] = {
        Array(input)
    }
}

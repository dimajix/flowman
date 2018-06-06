/*
 * Copyright 2018 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.model

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.schema.Field
import com.dimajix.flowman.spec.schema.FieldValue
import com.dimajix.flowman.spec.schema.PartitionField
import com.dimajix.flowman.spec.schema.SingleValue
import com.dimajix.flowman.util.SchemaUtils


class HiveTableRelation extends BaseRelation  {
    private val logger = LoggerFactory.getLogger(classOf[HiveTableRelation])

    @JsonProperty(value="database", required=false) private var _database: String = ""
    @JsonProperty(value="table", required=true) private var _table: String = ""
    @JsonProperty(value="external", required=false) private var _external: String = "false"
    @JsonProperty(value="location", required=false) private var _location: String = _
    @JsonProperty(value="format", required=false) private var _format: String = _
    @JsonProperty(value="rowFormat", required=false) private var _rowFormat: String = _
    @JsonProperty(value="inputFormat", required=false) private var _inputFormat: String = _
    @JsonProperty(value="outputFormat", required=false) private var _outputFormat: String = _
    @JsonProperty(value="partitions", required=false) private var _partitions: Seq[PartitionField] = Seq()
    @JsonProperty(value="properties", required=false) private var _properties: Map[String,String] = Map()

    def database(implicit context:Context) : String = context.evaluate(_database)
    def table(implicit context:Context) : String = context.evaluate(_table)
    def external(implicit context:Context) : Boolean = context.evaluate(_external).toBoolean
    def location(implicit context:Context) : String = context.evaluate(_location)
    def format(implicit context: Context) : String = context.evaluate(_format)
    def rowFormat(implicit context: Context) : String = context.evaluate(_rowFormat)
    def inputFormat(implicit context: Context) : String = context.evaluate(_inputFormat)
    def outputFormat(implicit context: Context) : String = context.evaluate(_outputFormat)
    def partitions(implicit context: Context) : Seq[PartitionField] = _partitions
    def properties(implicit context: Context) : Map[String,String] = _properties.mapValues(context.evaluate)

    /**
      * Reads data from the relation, possibly from specific partitions
      *
      * @param executor
      * @param schema - the schema to read. If none is specified, all available columns will be read
      * @param partitions - List of partitions. If none are specified, all the data will be read
      * @return
      */
    override def read(executor:Executor, schema:StructType, partitions:Map[String,FieldValue] = Map()) : DataFrame = {
        assert(partitions != null)

        implicit val context = executor.context
        val partitionsByName = this.partitions.map(p => (p.name, p)).toMap
        val partitionNames = this.partitions.map(_.name)
        val tableName = if (database.nonEmpty) database + "." + table else table
        logger.info(s"Reading DataFrame from Hive table $tableName with partitions ${partitionNames.mkString(",")}")

        def applyPartitionFilter(df:DataFrame, partitionName:String, partitionValue:FieldValue): DataFrame = {
            val field = partitionsByName(partitionName)
            val values = field.interpolate(partitionValue).toSeq
            df.filter(df(partitionName).isin(values:_*))
        }

        val reader = this.reader(executor)
        val tableDf = reader.table(tableName)
        val df = partitions.foldLeft(tableDf)((df,pv) => applyPartitionFilter(df, pv._1, pv._2))

        SchemaUtils.applySchema(df, schema)
    }

    /**
      * Writes data into the relation, possibly into a specific partition
      * @param executor
      * @param df - dataframe to write
      * @param partition - destination partition
      */
    override def write(executor:Executor, df:DataFrame, partition:Map[String,SingleValue], mode:String) : Unit = {
        implicit val context = executor.context
        val partitionNames = partitions.map(_.name)
        val tableName = database + "." + table
        logger.info(s"Writing DataFrame to Hive table $tableName with partitions ${partitionNames.mkString(",")}")

        val writer = df.write
            .format(format)
            .mode(mode)
            .partitionBy(partitionNames:_*)
        writer.saveAsTable(tableName)
    }

    /**
      * Creates a Hive table by executing the appropriate DDL
      * @param executor
      */
    override def create(executor:Executor) : Unit = {
        implicit val context = executor.context
        val external = if (this.external) "EXTERNAL" else ""
        val create = s"CREATE $external TABLE $database.$table"
        val fields = "(\n" + schema.fields.map(field => "    " + field.name + " " + field.ftype.sqlType).mkString(",\n") + "\n)"
        val comment = Option(this.description).map(d => s"\nCOMMENT '$d')").getOrElse("")
        val partitionBy = Option(partitions).filter(_.nonEmpty).map(p => s"\nPARTITIONED BY (${p.map(p => p.name + " " + p.ftype.sqlType).mkString(", ")})").getOrElse("")
        val rowFormat = Option(this.rowFormat).map(f => s"\nROW FORMAT SERDE '$f'").getOrElse("")
        val storedAs = Option(format).map(f => s"\nSTORED AS $f").getOrElse(
            Option(inputFormat).map(f => s"\nSTORED AS INPUTFORMAT '$f'" + Option(outputFormat).map(f => s"\nOUTPUTFORMAT '$f'").getOrElse("")).getOrElse("")
        )
        val location = Option(this.location).map(l => s"\nLOCATION '$l'").getOrElse("")
        val props = if (_properties.nonEmpty) "\nTBLPROPERTIES(" + this.properties.map(kv => "\n    \"" + kv._1 + "\"=\"" + kv._2 + "\"").mkString(",") + "\n)" else ""
        val stmt = create + fields + comment + partitionBy + rowFormat + storedAs + location + props
        logger.info(s"Executing SQL statement:\n$stmt")
        executor.spark.sql(stmt)
    }

    /**
      * Destroys the Hive table by executing an appropriate DROP statement
      * @param executor
      */
    override def destroy(executor:Executor) : Unit = {
        implicit val context = executor.context
        val stmt = s"DROP TABLE $database.$table"
        logger.info(s"Executing SQL statement:\n$stmt")
        executor.spark.sql(stmt)
    }
    override def migrate(executor:Executor) : Unit = ???
}

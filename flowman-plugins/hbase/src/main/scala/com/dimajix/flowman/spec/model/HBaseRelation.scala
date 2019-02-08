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

import scala.xml.XML

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Scan
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.hbase.HBaseRelation
import org.apache.spark.sql.execution.datasources.hbase.SparkHBaseConf
import org.apache.spark.sql.execution.datasources.hbase.{HBaseRelation => ShcRelation}
import org.apache.spark.sql.execution.datasources.hbase.{HBaseTableCatalog => ShcCatalog}
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.json4s.jackson.JsonMethods.parse
import org.slf4j.LoggerFactory

import com.dimajix.flowman.annotation.RelationType
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.schema.EmbeddedSchema
import com.dimajix.flowman.spec.schema.Schema
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.FieldType
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.types.StringType
import com.dimajix.flowman.util.SchemaUtils


object HBaseRelation {
    def supportsRead() : Boolean = {
        try {
            val method = classOf[Scan].getDeclaredMethod("setCaching", Integer.TYPE)
            method.getReturnType == classOf[Scan]
        }
        catch {
            case _:NoSuchMethodException => false
        }
    }


    class Column {
        @JsonProperty(value="family", required = true) private[HBaseRelation] var _family: String = _
        @JsonProperty(value="column", required = true) private[HBaseRelation] var _column: String = _
        @JsonProperty(value="alias", required = true) private[HBaseRelation] var _alias: String = _
        @JsonProperty(value="type", required = true) private[HBaseRelation] var _dtype: FieldType = com.dimajix.flowman.types.StringType
        @JsonProperty(value="description", required = false) private[HBaseRelation] var _description: String = _

        def family(implicit context: Context) : String = context.evaluate(_family)
        def column(implicit context: Context) : String = context.evaluate(_column)
        def alias(implicit context: Context) : String = Option(context.evaluate(_alias)).filter(_.nonEmpty).getOrElse(column)
        def dtype(implicit context: Context) : FieldType = _dtype
        def description(implicit context: Context) : String = _description
    }

    def apply(namespace:String, table:String, rowKey:String, columns:Seq[Column]) : HBaseRelation = {
        val relation = new HBaseRelation
        relation._namespace = namespace
        relation._table = table
        relation._rowKey = rowKey
        relation._columns = columns
        relation
    }

    def column(family:String, name:String, alias:String, dtype:FieldType = com.dimajix.flowman.types.StringType) : Column = {
        val result = new Column
        result._family = family
        result._column = name
        result._alias = alias
        result._dtype = dtype
        result
    }
}


@RelationType(kind = "hbase")
class HBaseRelation extends BaseRelation {
    private val logger = LoggerFactory.getLogger(classOf[HBaseRelation])

    @JsonProperty(value="namespace", required = true) private var _namespace:String = "default"
    @JsonProperty(value="table", required = true) private var _table:String = _
    @JsonProperty(value="rowKey", required = true) private var _rowKey:String = _
    @JsonProperty(value="columns", required = true) private var _columns:Seq[HBaseRelation.Column] = Seq()

    def namespace(implicit context: Context) : String = context.evaluate(_namespace)
    def table(implicit context: Context) : String = context.evaluate(_table)
    def rowKey(implicit context: Context) : String = context.evaluate(_rowKey)
    def columns(implicit context: Context) : Seq[HBaseRelation.Column] = _columns

    /**
      * Returns the schema of the relation
      * @param context
      * @return
      */
    override def schema(implicit context: Context) : Schema = {
        val fields = Field(rowKey, StringType, nullable = false) +: columns.map(c => Field(c.alias, c.dtype, description=c.description))
        EmbeddedSchema(fields)
    }

    /**
      * Reads data from the relation, possibly from specific partitions
      *
      * @param executor
      * @param schema     - the schema to read. If none is specified, all available columns will be read
      * @param partitions - List of partitions. If none are specified, all the data will be read
      * @return
      */
    override def read(executor: Executor, schema: StructType, partitions: Map[String, FieldValue]): DataFrame = {
        implicit val context = executor.context
        logger.info(s"Reading from HBase table '$namespace.$table'")

        val options = hbaseOptions
        val df = this.reader(executor)
            .options(options)
            .format("org.apache.spark.sql.execution.datasources.hbase")
            .load()

        SchemaUtils.applySchema(df, schema)
    }

    /**
      * Writes data into the relation, possibly into a specific partition
      *
      * @param executor
      * @param df        - dataframe to write
      * @param partition - destination partition
      */
    override def write(executor: Executor, df: DataFrame, partition: Map[String, SingleValue], mode: String): Unit = {
        implicit val context = executor.context
        logger.info(s"Writing to HBase table '$namespace.$table'")

        val options = hbaseOptions
        this.writer(executor, df)
            .options(options)
            .mode(mode)
            .format("org.apache.spark.sql.execution.datasources.hbase")
            .save()
    }

    override def clean(executor: Executor, partitions: Map[String, FieldValue]): Unit = {
        implicit val context = executor.context
        val namespace = this.namespace
        val table = this.table
        logger.info(s"Truncating to HBase table '$namespace.$table'")

        val config = hbaseConf
        val connection = ConnectionFactory.createConnection(config)
        val admin = connection.getAdmin()
        admin.truncateTable(TableName.valueOf(namespace + ":" + table), false)
        admin.close()
    }

    /**
      * This method will physically create the corresponding relation. This might be a Hive table or a directory. The
      * relation will not contain any data, but all metadata will be processed
      *
      * @param executor
      */
    override def create(executor: Executor): Unit = ???

    /**
      * This will delete any physical representation of the relation. Depending on the type only some meta data like
      * a Hive table might be dropped or also the physical files might be deleted
      *
      * @param executor
      */
    override def destroy(executor: Executor): Unit = ???

    /**
      * This will update any existing relation to the specified metadata.
      *
      * @param executor
      */
    override def migrate(executor: Executor): Unit = ???

    /**
     * Return null, because SHC does not support explicit schemas on read
     * @param context
     * @return
     */
    override protected def inputSchema(implicit context:Context) : StructType = null

    /**
     * Creates a Spark schema from the list of fields. The list is used for output operations, i.e. for writing
     * @param context
     * @return
     */
    override protected def outputSchema(implicit context:Context) : StructType = {
        val fields = StructField(rowKey, org.apache.spark.sql.types.StringType, nullable = false) +:
            columns.map(c => StructField(c.alias, c.dtype.sparkType))
        StructType(fields)
    }

    private def hbaseOptions(implicit context: Context) = {
        val keySpec = Seq(s""""$rowKey":{"cf":"rowkey", "col":"$rowKey", "type":"string"}""")
        val fieldSpec = columns.map(col => s""""${col.alias}":{"cf":"${col.family}", "col":"${col.column}", "type":"${col.dtype.sqlType}"}""")
        val columnsSpec = keySpec ++ fieldSpec

        val catalog = s"""{
                         |"table":{"name":"${this.table}", "namespace":"${this.namespace}", "tableCoder":"PrimitiveType"},
                         |"rowkey":"${this.rowKey}",
                         |"columns":{ ${columnsSpec.mkString(",\n")} }
                         |}
                         |""".stripMargin
        Map(
            ShcCatalog.tableCatalog -> catalog,
            ShcRelation.HBASE_CONFIGFILE -> "/etc/hbase/conf/hbase-site.xml",
            ShcCatalog.newTable -> "5"
        )

    }

    private def hbaseConf(implicit context:Context) = {
        val cFile = "/etc/hbase/conf/hbase-site.xml"
        val conf = HBaseConfiguration.create
        val xmlFile = XML.loadFile(cFile)
        (xmlFile \\ "property").foreach(
            x => conf.set((x \ "name").text, (x \ "value").text)
        )
        conf
    }
}

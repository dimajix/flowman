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
import org.apache.spark.sql.execution.datasources.hbase.{HBaseRelation => ShcRelation}
import org.apache.spark.sql.execution.datasources.hbase.{HBaseTableCatalog => ShcCatalog}
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import com.dimajix.flowman.annotation.RelationType
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.ResourceIdentifier
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


    case class Column(
        family:String,
        column:String,
        alias:String,
        dtype:FieldType=com.dimajix.flowman.types.StringType,
        description:Option[String]=None
    )
}


case class HBaseRelation(
    instanceProperties:Relation.Properties,
    tableSpace:String,
    table:String,
    rowKey:String,
    columns:Seq[HBaseRelation.Column]
) extends BaseRelation {
    private val logger = LoggerFactory.getLogger(classOf[HBaseRelation])

    /**
      * Returns the list of all resources which will be created by this relation.
      *
      * @return
      */
    override def provides: Seq[ResourceIdentifier] = ???

    /**
      * Returns the list of all resources which will be required by this relation for creation.
      *
      * @return
      */
    override def requires: Seq[ResourceIdentifier] = ???

    /**
      * Returns the list of all resources which will are managed by this relation for reading or writing a specific
      * partition. The list will be specifically  created for a specific partition, or for the full relation (when the
      * partition is empty)
      *
      * @param partitions
      * @return
      */
    override def resources(partitions: Map[String, FieldValue]): Seq[ResourceIdentifier] = ???

    /**
      * Returns the schema of the relation
      * @return
      */
    override def schema : Schema = {
        val fields = Field(rowKey, StringType, nullable = false) +: columns.map(c => Field(c.alias, c.dtype, description=c.description))
        EmbeddedSchema(
            Schema.Properties(context),
            None,
            fields,
            Nil)
    }

    /**
      * Reads data from the relation, possibly from specific partitions
      *
      * @param executor
      * @param schema     - the schema to read. If none is specified, all available columns will be read
      * @param partitions - List of partitions. If none are specified, all the data will be read
      * @return
      */
    override def read(executor: Executor, schema: Option[StructType], partitions: Map[String, FieldValue]): DataFrame = {
        logger.info(s"Reading from HBase table '$tableSpace.$table'")

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
        logger.info(s"Writing to HBase table '$tableSpace.$table'")

        val options = hbaseOptions
        this.writer(executor, df)
            .options(options)
            .mode(mode)
            .format("org.apache.spark.sql.execution.datasources.hbase")
            .save()
    }

    override def truncate(executor: Executor, partitions: Map[String, FieldValue]): Unit = {
        logger.info(s"Truncating to HBase table '$tableSpace.$table'")

        val config = hbaseConf
        val connection = ConnectionFactory.createConnection(config)
        val admin = connection.getAdmin()
        admin.truncateTable(TableName.valueOf(namespace + ":" + table), false)
        admin.close()
    }

    /**
      * Verify if the corresponding physical backend of this relation already exists
      * @param executor
      */
    override def exists(executor: Executor): Boolean = ???

    /**
      * This method will physically create the corresponding relation. This might be a Hive table or a directory. The
      * relation will not contain any data, but all metadata will be processed
      *
      * @param executor
      */
    override def create(executor: Executor, ignoreIfExists:Boolean): Unit = ???

    /**
      * This will delete any physical representation of the relation. Depending on the type only some meta data like
      * a Hive table might be dropped or also the physical files might be deleted
      *
      * @param executor
      */
    override def destroy(executor: Executor, ignoreIfNotExists:Boolean): Unit = ???

    /**
      * This will update any existing relation to the specified metadata.
      *
      * @param executor
      */
    override def migrate(executor: Executor): Unit = ???

    /**
     * Return null, because SHC does not support explicit schemas on read
     * @return
     */
    override protected def inputSchema : StructType = null

    /**
     * Creates a Spark schema from the list of fields. The list is used for output operations, i.e. for writing
     * @return
     */
    override protected def outputSchema : StructType = {
        val fields = StructField(rowKey, org.apache.spark.sql.types.StringType, nullable = false) +:
            columns.map(c => StructField(c.alias, c.dtype.sparkType))
        StructType(fields)
    }

    private def hbaseOptions = {
        val keySpec = Seq(s""""$rowKey":{"cf":"rowkey", "col":"$rowKey", "type":"string"}""")
        val fieldSpec = columns.map(col => s""""${col.alias}":{"cf":"${col.family}", "col":"${col.column}", "type":"${col.dtype.sqlType}"}""")
        val columnsSpec = keySpec ++ fieldSpec

        val catalog = s"""{
                         |"table":{"name":"${table}", "namespace":"${tableSpace}", "tableCoder":"PrimitiveType"},
                         |"rowkey":"${rowKey}",
                         |"columns":{ ${columnsSpec.mkString(",\n")} }
                         |}
                         |""".stripMargin
        Map(
            ShcCatalog.tableCatalog -> catalog,
            ShcRelation.HBASE_CONFIGFILE -> "/etc/hbase/conf/hbase-site.xml",
            ShcCatalog.newTable -> "5"
        )

    }

    private def hbaseConf = {
        val cFile = "/etc/hbase/conf/hbase-site.xml"
        val conf = HBaseConfiguration.create
        val xmlFile = XML.loadFile(cFile)
        (xmlFile \\ "property").foreach(
            x => conf.set((x \ "name").text, (x \ "value").text)
        )
        conf
    }
}



object HBaseRelationSpec {
    class Column {
        @JsonProperty(value="family", required = true) private var family: String = _
        @JsonProperty(value="column", required = true) private var column: String = _
        @JsonProperty(value="alias", required = true) private var alias: String = _
        @JsonProperty(value="type", required = true) private var dtype: FieldType = com.dimajix.flowman.types.StringType
        @JsonProperty(value="description", required = false) private var description: Option[String] = None

        def instantiate(context:Context) : HBaseRelation.Column = {
            HBaseRelation.Column(
                context.evaluate(family),
                context.evaluate(column),
                Option(context.evaluate(alias)).filter(_.nonEmpty).getOrElse(context.evaluate(column)),
                dtype,
                description.map(context.evaluate)
            )
        }
    }
}

@RelationType(kind = "hbase")
class HBaseRelationSpec extends RelationSpec {
    @JsonProperty(value = "namespace", required = true) private var namespace: String = "default"
    @JsonProperty(value = "table", required = true) private var table: String = _
    @JsonProperty(value = "rowKey", required = true) private var rowKey: String = _
    @JsonProperty(value = "columns", required = true) private var columns: Seq[HBaseRelationSpec.Column] = Seq()

    override def instantiate(context: Context): Relation = {
        HBaseRelation(
            instanceProperties(context),
            context.evaluate(namespace),
            context.evaluate(table),
            context.evaluate(rowKey),
            columns.map(_.instantiate(context))
        )
    }
}

package com.dimajix.flowman.spec.model

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.schema.Field
import com.dimajix.flowman.spec.schema.FieldValue
import com.dimajix.flowman.spec.schema.SingleValue
import com.dimajix.flowman.util.SchemaUtils


class HiveRelation extends BaseRelation  {
    private val logger = LoggerFactory.getLogger(classOf[HiveRelation])

    @JsonProperty(value="namespace") private var _namespace: String = _
    @JsonProperty(value="table") private var _table: String = _
    @JsonProperty(value="location") private var _location: String = _
    @JsonProperty(value="format") private var _format: String = _
    @JsonProperty(value="partitions") private var _partitions: Seq[Field] = Seq()

    def partitions(implicit context: Context) : Seq[Field] = _partitions
    def format(implicit context: Context) : String = context.evaluate(_format)
    def namespace(implicit context:Context) : String = context.evaluate(_namespace)
    def table(implicit context:Context) : String = context.evaluate(_table)

    /**
      * Reads data from the relation, possibly from specific partitions
      *
      * @param executor
      * @param schema - the schema to read. If none is specified, all available columns will be read
      * @param partitions - List of partitions. If none are specified, all the data will be read
      * @return
      */
    override def read(executor:Executor, schema:StructType, partitions:Map[String,FieldValue] = Map()) : DataFrame = {
        implicit val context = executor.context
        val partitionNames = this.partitions.map(_.name)
        val tableName = namespace + "." + table
        logger.info(s"Reading DataFrame from Hive table $tableName with partitions ${partitionNames.mkString(",")}")

        val reader = this.reader(executor)
        val df = reader.table(tableName)
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
        val tableName = namespace + "." + table
        logger.info(s"Writing DataFrame to Hive table $tableName with partitions ${partitionNames.mkString(",")}")

        val writer = df.write
            .format(format)
            .mode(mode)
            .partitionBy(partitionNames:_*)
        writer.saveAsTable(tableName)
    }

    override def create(executor:Executor) : Unit = ???
    override def destroy(executor:Executor) : Unit = ???
    override def migrate(executor:Executor) : Unit = ???
}

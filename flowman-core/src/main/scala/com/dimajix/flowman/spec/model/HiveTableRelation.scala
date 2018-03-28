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


class HiveTableRelation extends BaseRelation  {
    private val logger = LoggerFactory.getLogger(classOf[HiveTableRelation])

    @JsonProperty(value="database") private var _database: String = _
    @JsonProperty(value="table") private var _table: String = _
    @JsonProperty(value="external", required=false) private var _external: String = "false"
    @JsonProperty(value="location") private var _location: String = _
    @JsonProperty(value="format") private var _format: String = _
    @JsonProperty(value="partitions") private var _partitions: Seq[Field] = _

    def database(implicit context:Context) : String = context.evaluate(_database)
    def table(implicit context:Context) : String = context.evaluate(_table)
    def external(implicit context:Context) : Boolean = context.evaluate(_external).toBoolean
    def location(implicit context:Context) : String = context.evaluate(_location)
    def format(implicit context: Context) : String = context.evaluate(_format)
    def partitions(implicit context: Context) : Seq[Field] = _partitions

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
        val tableName = database + "." + table
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
        val fields = "(\n" + schema.map(field => "    " + field.name + " " + field.ftype.sqlType).mkString(",\n") + "\n)"
        val comment = Option(this.description).map(d => s"\nCOMMENT '$d')").getOrElse("")
        val partitionBy = Option(partitions).map(p => s"\nPARTITIONED BY (${p.map(p => p.name + " " + p.ftype.sqlType).mkString(", ")})").getOrElse("")
        val storedAs = Option(format).map(f => s"\nSTORED AS $f ").getOrElse("")
        val location = Option(this.location).map(l => s"\nLOCATION $l ").getOrElse("")
        val stmt = create + fields + comment + partitionBy + storedAs + location
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

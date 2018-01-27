package com.dimajix.dataflow.spec.model

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import com.dimajix.dataflow.execution.Context
import com.dimajix.dataflow.execution.Executor
import com.dimajix.dataflow.spec.model.Relation.Partition


class HiveRelation extends BaseRelation  {
    private val logger = LoggerFactory.getLogger(classOf[HiveRelation])

    @JsonProperty(value="location") private var _location: String = _
    @JsonProperty(value="format") private var _format: String = _
    @JsonProperty(value="partitions") private var _partitions: Seq[Field] = Seq()

    def partitions(implicit context: Context) : Seq[Field] = _partitions
    def format(implicit context: Context) : String = context.evaluate(_format)

    override def read(executor:Executor, schema:StructType, partition:Seq[Partition] = Seq()) : DataFrame = {
        implicit val context = executor.context
        val partitionNames = partitions.map(_.name)
        val tableName = namespace + "." + entity
        logger.info(s"Reading DataFrame from Hive table $tableName with partitions ${partitionNames.mkString(",")}")

        executor.session.read.table(tableName)
    }
    override def write(executor:Executor, df:DataFrame, partition:Partition, mode:String) : Unit = {
        implicit val context = executor.context
        val partitionNames = partitions.map(_.name)
        val tableName = namespace + "." + entity
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

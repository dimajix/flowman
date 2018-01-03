package com.dimajix.dataflow.spec.model

import java.util.Properties

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import com.dimajix.dataflow.execution.Context
import com.dimajix.dataflow.spec.model.Relation.Partition
import com.dimajix.dataflow.util.SchemaUtils


class JdbcRelation extends BaseRelation {
    private val logger = LoggerFactory.getLogger(classOf[JdbcRelation])

    @JsonProperty(value="properties") private var _properties:Map[String,String] = Map()

    def properties(implicit context: Context) : Map[String,String] = _properties.mapValues(context.evaluate)

    /**
      * Reads the configured table from the source
      * @param context
      * @param schema
      * @return
      */
    override def read(context:Context, schema:StructType, partitions:Seq[Partition] = Seq()) : DataFrame = {
        implicit val icontext = context

        val table = namespace + "." + entity

        logger.info(s"Reading data from JDBC source $table in database $database")

        val reader = context.session.read
        options.foreach(kv => reader.option(kv._1, kv._2))

        // Get Connection
        val (db,props) = createProperties(context)

        // Connect to database
        SchemaUtils.applySchema(reader.jdbc(db.url, table, props), schema)
    }

    override def write(context:Context, df:DataFrame, partition:Partition = null) : Unit = {
        implicit val icontext = context

        val table = namespace + "." + entity

        logger.info(s"Writing data to JDBC source $table in database $database")

        val writer = df.write
        options.foreach(kv => writer.option(kv._1, kv._2))

        val (db,props) = createProperties(context)
        writer.jdbc(db.url, table, props)
    }

    override def create(context:Context) : Unit = Unit
    override def destroy(context:Context) : Unit = Unit
    override def migrate(context: Context) : Unit = Unit

    private def createProperties(implicit context: Context) = {
        // Get Connection
        val db = context.getDatabase(database)
        val props = new Properties()
        props.setProperty("user", db.username)
        props.setProperty("password", db.password)
        props.setProperty("driver", db.driver)

        db.properties.foreach(kv => props.setProperty(kv._1, kv._2))
        properties.foreach(kv => props.setProperty(kv._1, kv._2))

        logger.info("Connecting to jdbc source at {}", db.url)

        (db,props)
    }
}

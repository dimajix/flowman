/*
 * Copyright (C) 2018 The Flowman Authors
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

package com.dimajix.flowman.spec.relation

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import com.dimajix.common.Trilean
import com.dimajix.common.Unknown
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.Operation
import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.model.BaseRelation
import com.dimajix.flowman.model.RegexResourceIdentifier
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Schema
import com.dimajix.flowman.spec.annotation.RelationType
import com.dimajix.flowman.spec.schema.InlineSchema
import com.dimajix.flowman.types
import com.dimajix.flowman.types.BinaryType
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.FieldValue
import com.dimajix.flowman.types.IntegerType
import com.dimajix.flowman.types.LongType
import com.dimajix.flowman.types.SingleValue
import com.dimajix.flowman.types.StringType
import com.dimajix.flowman.types.TimestampType


case class KafkaRelation(
    instanceProperties:Relation.Properties,
    hosts:Seq[String],
    topics:Seq[String],
    startOffset:String="earliest",
    endOffset:String="latest",
    options:Map[String,String]=Map()
) extends BaseRelation {
    private val logger = LoggerFactory.getLogger(classOf[KafkaRelation])

    /**
      * Returns the list of all resources which will be created by this relation.
      *
      * @return
      */
    override def provides(op:Operation, partitions:Map[String,FieldValue] = Map.empty) : Set[ResourceIdentifier] = {
        op match {
            case Operation.CREATE | Operation.DESTROY => Set.empty
            case Operation.READ => Set.empty
            case Operation.WRITE =>
                topics.map(t => RegexResourceIdentifier("kafkaTopic", t)).toSet
        }
    }

    /**
      * Returns the list of all resources which will be required by this relation for creation.
      *
      * @return
      */
    override def requires(op:Operation, partitions:Map[String,FieldValue] = Map.empty) : Set[ResourceIdentifier] = {
        op match {
            case Operation.CREATE | Operation.DESTROY => Set.empty
            case Operation.READ =>
                topics.map(t => RegexResourceIdentifier("kafkaTopic", t)).toSet
            case Operation.WRITE => Set.empty
        }
    }

    /**
      * Returns the schema of the relation
      * @return
      */
    override def schema : Option[Schema] = {
        Some(InlineSchema(
            Schema.Properties(context),
            description,
            fields,
            Nil
        ))
    }

    /**
     * Returns a list of fields including the partition columns. This method should not perform any physical schema
     * inference.
     *
     * @return
     */
    override def fields: Seq[Field] = Seq(
        Field("key", BinaryType, nullable = true),
        Field("value", BinaryType, nullable = false),
        Field("topic", StringType, nullable = false),
        Field("partition", IntegerType, nullable = false),
        Field("offset", LongType, nullable = false),
        Field("timestamp", TimestampType, nullable = false),
        Field("timestampType", IntegerType, nullable = false)
    )

    /**
     * Returns the schema of the relation, either from an explicitly specified schema or by schema inference from
     * the physical source
     *
     * @param execution
     * @return
     */
    override def describe(execution: Execution, partitions:Map[String,FieldValue] = Map()): types.StructType = {
        val result = types.StructType(fields)

        applyDocumentation(result)
    }

    /**
      * Reads data from the relation, possibly from specific partitions
      *
      * @param execution
      * @param schema     - the schema to read. If none is specified, all available columns will be read
      * @param partitions - List of partitions. If none are specified, all the data will be read
      * @return
      */
    override def read(execution: Execution, partitions: Map[String, FieldValue]): DataFrame = {
        require(execution != null)
        require(schema != null)
        require(partitions != null)

        val hosts = this.hosts.mkString(",")
        val topics = this.topics.mkString(",")
        logger.info(s"Reading Kafka topics '$topics' at hosts '$hosts'")

        val reader = this.reader(execution, "kafka", options)
            .option("subscribe", topics)
            .option("kafka.bootstrap.servers", hosts)
            .option("startingOffsets", startOffset)
            .option("endingOffsets", endOffset)
        reader.load()
    }

    /**
      * Writes data into the relation, possibly into a specific partition
      *
      * @param execution
      * @param df        - dataframe to write
      * @param partition - destination partition
      */
    override def write(execution: Execution, df: DataFrame, partition: Map[String, SingleValue], mode: OutputMode): Unit = {
        require(execution != null)
        require(df != null)
        require(partition != null)

        val hosts = this.hosts.mkString(",")
        val topic = this.topics.headOption.getOrElse(throw new IllegalArgumentException(s"Missing field 'topic' in relation '$name'"))
        logger.info(s"Writing to Kafka topic '$topic' at hosts '$hosts'")

        this.writer(execution, df, "kafka", options, mode.batchMode)
            .option("topic", topic)
            .option("kafka.bootstrap.servers", hosts)
            .save()
    }

    override def truncate(execution: Execution, partitions: Map[String, FieldValue]): Unit = {
        throw new UnsupportedOperationException("Cleaning Kafka topics is not supported")
    }

    /**
      * Reads data from a streaming source
      *
      * @param execution
      * @param schema
      * @return
      */
    override def readStream(execution: Execution): DataFrame = {
        require(execution != null)
        require(schema != null)

        val hosts = this.hosts.mkString(",")
        val topics = this.topics.mkString(",")
        logger.info(s"Streaming from Kafka topics '$topics' at hosts '$hosts'")

        val reader = execution.spark.readStream.options(options)
            .format("kafka")
            .option("subscribe", topics)
            .option("kafka.bootstrap.servers", hosts)
            .option("startingOffsets", startOffset)
        reader.load()
    }

    /**
      * Writes data to a streaming sink
      *
      * @param execution
      * @param df
      * @return
      */
    override def writeStream(execution: Execution, df: DataFrame, mode: OutputMode, trigger:Trigger, checkpointLocation: Path): StreamingQuery = {
        require(execution != null)
        require(df != null)

        val hosts = this.hosts.mkString(",")
        val topic = this.topics.headOption.getOrElse(throw new IllegalArgumentException(s"Missing field 'topic' in Kafka relation '$name'"))
        logger.info(s"Streaming to Kafka topic '$topic' at hosts '$hosts'")

        this.streamWriter(execution, df, "kafka", options, mode.streamMode, trigger, checkpointLocation)
            .option("topic", topic)
            .option("kafka.bootstrap.servers", hosts)
            .start()
    }

    /**
      * Verify if the corresponding physical backend of this relation already exists
      * @param execution
      */
    override def exists(execution: Execution): Trilean = Unknown

    /**
     * Returns true if the relation exists and has the correct schema. If the method returns false, but the
     * relation exists, then a call to [[migrate]] should result in a conforming relation.
     *
     * @param execution
     * @return
     */
    override def conforms(execution: Execution): Trilean = Unknown

    /**
     * Verify if the corresponding physical backend of this relation already exists
     * @param execution
     */
    override def loaded(execution: Execution, partition:Map[String,SingleValue]): Trilean = Unknown

    /**
      * This method will physically create the corresponding relation. This might be a Hive table or a directory. The
      * relation will not contain any data, but all metadata will be processed
      *
      * @param execution
      */
    override def create(execution: Execution): Unit = ???

    /**
      * This will delete any physical representation of the relation. Depending on the type only some meta data like
      * a Hive table might be dropped or also the physical files might be deleted
      *
      * @param execution
      */
    override def destroy(execution: Execution): Unit = ???

    /**
      * This will update any existing relation to the specified metadata.
      *
      * @param execution
      */
    override def migrate(execution: Execution): Unit = {}

    /**
      * Returns empty schema, so we read in all columns from Kafka
      *
      * @return
      */
    override protected def inputSchema : Option[StructType] = {
        Some(StructType(Seq(
            StructField("key", org.apache.spark.sql.types.BinaryType),
            StructField("value", org.apache.spark.sql.types.BinaryType),
            StructField("topic", org.apache.spark.sql.types.StringType),
            StructField("partition", org.apache.spark.sql.types.IntegerType),
            StructField("offset", org.apache.spark.sql.types.LongType),
            StructField("timestamp", org.apache.spark.sql.types.TimestampType),
            StructField("timestampType", org.apache.spark.sql.types.IntegerType)
        )))
    }

    /**
      * Returns empty schema, so we write columns as they are given to Kafka
      *
      * @return
      */
    override protected def outputSchema(execution:Execution) : Option[StructType] = None
}



@RelationType(kind="kafka")
class KafkaRelationSpec extends RelationSpec {
    @JsonProperty(value = "hosts", required = false) private var hosts: Seq[String] = Seq()
    @JsonProperty(value = "topics", required = false) private var topics: Seq[String] = Seq()
    @JsonProperty(value = "startOffset", required = false) private var startOffset: String = "earliest"
    @JsonProperty(value = "endOffset", required = false) private var endOffset: String = "latest"
    @JsonProperty(value = "options", required=false) private var options:Map[String,String] = Map()

    override def instantiate(context: Context, properties:Option[Relation.Properties] = None): Relation = {
        KafkaRelation(
            instanceProperties(context, properties),
            hosts.map(context.evaluate),
            topics.map(context.evaluate),
            context.evaluate(startOffset),
            context.evaluate(endOffset),
            context.evaluate(options)
        )
    }
}

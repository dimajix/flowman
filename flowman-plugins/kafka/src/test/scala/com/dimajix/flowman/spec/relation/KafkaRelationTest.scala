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

package com.dimajix.flowman.spec.relation

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.kafka010.KafkaTestUtils
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.OutputMode
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.spec.ObjectMapper
import com.dimajix.spark.testing.LocalSparkSession
import com.dimajix.spark.testing.QueryTest


class KafkaRelationTest extends AnyFlatSpec with Matchers with QueryTest with LocalSparkSession {
    private var testUtils: KafkaTestUtils = _

    private val topicId = new AtomicInteger(0)

    private def newTopic(): String = s"topic-${topicId.getAndIncrement()}"

    override def beforeAll(): Unit = {
        super.beforeAll()
        testUtils = new KafkaTestUtils
        testUtils.setup()
    }

    override def afterAll(): Unit = {
        if (testUtils != null) {
            testUtils.teardown()
            testUtils = null
        }
        super.afterAll()
    }

    "The KafkaRelation" should "be parseable" in {
        val spec =
            """
              |kind: kafka
              |hosts: localhost:9092
              |topics: topic-01
            """.stripMargin
        val relationSpec = ObjectMapper.parse[RelationSpec](spec)
        relationSpec shouldBe a[KafkaRelationSpec]

        val session = Session.builder().withSparkSession(spark).build()
        val relation = relationSpec.instantiate(session.context)

        val fields = relation.schema.get.fields
        fields(0).name should be ("key")
        fields(1).name should be ("value")
        fields(2).name should be ("topic")
        fields(3).name should be ("partition")
        fields(4).name should be ("offset")
        fields(5).name should be ("timestamp")
        fields(6).name should be ("timestampType")
    }

    it should "support batch reading" in {
        val lspark = spark
        import lspark.implicits._
        import org.apache.spark.sql.functions._

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution
        val context = session.context

        val topic = newTopic()
        testUtils.createTopic(topic, partitions = 3)
        testUtils.sendMessages(topic, (0 to 9).map(_.toString).toArray, Some(0))
        testUtils.sendMessages(topic, (10 to 19).map(_.toString).toArray, Some(1))
        testUtils.sendMessages(topic, Array("20"), Some(2))

        val relation = KafkaRelation(
            Relation.Properties(context),
            Seq(testUtils.brokerAddress),
            Seq(topic)
        )

        val schema = StructType(Seq(StructField("value", BinaryType)))
        val df = relation.read(executor, Some(schema), Map()).select(expr("CAST(value AS STRING)"))
        df.count() should be (21)
        checkAnswer(df, (0 to 20).map(_.toString).toDF)
    }

    it should "support batch writing only values" in {
        val lspark = spark
        import lspark.implicits._
        import org.apache.spark.sql.functions._

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution
        val context = session.context

        val topic = newTopic()
        testUtils.createTopic(topic, partitions = 3)

        val relation = KafkaRelation(
            Relation.Properties(context),
            Seq(testUtils.brokerAddress),
            Seq(topic)
        )

        val values = (0 to 20).map(_.toString).toDF
        relation.write(executor, values, Map(), OutputMode.APPEND)

        val schema = StructType(Seq(StructField("value", BinaryType)))
        val df = relation.read(executor, Some(schema), Map()).select(expr("CAST(value AS STRING)"))
        df.count() should be (21)
        checkAnswer(df, (0 to 20).map(_.toString).toDF)
    }

    it should "support batch writing keys and values" in {
        val lspark = spark
        import lspark.implicits._
        import org.apache.spark.sql.functions._

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution
        val context = session.context

        val topic = newTopic()
        testUtils.createTopic(topic, partitions = 3)

        val relation = KafkaRelation(
            Relation.Properties(context),
            Seq(testUtils.brokerAddress),
            Seq(topic)
        )

        val values = (0 to 20).toDF.select(
            (col("value") * 2).cast(StringType) as "key",
            col("value").cast(StringType) as "value"
        )
        relation.write(executor, values, Map(), OutputMode.APPEND)

        val schema = StructType(Seq(StructField("key", BinaryType), StructField("value", BinaryType)))
        val df = relation.read(executor, Some(schema), Map()).select(expr("CONCAT(CAST(key AS STRING),'_',CAST(value AS STRING))"))
        df.count() should be (21)
        checkAnswer(df, (0 to 20).map(k => (k*2).toString + "_" + k.toString).toDF)
    }

    it should "support stream reading" in {
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution
        val context = session.context

        val topic = newTopic()
        testUtils.createTopic(topic, partitions = 3)
        testUtils.sendMessages(topic, (0 to 9).map(_.toString).toArray, Some(0))
        testUtils.sendMessages(topic, (10 to 19).map(_.toString).toArray, Some(1))
        testUtils.sendMessages(topic, Array("20"), Some(2))

        val relation = KafkaRelation(
            Relation.Properties(context),
            Seq(testUtils.brokerAddress),
            Seq(topic)
        )

        withTempDir { checkpointLocation =>
            val spark = session.spark
            import spark.implicits._
            import org.apache.spark.sql.functions._

            val schema = StructType(Seq(StructField("value", BinaryType)))
            val values = relation.readStream(executor, Some(schema)).select(expr("CAST(value AS STRING)"))
            val query = values.writeStream
                .format("memory")
                .queryName("kafka_dings")
                .outputMode("append")
                .option("checkpointLocation", checkpointLocation.toString)
                .start()

            Thread.sleep(10*1000)

            val df = spark.read.table("kafka_dings")
            df.count() should be (21)
            checkAnswer(df, (0 to 20).map(_.toString).toDF)

            query.stop()
            session.spark.streams.awaitAnyTermination()
        }
    }

    it should "support stream writing" in {

    }
}

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

import org.apache.hadoop.hbase.HBaseTestingUtility
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Get
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.hbase.SparkHBaseConf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.Module
import com.dimajix.flowman.testing.LocalSparkSession


class HBaseRelationTest extends FlatSpec with Matchers  with LocalSparkSession {
    val hbaseTestingUtility = new HBaseTestingUtility
    conf.set(SparkHBaseConf.testConf, "true")

    override def beforeAll() {
        super.beforeAll()
        hbaseTestingUtility.startMiniCluster
        SparkHBaseConf.conf = hbaseTestingUtility.getConfiguration
    }

    override def afterAll() {
        hbaseTestingUtility.shutdownMiniCluster()
        super.afterAll()
    }

    def createTable(tableName: String, columnFamily:String) {
        hbaseTestingUtility.createMultiRegionTable(TableName.valueOf(tableName), columnFamily.getBytes)
    }


    "The HBase relation" should "be parseable" in {
        val spec =
            s"""
               |relations:
               |  t0:
               |    kind: hbase
               |    namespace: default
               |    table: table0
               |    rowKey: "key"
               |    columns:
               |      - family: f0
               |        column: c0
               |        alias: "col_0"
               |        type: string
               |      - family: f0
               |        column: c1
               |        alias: "col_1"
               |        type: string
               |""".stripMargin
        val project = Module.read.string(spec).toProject("project")
        val relation = project.relations("t0")
        relation shouldBe a[HBaseRelationSpec]
    }

    it should "support writing to" in {
        createTable("table1", "f")
        val relation = HBaseRelation("default", "table1", "pk", Seq(HBaseRelation.Column("f", "col1", "col_1")))
        val df = spark.range(1, 10).toDF().select(
            col("id").cast("string").as("pk"),
            (col("id")*lit(2)).cast("string").as("col_1")
        )

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor
        relation.write(executor, df)

        val connection = ConnectionFactory.createConnection(hbaseTestingUtility.getConfiguration)
        val table = connection.getTable(TableName.valueOf("table1"))
        (1 until 10).foreach { id =>
            val row = table.get(new Get(id.toString.getBytes))
            row.containsColumn("f".getBytes(), "col1".getBytes()) should be (true)
            row.getColumnLatestCell("f".getBytes(), "col1".getBytes()).getValue.map(_.toChar).mkString("") should be ((id*2).toString)
        }
    }

    it should "support reading without a Schema" in {
        if (!HBaseRelation.supportsRead())
            cancel()

        val relation = HBaseRelation("default", "table1", "pk", Seq(HBaseRelation.Column("f", "col1", "col_1")))

        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.executor
        val df = relation.read(executor, null, Map())
        val rows = df.sort("pk").collect()

        rows.toSeq should be((1 until 10).map(i => Row(i.toString, (2*i).toString)))
    }
}

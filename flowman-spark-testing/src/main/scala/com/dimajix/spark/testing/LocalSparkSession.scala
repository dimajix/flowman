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

package com.dimajix.spark.testing

import java.io.File

import scala.util.Try

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.Suite


trait LocalSparkSession extends LocalTempDir { this:Suite =>
    var spark: SparkSession = _
    var sc: SparkContext = _
    val conf = new SparkConf(false)

    val hiveSupported: Boolean = Try {
          org.apache.hadoop.hive.shims.ShimLoader.getMajorVersion
          true
        }.getOrElse(false)

    def configureSpark(builder: SparkSession.Builder) : SparkSession.Builder = {
        builder
    }

    override def beforeAll() : Unit = {
        super.beforeAll()

        val builder = SparkSession.builder()
            .master("local[4]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.sql.session.timeZone", "UTC")

        val localMetastorePath = new File(tempDir, "metastore").getCanonicalPath
        val localWarehousePath = new File(tempDir, "warehouse").getCanonicalPath
        val checkpointPath  = new File(tempDir, "checkpoints").getCanonicalPath
        val streamingCheckpointPath  = new File(tempDir, "streamingCheckpoints").getCanonicalPath

        // Only enable Hive support when it actually works. Currently Spark 2.x will not support Hadoop 3.x
        if (hiveSupported) {
            // We have to mask all properties in hive-site.xml that relates to metastore
            // data source as we used a local metastore here.
            val hiveConfVars = HiveConf.ConfVars.values()
            hiveConfVars.foreach { confvar =>
                if (confvar.varname.contains("datanucleus") ||
                    confvar.varname.contains("jdo")) {
                    builder.config("spark.hadoop." + confvar.varname, confvar.getDefaultExpr())
                }
            }
            builder.config("spark.hadoop.javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=$localMetastorePath;create=true")
                .config("spark.hadoop.datanucleus.rdbms.datastoreAdapterClassName", "org.datanucleus.store.rdbms.adapter.DerbyAdapter")
                .config("spark.hadoop.datanucleus.schema.autoCreateTables", true)
                .config("spark.hadoop.datanucleus.schema.autoCreateAll", true)
                .config("spark.hadoop.datanucleus.autoCreateSchema", true)
                .config("spark.hadoop.datanucleus.autoCreateColumns", true)
                .config("spark.hadoop.datanucleus.autoCreateConstraints", true)
                .config("spark.hadoop.datanucleus.autoStartMechanismMode", "ignored")
                .config("spark.hadoop.hive.metastore.schema.verification.record.version", true)
                .config("spark.hadoop.hive.metastore.schema.verification", false)
                .config("spark.hadoop.hive.metastore.uris", "")
                .config("spark.sql.hive.metastore.sharedPrefixes", "org.apache.derby")
                .enableHiveSupport()
        }

        builder.config("spark.sql.streaming.checkpointLocation", streamingCheckpointPath.toString)
            .config("spark.sql.warehouse.dir", localWarehousePath)
            .config(conf)

        configureSpark(builder)

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        sc = spark.sparkContext
        sc.setCheckpointDir(checkpointPath)

        // Perform one Spark operation, this help to fix some race conditions with frequent setup/teardown
        spark.emptyDataFrame.count()
    }

    override def afterAll() : Unit = {
        if (spark != null) {
            spark.stop()
            spark = null
            sc = null
        }

        super.afterAll()
    }
}

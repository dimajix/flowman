package com.dimajix.dataflow

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite

/**
  * Created by kaya on 18.01.17.
  */
trait LocalSparkSession extends BeforeAndAfterAll { this:Suite =>
    var session: SparkSession = _

    override def beforeAll() : Unit = {
        session = SparkSession.builder()
            .master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
            .config("spark.sql.shuffle.partitions", "8")
            .getOrCreate()
        session.sparkContext.setLogLevel("WARN")
    }
    override def afterAll() : Unit = {
        if (session != null) {
            session.stop()
            session = null
        }
    }
}

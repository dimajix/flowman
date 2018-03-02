package com.dimajix.flowman

import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite
import org.scalatest.mockito.MockitoSugar


trait MockedSparkSession extends BeforeAndAfterAll with MockitoSugar { this:Suite =>
    private var session: SparkSession = _
    var spark: SparkSession = _

    override def beforeAll() : Unit = {
        session = SparkSession.builder()
            .master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
            .config("spark.sql.shuffle.partitions", "8")
            .getOrCreate()
        session.sparkContext.setLogLevel("WARN")

        spark = mock[SparkSession]
        when(spark.sparkContext).thenReturn(session.sparkContext)
        when(spark.conf).thenReturn(session.conf)
    }
    override def afterAll() : Unit = {
        if (session != null) {
            session.stop()
            session = null
            spark = null
        }
    }
}

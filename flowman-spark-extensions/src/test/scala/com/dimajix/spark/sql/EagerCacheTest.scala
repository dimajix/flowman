package com.dimajix.spark.sql

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.spark.sql.execution.ExtraStrategies
import com.dimajix.spark.sql.functions._
import com.dimajix.spark.testing.LocalSparkSession


class EagerCacheTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    override def beforeAll() : Unit = {
        super.beforeAll()
        ExtraStrategies.register(spark)
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    }

    "The EagerCache" should "work" in {
        val accumulator = spark.sparkContext.longAccumulator
        val source = count_records(spark.range(10000).toDF, accumulator).cache()
        //val source = spark.range(10000).toDF.cache()
        //val source = count_records(spark.read.csv("/dimajix/projects/smartclip/data/moat"), accumulator).cache().toDF()
        //val source = spark.read.csv("/srv/ssd/smartclip/adselect-req/2017/02/01").cache().toDF()
        //val source = count_records(spark.read.csv("/srv/ssd/smartclip/adselect-req/2017/02/01").toDF(), accumulator).cache()

        val df2 = source.withColumn("id2", source("id") * 2)
        val df3 = df2.groupBy("id", "id2").count().where("count < 3")
        val result = df2.join(df3, Seq("id")).groupBy().count()
        result.explain(false)
        result.show()

        // Check that every record is generated only once
        val count = source.count()
        count should be (accumulator.value)

        // Check that cache is working and that source isn't processed a second time
        result.count()
        count should be (accumulator.value)
    }

    it should "not insert EagerCache if no cache is reused" in {
        val accumulator = spark.sparkContext.longAccumulator
        val source = count_records(spark.range(10000).toDF, accumulator).cache()
        val result = source.groupBy().count()
        result.explain(false)
        result.show()

        // Check that every record is generated only once
        val count = source.count()
        count should be (accumulator.value)

        // Check that cache is working and that source isn't processed a second time
        result.count()
        count should be (accumulator.value)
    }

    it should "work with multiple caches" in {
        val accumulator1 = spark.sparkContext.longAccumulator
        val source1 = count_records(spark.range(10000).toDF, accumulator1).cache()
        val df2 = source1.withColumn("id2", source1("id") * 2)
        val df3 = df2.groupBy("id", "id2").count().where("count < 3")
        val df4 = df2.join(df3, Seq("id"))

        val accumulator2 = spark.sparkContext.longAccumulator
        val source2 = count_records(spark.range(5000).toDF, accumulator2).cache()
        val df5 = source2.withColumn("id2", source2("id") * 2)
        val df6 = df5.groupBy("id", "id2").count().where("count < 2")
        val df7 = df5.join(df6, Seq("id"))

        val result = df4.join(df7, Seq("id")).groupBy().count()
        result.explain(false)
        result.show()

        // Check that every record is generated only once
        source1.count() should be (accumulator1.value)
        source2.count() should be (accumulator2.value)

        // Check that cache is working and that source isn't processed a second time
        result.count()
        source1.count() should be (accumulator1.value)
        source2.count() should be (accumulator2.value)
    }

    it should "work with multiple nested caches" in {
        val accumulator1 = spark.sparkContext.longAccumulator
        val source1 = count_records(spark.range(10000).toDF, accumulator1).cache()
        val df2 = source1.withColumn("id2", source1("id") * 2)
        val df3 = df2.groupBy("id", "id2").count().where("count < 3")
        val df4 = df2.join(df3, Seq("id")).cache()

        val accumulator2 = spark.sparkContext.longAccumulator
        val source2 = count_records(spark.range(5000).toDF, accumulator2).cache()
        val df5 = source2.withColumn("id2", source2("id") * 2)
        val df6 = df5.groupBy("id", "id2").count().where("count < 2")
        val df7 = df5.join(df6, Seq("id"))

        val result = df4.join(df7, Seq("id")).groupBy().count()
        result.explain(false)
        result.show()

        // Check that every record is generated only once
        source1.count() should be (accumulator1.value)
        source2.count() should be (accumulator2.value)

        // Check that cache is working and that source isn't processed a second time
        result.count()
        source1.count() should be (accumulator1.value)
        source2.count() should be (accumulator2.value)
    }
}

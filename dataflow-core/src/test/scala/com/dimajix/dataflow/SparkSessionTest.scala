package com.dimajix.dataflow

import org.scalatest.FlatSpec
import org.scalatest.Matchers

class SparkSessionTest extends FlatSpec with Matchers with LocalSparkSession {
    "A SparkSession" should "have configurable properties" in {
        spark.conf.set("spark.sql.prop1", "p1")
        spark.conf.get("spark.sql.prop1") should be ("p1")
        an[NoSuchElementException] should be thrownBy spark.conf.get("spark.sql.prop2")
    }

    "An inherited SparkSession" should "be able to have different configuration" in {
        spark.conf.set("spark.sql.prop1", "p1")
        spark.conf.get("spark.sql.prop1") should be ("p1")
        an[NoSuchElementException] should be thrownBy spark.conf.get("spark.sql.prop2")

        val derivedSession = spark.newSession()
        derivedSession.conf.set("spark.sql.prop1", "derived1")
        spark.conf.get("spark.sql.prop1") should be ("p1")
        derivedSession.conf.get("spark.sql.prop1") should be ("derived1")

        derivedSession.conf.set("spark.sql.prop2", "derived2")
        derivedSession.conf.get("spark.sql.prop2") should be ("derived2")
        an[NoSuchElementException] should be thrownBy spark.conf.get("spark.sql.prop2")

        spark.conf.set("spark.sql.prop1", "p1x")
        spark.conf.get("spark.sql.prop1") should be ("p1x")
        derivedSession.conf.get("spark.sql.prop1") should be ("derived1")

        spark.conf.set("spark.sql.prop3", "p3")
        spark.conf.get("spark.sql.prop3") should be ("p3")
        an[NoSuchElementException] should be thrownBy derivedSession.conf.get("spark.sql.prop3")
    }
}

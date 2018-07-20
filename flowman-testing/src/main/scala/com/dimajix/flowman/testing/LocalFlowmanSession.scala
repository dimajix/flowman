package com.dimajix.flowman.testing

import org.scalatest.Suite

import com.dimajix.flowman.execution.Session


trait LocalFlowmanSession extends LocalSparkSession { this:Suite =>
    var session:Session = null

    override def beforeAll() : Unit = {
        super.beforeAll()

        session = Session.builder()
                .withSparkSession(spark)
                .build()
        // Force configuration of Spark session
        spark = session.spark
    }
    override def afterAll() : Unit = {
        if (session != null) {
            session = null
        }
        super.afterAll()
    }
}

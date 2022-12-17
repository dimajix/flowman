package com.dimajix.flowman.dsl

import java.io.File

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.dsl.example.DqmProject
import com.dimajix.flowman.execution.Lifecycle
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.model.JobIdentifier
import com.dimajix.spark.testing.LocalSparkSession


class ExampleSpec extends AnyFlatSpec with Matchers with LocalSparkSession {
    "A Project" should "be loadable" in (if (hiveSupported) {
        val project = DqmProject.instantiate()

        // Create dummy source directory
        new File(tempDir, "hdfs/landing/kafka/topic=stream.events.1.dev").mkdirs()

        val session = Session.builder()
            .withProject(project)
            .withEnvironment("hdfs_basedir", new File(tempDir, "hdfs").toString)
            .withSparkSession(spark)
            .build()

        val context = session.getContext(project)
        val runner = session.runner

        val job = context.getJob(JobIdentifier("test"))

        runner.executeJob(job, Lifecycle.ALL) should be (Status.SUCCESS)

        session.shutdown()
    })
}

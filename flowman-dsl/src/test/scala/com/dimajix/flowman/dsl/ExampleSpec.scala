package com.dimajix.flowman.dsl

import java.io.File

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.dsl.example.DqmProject
import com.dimajix.flowman.execution.Lifecycle
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.model.JobIdentifier
import com.dimajix.spark.testing.LocalSparkSession


class ExampleSpec extends FlatSpec with Matchers with LocalSparkSession {
    "A Project" should "be loadable" in (if (hiveSupported) {
        val project = DqmProject.instantiate()

        val session = Session.builder()
            .withProject(project)
            .withEnvironment("hdfs_basedir", new File(tempDir, "hdfs").toString)
            .withSparkSession(spark)
            .build()

        val context = session.getContext(project)
        val executor = session.executor
        val runner = session.runner

        val job = context.getJob(JobIdentifier("test"))

        runner.executeJob(executor, job, Lifecycle.ALL) should be (Status.SUCCESS)
    })
}

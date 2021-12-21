/*
 * Copyright 2019 Kaya Kupferschmidt
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

package com.dimajix.flowman.testing

import java.io.File
import java.io.IOException
import java.net.URI
import java.net.URL
import java.util.UUID

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf

import com.dimajix.flowman.common.Logging
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.hadoop.FileSystem
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.JobIdentifier
import com.dimajix.flowman.model.Namespace
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.Test
import com.dimajix.flowman.model.TestIdentifier
import com.dimajix.spark.features


object Runner {
    private def setupLogging() : Unit = {
        val log4j = System.getProperty("log4j.configuration")
        if (log4j == null || log4j.isEmpty) {
            val loader = Thread.currentThread.getContextClassLoader
            val url = loader.getResource("com/dimajix/flowman/testing/log4j-defaults.properties")
            PropertyConfigurator.configure(url)
        }

        Logging.setSparkLogging("WARN")
    }

    setupLogging()

    class Builder {
        private var namespace:Namespace = Namespace.read.default()
        private var project:Project = _
        private var environment:Map[String,String] = Map()
        private var config:Map[String,String] = Map()
        private var profiles:Seq[String] = Seq()
        private var sparkMaster:String = "local[*]"
        private var sparkName:String = ""
        private var allowHive:Boolean = true
        private lazy val fs = FileSystem(new Configuration(false))

        def withNamespace(namespace:URL) : Builder = {
            this.namespace = Namespace.read.url(namespace)
            this
        }
        def withNamespace(namespace:File) : Builder = {
            this.namespace = Namespace.read.file(namespace)
            this
        }
        def withNamespace(namespace:Namespace) : Builder = {
            this.namespace = namespace
            this
        }

        def withProject(project:File) : Builder = {
            this.project = Project.read.file(fs.local(project))
            this
        }
        def withProject(project:URL) : Builder = {
            this.project = Project.read.file(fs.local(project.toURI))
            this
        }
        def withProject(project:URI) : Builder = {
            this.project = Project.read.file(fs.local(project))
            this
        }
        def withProject(project:Project) : Builder = {
            this.project = project
            this
        }

        def withProfile(profile:String) : Builder = {
            this.profiles = this.profiles :+ profile
            this
        }
        def withProfiles(profiles:Seq[String]) : Builder= {
            this.profiles = this.profiles ++ profiles
            this
        }

        def withEnvironment(key:String, value:String) : Builder = {
            environment = environment + (key -> value)
            this
        }
        def withEnvironment(env:Map[String,String]) : Builder = {
            environment = environment ++ env
            this
        }

        def withConfig(key:String, value:String) : Builder = {
            config = config + (key -> value)
            this
        }
        def withConfig(env:Map[String,String]) : Builder = {
            config = config ++ env
            this
        }

        /**
         * Explicitly sets the Spark appliction name.
         * @param name
         * @return
         */
        def withSparkName(name:String) : Builder = {
            this.sparkName = name
            this
        }

        /**
         * Explicitly set a Spark master. Will use `local[*]` if no master is set
         * @param master
         * @return
         */
        def withSparkMaster(master:String) : Builder = {
            this.sparkMaster = master
            this
        }

        /**
         * Explicitly enable Hive support (although it is already enabled per default)
         * @return
         */
        def enableHive() : Builder = {
            this.allowHive = true
            this
        }

        /**
         * Disable Hive support (which is enabled per default)
         * @return
         */
        def disableHive() : Builder = {
            this.allowHive = false
            this
        }

        def build() : Runner = {
            if (project == null)
                throw new IllegalArgumentException("Runner requires a project")
            new Runner(namespace, project, environment, config, profiles, sparkMaster, sparkName, allowHive)
        }
    }

    def builder() : Builder = new Builder()
}


class Runner private(
    namespace: Namespace,
    project: Project,
    environment: Map[String,String],
    config: Map[String,String],
    profiles: Seq[String],
    sparkMaster:String,
    sparkName:String,
    enableHive:Boolean
) {
    /** Temp directory which can be used for storing test data */
    val tempDir : File = createTempDir()

    /** Hive MetaStore directory inside the [[tempDir]] */
    val metastorePath : String = new File(tempDir, "metastore").getCanonicalPath

    /** Hive Warehouse directory inside the [[tempDir]] */
    val warehousePath : String = new File(tempDir, "warehouse").getCanonicalPath

    /** Spark checkpoint directory inside the [[tempDir]] */
    val checkpointPath : String = new File(tempDir, "checkpoints").getCanonicalPath

    /** Spark streaming checkpoint directory inside the [[tempDir]] */
    val streamingCheckpointPath : String = new File(tempDir, "streamingCheckpoints").getCanonicalPath

    // Spark override properties
    private val sparkOverrides = Map(
        "spark.hadoop.javax.jdo.option.ConnectionURL" -> s"jdbc:derby:;databaseName=$metastorePath;create=true",
        "spark.hadoop.datanucleus.rdbms.datastoreAdapterClassName" -> "org.datanucleus.store.rdbms.adapter.DerbyAdapter",
        "spark.hadoop.datanucleus.schema.autoCreateTables" -> "true",
        "spark.hadoop.datanucleus.schema.autoCreateAll" -> "true",
        "spark.hadoop.datanucleus.autoCreateSchema" -> "true",
        "spark.hadoop.datanucleus.autoCreateColumns" -> "true",
        "spark.hadoop.datanucleus.autoCreateConstraints" -> "true",
        "spark.hadoop.datanucleus.autoStartMechanismMode" -> "ignored",
        "spark.hadoop.hive.metastore.schema.verification.record.version" -> "true",
        "spark.hadoop.hive.metastore.schema.verification" -> "false",
        "spark.hadoop.hive.metastore.uris" -> "",
        "spark.sql.hive.metastore.sharedPrefixes" -> "org.apache.derby",
        "spark.sql.session.timeZone" -> "UTC",
        SQLConf.CHECKPOINT_LOCATION.key -> streamingCheckpointPath,
        "spark.sql.warehouse.dir" -> warehousePath,
        SQLConf.SHUFFLE_PARTITIONS.key -> "8",
        "spark.ui.enabled" -> "false"
    )
    // Spark override properties for Hive related configuration stuff
    private val hiveOverrides = if (features.hiveSupported) {
            // We have to mask all properties in hive-site.xml that relates to metastore
            // data source as we used a local metastore here.
            HiveConf.ConfVars.values().flatMap { confvar =>
                if (confvar.varname.contains("datanucleus") ||
                    confvar.varname.contains("jdo")) {
                    Some(("spark.hadoop." + confvar.varname, confvar.getDefaultExpr()))
                }
                else {
                    None
                }
            }.toMap
        }
        else {
            Map[String,String]()
        }

    /**
      * Provides access to the Flowman session
      */
    val session : Session = {
        val builder = Session.builder()
            .withSparkSession(conf => createSparkSession(conf))
            .withNamespace(namespace)
            .withProject(project)
            .withEnvironment(environment)
            .withConfig(hiveOverrides)
            .withConfig(sparkOverrides)
            .withConfig(config)
            .withProfiles(profiles)

        if (sparkMaster.nonEmpty)
            builder.withSparkMaster(sparkMaster)
        if (sparkName.nonEmpty)
            builder.withSparkName(sparkName)

        builder.build()
    }

    /**
      * Run the specified phases of a single job within the project with the given arguments
      * @param jobName
      * @param args
      * @return
      */
    def runJob(jobName:String, phases:Seq[Phase], args:Map[String,String] = Map()) : Boolean = {
        val context = session.getContext(project)
        val job = context.getJob(JobIdentifier(jobName))
        runJob(job, phases, args)
    }

    /**
     * RUn the specified phases of a job with the specified arguments
     * @param job
     * @param phases
     * @param args
     * @return
     */
    def runJob(job:Job, phases:Seq[Phase], args:Map[String,String]) : Boolean = {
        val runner = session.runner
        val result = runner.executeJob(job, phases, args, force=true)

        result.success
    }

    /**
     * Run the specified phases of a job with the specified arguments
     * @param job
     * @param phases
     * @param args
     * @return
     */
    def runJob(jobName:String, phases:Seq[Phase], args:java.util.Map[String,String]) : Boolean = {
        runJob(jobName, phases, args.asScala.toMap)
    }

    /**
     * Run the specified phases of a job with the specified arguments (Java API)
     * @param job
     * @param phases
     * @param args
     * @return
     */
    def runJob(jobName:String, phases:java.util.List[Phase], args:java.util.Map[String,String]) : Boolean = {
        runJob(jobName, phases.asScala, args.asScala.toMap)
    }


    /**
     * Run a single test within the project
     * @param testName
     * @param args
     * @return
     */
    def runTest(testName:String) : Boolean = {
        val context = session.getContext(project)
        val test = context.getTest(TestIdentifier(testName))
        runTest(test)
    }

    /**
     * Runs an individual test
     * @param test
     * @return
     */
    def runTest(test:Test) : Boolean = {
        val runner = session.runner
        val result = runner.executeTest(test)

        result.success
    }

    /**
     * Runs all non-empty tests in a project. Tests without any assertions will be skipped.
     * @return
     */
    def runTests(parallel:Boolean=false) : Boolean = {
        val context = session.getContext(project)

        def run(testName:String) : Boolean = {
            val test = context.getTest(TestIdentifier(testName))
            if (test.assertions.nonEmpty) {
                runTest(test)
            }
            else {
                true
            }
        }

        val testNames = project.tests.keys.toSeq
        if (parallel)
            testNames.par.forall(run)
        else
            testNames.forall(run)
    }

    /**
      * Releases all resources including the Spark session and temporary directory
      */
    def shutdown() : Unit = {
        session.shutdown()
        deleteTempDir(tempDir)
    }

    /**
      * Creates a Spark session
      * @return
      */
    private def createSparkSession(conf:SparkConf) : SparkSession = {
        val builder = SparkSession.builder()
            .config(conf)
        if (features.hiveSupported && enableHive)
            builder.enableHiveSupport()
        val spark = builder.getOrCreate()
        val sc = spark.sparkContext
        sc.setLogLevel("WARN")
        sc.setCheckpointDir(checkpointPath)

        // Perform one Spark operation, this help to fix some race conditions with frequent setup/teardown
        spark.emptyDataFrame.count()
        spark
    }

    /**
      * Create a directory inside the given parent directory.
      * The directory is guaranteed to be newly created, and is not marked for automatic
      * deletion.
      */
    private def createDirectory(root: String): File = {
        var attempts = 0
        val maxAttempts = 10
        var dir: File = null
        while (dir == null) {
            attempts += 1
            if (attempts > maxAttempts) {
                throw new IOException(
                    s"Failed to create a temp directory (under ${root}) after ${maxAttempts}")
            }
            try {
                dir = new File(root, "spark-" + UUID.randomUUID.toString)
                if (dir.exists() || !dir.mkdirs()) {
                    dir = null
                }
            } catch { case e: SecurityException => dir = null; }
        }

        dir
    }

    /**
      * Create a temporary directory inside the given parent directory.
      * The directory will be automatically deleted when the VM shuts down.
      */
    private def createTempDir(root: String = System.getProperty("java.io.tmpdir")): File = {
        val dir = createDirectory(root)
        dir
    }

    private def deleteTempDir(dir:File) : Unit = {
        deleteRecursively(dir)
    }

    private def deleteRecursively(file: File): Unit = {
        if (file.isDirectory)
            file.listFiles.foreach(deleteRecursively)
        if (file.exists) {
            // Silently eat up all exceptions
            try {
                file.delete()
            }
            catch {
                case _:IOException =>
            }
        }
    }
}

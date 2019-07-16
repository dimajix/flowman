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
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.hadoop.FileSystem
import com.dimajix.flowman.history.Status
import com.dimajix.flowman.spec.JobIdentifier
import com.dimajix.flowman.spec.Namespace
import com.dimajix.flowman.spec.Project


object Runner {
    private def setupLogging() : Unit = {
        val log4j = System.getProperty("log4j.configuration")
        if (log4j == null || log4j.isEmpty) {
            val loader = Thread.currentThread.getContextClassLoader
            val url = loader.getResource("com/dimajix/flowman/testing/log4j-defaults.properties")
            PropertyConfigurator.configure(url)
        }

        // Adjust Spark logging level
        val l = org.apache.log4j.Level.toLevel("WARN")
        org.apache.log4j.Logger.getLogger("org").setLevel(l)
        org.apache.log4j.Logger.getLogger("akka").setLevel(l)
        org.apache.log4j.Logger.getLogger("hive").setLevel(l)
    }

    setupLogging()

    class Builder {
        private var namespace:Namespace = Namespace.read.default()
        private var project:Project = _
        private var environment:Map[String,String] = Map()
        private var profiles:Seq[String] = Seq()
        private var sparkMaster:String = "local[2]"
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

        def withSparkMaster(master:String) : Builder = {
            this.sparkMaster = master
            this
        }

        def build() : Runner = {
            new Runner(namespace, project, environment, profiles, sparkMaster)
        }
    }

    def builder() : Builder = new Builder()
}


class Runner private(
    namespace: Namespace,
    project: Project,
    environment: Map[String,String],
    profiles: Seq[String],
    sparkMaster:String) {

    val tempDir : File = createTempDir()

    val metastorePath : String = new File(tempDir, "metastore").getCanonicalPath
    val warehousePath : String = new File(tempDir, "wharehouse").getCanonicalPath
    val checkpointPath : String = new File(tempDir, "checkpoints").getCanonicalPath
    val streamingCheckpointPath : String = new File(tempDir, "streamingCheckpoints").getCanonicalPath

    // Spark override properties
    private val sparkOverrides = Map(
        "javax.jdo.option.ConnectionURL" -> s"jdbc:derby:;databaseName=$metastorePath;create=true",
        "datanucleus.rdbms.datastoreAdapterClassName" -> "org.datanucleus.store.rdbms.adapter.DerbyAdapter",
        ConfVars.METASTOREURIS.varname -> "",
        "spark.sql.streaming.checkpointLocation" -> streamingCheckpointPath.toString,
        "spark.sql.warehouse.dir" -> warehousePath
    )
    // Spark override properties for Hive related configuration stuff
    private val hiveOverrides = {
        // We have to mask all properties in hive-site.xml that relates to metastore
        // data source as we used a local metastore here.
        HiveConf.ConfVars.values().flatMap { confvar =>
            if (confvar.varname.contains("datanucleus") ||
                confvar.varname.contains("jdo")) {
                Some((confvar.varname, confvar.getDefaultExpr()))
            }
            else {
                None
            }
        }.toMap
    }

    /**
      * Provides access to the Flowman session
      */
    val session : Session = Session.builder()
        .withSparkSession(() => createSparkSession())
        .withNamespace(namespace)
        .withProject(project)
        .withEnvironment(environment)
        .withSparkConfig(hiveOverrides)
        .withSparkConfig(sparkOverrides)
        .withProfiles(profiles)
        .build()

    /**
      * Run a single job within the project
      * @param jobName
      * @param args
      * @return
      */
    def runJob(jobName:String, args:Map[String,String] = Map()) : Boolean = {
        val context = session.getContext(project)
        val executor = session.executor
        val runner = session.runner

        val job = context.getJob(JobIdentifier(jobName))
        val result = runner.execute(executor, job, args, true)

        result match {
            case Status.SUCCESS => true
            case Status.SKIPPED => true
            case _ => false
        }
    }

    def runJob(jobName:String, args:java.util.Map[String,String]) : Boolean = {
        runJob(jobName, args.asScala.toMap)
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
    private def createSparkSession() : SparkSession = {
        val conf = new SparkConf(false)
            .setAll(hiveOverrides)
            .setAll(sparkOverrides)
        val builder = SparkSession.builder()
            .master(sparkMaster)
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "8")
            .config(conf)
            .enableHiveSupport()
        val spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        val sc = spark.sparkContext
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
                case ex:IOException =>
            }
        }
    }
}

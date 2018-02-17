package com.dimajix.flowman.execution

import java.util.NoSuchElementException

import scala.collection.mutable

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import com.dimajix.flowman.spec.Namespace
import com.dimajix.flowman.spec.Project
import com.dimajix.flowman.spec.TableIdentifier
import com.dimajix.flowman.util.SparkUtils


private[execution] class RootExecutor(context:RootContext, sessionFactory:() => Option[SparkSession])
    extends AbstractExecutor(context, sessionFactory) {
    override protected val logger = LoggerFactory.getLogger(classOf[RootExecutor])

    private val _children = mutable.Map[String,ProjectExecutor]()
    private val _namespace = context.namespace

    /**
      * Returns the Namespace of the Executor
      *
      * @return
      */
    def namespace : Namespace = _namespace

     /**
      * Returns a fully qualified table as a DataFrame from a project belonging to the namespace of this executor
      *
      * @param name
      * @return
      */
    override def getTable(name: TableIdentifier): DataFrame = {
        if (name.project.isEmpty)
            throw new NoSuchElementException("Expected project name in table specifier")
        val child = _children.getOrElseUpdate(name.project.get, getProjectExecutor(name.project.get))
        child.getTable(TableIdentifier(name.name, None))
    }

    /**
      * Returns all tables belonging to the Root and child executors
      * @return
      */
    override def tables: Map[TableIdentifier, DataFrame] = {
        _children.values.map(_.tables).reduce(_ ++ _)
    }

    /**
      * Creates an instance of a table of a Dataflow, or retrieves it from cache
      *
      * @param identifier
      */
    override def instantiate(identifier: TableIdentifier): DataFrame = {
        if (identifier.project.isEmpty)
            throw new NoSuchElementException("Expected project name in table specifier")
        val child = getProjectExecutor(identifier.project.get)
        child.instantiate(TableIdentifier(identifier.name, None))
    }

    override def cleanup(): Unit = {
        logger.info("Cleaning up root executor and all children")
        _children.values.foreach(_.cleanup())
    }

    def getProjectExecutor(project:Project) : ProjectExecutor = {
        _children.getOrElseUpdate(project.name, createProjectExecutor(project))
    }
    def getProjectExecutor(name:String) : ProjectExecutor = {
        _children.getOrElseUpdate(name, createProjectExecutor(name))
    }

    private def createProjectExecutor(project:Project) : ProjectExecutor = {
        val pcontext = context.getProjectContext(project)
        val executor = new ProjectExecutor(this, pcontext, createProjectSession(pcontext))
        _children.update(project.name, executor)
        executor
    }
    private def createProjectExecutor(project:String) : ProjectExecutor = {
        val pcontext = context.getProjectContext(project)
        val executor = new ProjectExecutor(this, pcontext, createProjectSession(pcontext))
        _children.update(project, executor)
        executor
    }
    private def createProjectSession(context: Context)() : Option[SparkSession] = {
        logger.info("Creating new Spark session for project")
        val session = spark
        if (session != null) {
            val sparkSession = session.newSession()
            Some(sparkSession)
        }
        else {
            None
        }
    }
}

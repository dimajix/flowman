package com.dimajix.dataflow.execution

import java.util.NoSuchElementException

import scala.collection.mutable

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import com.dimajix.dataflow.spec.Namespace
import com.dimajix.dataflow.spec.Project
import com.dimajix.dataflow.spec.RelationIdentifier
import com.dimajix.dataflow.spec.TableIdentifier
import com.dimajix.dataflow.spec.flow.Mapping
import com.dimajix.dataflow.spec.model.Relation


class RootExecutor(context:RootContext, profiles:Seq[String], sessionFactory:() => SparkSession) extends AbstractExecutor(context) {
    private val logger = LoggerFactory.getLogger(classOf[RootExecutor])

    private var _session:Option[SparkSession] = None
    private val _children = mutable.Map[String,ProjectExecutor]()
    private val _namespace = context.namespace

    /**
      * Returns the Namespace of the Executor
      *
      * @return
      */
    def namespace : Namespace = _namespace

    /**
      * Returns (or lazily creates) a SparkSession of this Executor. The SparkSession will be derived from the global
      * SparkSession, but a new derived session with a separate namespace will be created.
      *
      * @return
      */
    override def session: SparkSession = {
        logger.info("Creating new local session for context")
        if (_session.isEmpty) {
            val session = sessionFactory()
            _session = Some(session)
            context.config.foreach(kv => session.conf.set(kv._1, kv._2))
        }
        _session.get
    }

    override def mappings: Map[TableIdentifier, Mapping] = {
        _children.values.map(_.mappings).reduce(_ ++ _)
    }

    /**
      * Returns a fully qualified mapping from a project belonging to the namespace of this executor
      *
      * @param name
      * @return
      */
    override def getMapping(name: TableIdentifier): Mapping = {
        if (name.project.isEmpty)
            throw new NoSuchElementException("Expected project name in mapping specifier")
        val child = _children.getOrElseUpdate(name.project.get, loadProject(name.project.get))
        child.getMapping(TableIdentifier(name.name, None))
    }
    /**
      * Returns a fully qualified relation from a project belonging to the namespace of this executor
      *
      * @param name
      * @return
      */
    override def getRelation(name: RelationIdentifier): Relation = {
        if (name.project.isEmpty)
            throw new NoSuchElementException("Expected project name in relation specifier")
        val child = _children.getOrElseUpdate(name.project.get, loadProject(name.project.get))
        child.getRelation(RelationIdentifier(name.name, None))
    }
    /**
      * Returns a fully qualified table as a DataFrame from a project belonging to the namespace of this executor
      *
      * @param name
      * @return
      */
    override def getTable(name: TableIdentifier): DataFrame = {
        if (name.project.isEmpty)
            throw new NoSuchElementException("Expected project name in table specifier")
        val child = _children.getOrElseUpdate(name.project.get, loadProject(name.project.get))
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

    private def getProjectExecutor(name:String) : ProjectExecutor = {
        _children.getOrElseUpdate(name, loadProject(name))
    }

    private def createProjectExecutor(project:Project) : Executor = {
        val context = createProjectContext(project)
        val executor = new ProjectExecutor(this, context)
        _children.update(project.name, executor)
        executor
    }
    private def createProjectContext(project: Project) : ProjectContext = {
        val pcontext = context.newProjectContext(project)
        profiles.foreach(p => project.profiles.get(p).foreach { profile =>
            logger.info(s"Applying project profile $p")
            context.withProfile(profile)
        })
        pcontext.withEnvironment(project.environment)
        pcontext.withConfig(project.config)
        pcontext
    }

    private def loadProject(name: String): ProjectExecutor = {
        val project : Project = ???
        createProjectExecutor(project)
    }
}

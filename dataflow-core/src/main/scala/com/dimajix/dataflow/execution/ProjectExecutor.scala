package com.dimajix.dataflow.execution

import scala.collection.mutable

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.broadcast
import org.slf4j.LoggerFactory

import com.dimajix.dataflow.spec.RelationIdentifier
import com.dimajix.dataflow.spec.TableIdentifier
import com.dimajix.dataflow.spec.flow.Mapping
import com.dimajix.dataflow.spec.model.Relation


private[execution] class ProjectExecutor(_parent:Executor, context:ProjectContext) extends AbstractExecutor(context) {
    private val logger = LoggerFactory.getLogger(classOf[ProjectExecutor])
    private val _tables = mutable.Map[String,DataFrame]()
    private val _project = context.project

    /**
      * Instantiates a table and recursively all its dependencies
      *
      * @param tableName
      * @return
      */
    private def createTable(tableName: String): DataFrame = {
        logger.info(s"Creating instance of table ${_project.name}/$tableName")

        // Lookup table definition
        val transform = getMapping(TableIdentifier(tableName, None))
        if (transform == null) {
            logger.error(s"Table ${_project.name}/$tableName not found")
            throw new NoSuchElementException(s"Table ${_project.name}/$tableName not found")
        }

        // Ensure all dependencies are instantiated
        logger.info(s"Ensuring dependencies for table ${_project.name}/$tableName")
        val dependencies = transform.dependencies.map(d => (d, instantiate(d))).toMap

        // Process table and register result as temp table
        logger.info(s"Instantiating table ${_project.name}/$tableName")
        val instance = transform.execute(this, dependencies).persist(transform.cache)
        val df = if (transform.broadcast)
            broadcast(instance)
        else
            instance

        _tables.put(tableName, df)
        df
    }

    override def session: SparkSession = _parent.session

    /**
      * Returns a list of all tables of this Executor.
      *
      * @return
      */
    override def tables : Map[TableIdentifier,DataFrame] = _tables.toMap.map(kv => (TableIdentifier(kv._1, _project.name), kv._2))

    /**
      * Creates an instance of a table of a Dataflow, or retrieves it from cache
      *
      * @param tableName
      */
    override def instantiate(tableName: TableIdentifier) : DataFrame = {
        if (tableName.project.isEmpty)
            _tables.getOrElseUpdate(tableName.name, createTable(tableName.name))
        else
            _parent.instantiate(tableName)
    }

    /**
      * Cleans up the session
      */
    def cleanup() : Unit = {
        // Unregister all temporary tables
        logger.info("Cleaning up temporary tables and caches")
        val catalog = session.catalog
        catalog.clearCache()
        _tables.foreach(kv => catalog.dropTempView(kv._1))
        _tables.foreach(kv => kv._2.unpersist())
    }

    /**
      * Returns a map of all Transforms in the project belonging to this Executor.
      *
      * @return
      */
    override def mappings: Map[TableIdentifier, Mapping] = _project.transforms.map(kv => (TableIdentifier(kv._1, _project.name), kv._2))

    /**
      * Returns a specific named Transform. The Transform can either be inside this Executors project or in a different
      * project within the same namespace
      *
      * @param name
      * @return
      */
    override def getMapping(name: TableIdentifier): Mapping = {
        if (name.project.isEmpty)
            _project.transforms.getOrElse(name.name, throw new NoSuchElementException(s"Mapping $name not found in project ${_project.name}"))
        else
            _parent.getMapping(name)
    }

    /**
      * Returns a specific named Relation. The Relation can either be inside this Executors project or in a different
      * project within the same namespace
      *
      * @param name
      * @return
      */
    override def getRelation(name: RelationIdentifier): Relation = {
        if (name.project.isEmpty)
            _project.models.getOrElse(name.name, throw new NoSuchElementException(s"Relation $name not found in project ${_project.name}"))
        else
            _parent.getRelation(name)
    }

    /**
      * Returns a named table created by an executor. If a project is specified, Executors for other projects
      * will be searched as well
      *
      * @param identifier
      * @return
      */
    override def getTable(identifier: TableIdentifier): DataFrame = {
        if (identifier.project.isEmpty)
            _tables.getOrElse(identifier.name, throw new NoSuchElementException(s"Table ${identifier.name} not found in project ${_project.name}"))
        else
            _parent.getTable(identifier)
    }
}

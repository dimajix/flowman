package com.dimajix.dataflow.execution

import scala.collection.mutable

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.broadcast
import org.slf4j.LoggerFactory

import com.dimajix.dataflow.spec.Dataflow


class Executor(val context:Context, val dataflow:Dataflow) {
    private val logger = LoggerFactory.getLogger(classOf[Executor])

    private implicit val _context = context
    private val transforms = dataflow.transforms

    /**
      * Instantiates a table and recursively all its dependencies
      *
      * @param tableName
      * @return
      */
    private def createTable(tableName: String): DataFrame = {
        logger.info("Creating instance of table {}", tableName)
        if (!transforms.contains(tableName)) {
            logger.error("Table {} not found", tableName)
            throw new NoSuchElementException(s"Table $tableName not found")
        }

        // Lookup table definition
        val transform = transforms(tableName)

        // Ensure all dependencies are instantiated
        logger.info("Ensuring dependencies for table {}", tableName)
        val dependencies = transform.dependencies
        dependencies.foreach(instantiate)

        // Process table and register result as temp table
        logger.info("Instantiating table {}", tableName)
        val instance = transform.execute.persist(transform.cache)
        val df = if (transform.broadcast)
            broadcast(instance)
        else
            instance

        context.putTable(tableName, df)
        df
    }

    /**
      * Creates an instance of a table of a Dataflow, or retrieves it from cache
      *
      * @param tableName
      */
    def instantiate(tableName: String) : DataFrame = {
        context.getTable(tableName, createTable(tableName))
    }
}

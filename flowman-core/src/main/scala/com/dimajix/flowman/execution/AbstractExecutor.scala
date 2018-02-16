package com.dimajix.flowman.execution

import scala.collection.mutable

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.broadcast
import org.slf4j.LoggerFactory

import com.dimajix.flowman.spec.TableIdentifier


abstract class AbstractExecutor(_context:Context) extends Executor {
    /**
      * Returns the Context associated with this Executor. This context will be used for looking up
      * databases and relations and for variable substition
      *
      * @return
      */
    override def context: Context = _context
}

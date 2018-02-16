package com.dimajix.flowman.spec.output

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import org.apache.spark.sql.DataFrame

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.TableIdentifier


@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = Array(
    new JsonSubTypes.Type(name = "blackhole", value = classOf[BlackholeOutput]),
    new JsonSubTypes.Type(name = "count", value = classOf[CountOutput]),
    new JsonSubTypes.Type(name = "dump", value = classOf[DumpOutput]),
    new JsonSubTypes.Type(name = "file", value = classOf[FileOutput]),
    new JsonSubTypes.Type(name = "generic", value = classOf[RelationOutput]))
)
abstract class Output {
    def enabled(implicit context:Context) : Boolean

    /**
      * Returns the dependencies of this mapping, which is exactly one input table
      *
      * @param context
      * @return
      */
    def dependencies(implicit context: Context) : Array[TableIdentifier]

    /**
      * Abstract method which will perform the output operation. All required tables need to be
      * registered as temporary tables in the Spark session before calling the execute method.
      *
      * @param executor
      */
    def execute(executor:Executor, input:Map[TableIdentifier,DataFrame]) : Unit
}

package com.dimajix.flowman.spec.output

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.TableIdentifier


class FileOutput extends RelationOutput {
    private val logger = LoggerFactory.getLogger(classOf[FileOutput])

    @JsonProperty(value="directory", required=true) private[spec] var _directory:String = _
    @JsonProperty(value="pattern", required=true) private[spec] var _pattern:String = _

    def directory(implicit context: Context) : String = context.evaluate(_directory)
    def pattern(implicit context: Context) : String = context.evaluate(_pattern)


    override def execute(executor:Executor, input:Map[TableIdentifier,DataFrame]) = {
        implicit var context = executor.context
//        val datetime = DateTimeFormatter.ofPattern(pattern).format(context.startDate)
//        val path = new File(directory, datetime).toString
//
//        logger.info("Writing file output with format {} at {}", format:Any, path.toString:Any)
//
//        val df = context.getTable(input).coalesce(parallelism)
//        val writer = this.writer(context, df)
//        writer.save(path)
    }
}

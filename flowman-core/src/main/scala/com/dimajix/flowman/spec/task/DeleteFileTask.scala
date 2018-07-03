package com.dimajix.flowman.spec.task

import com.fasterxml.jackson.annotation.JsonProperty
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.fs.FileSystem


class DeleteFileTask extends BaseTask {
    private val logger = LoggerFactory.getLogger(classOf[DeleteFileTask])

    @JsonProperty(value="path", required=true) private var _path:String = ""
    @JsonProperty(value="recursive", required=false) private var _recusrive:String = "false"

    def path(implicit context:Context) : String = context.evaluate(_path)
    def recursive(implicit context:Context) : Boolean = context.evaluate(_recusrive).toBoolean

    override def execute(executor:Executor) : Boolean = {
        implicit val context = executor.context
        val fs = new FileSystem(executor.hadoopConf)
        val file = fs.file(path)
        logger.info(s"Deleting remote file '$file' (recursive=$recursive)")
        file.delete(recursive)
        true
    }
}

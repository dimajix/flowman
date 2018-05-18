package com.dimajix.flowman.spec.task

import com.fasterxml.jackson.annotation.JsonProperty
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.JobIdentifier


class CallTask extends BaseTask {
    private val logger = LoggerFactory.getLogger(classOf[CallTask])

    @JsonProperty(value="job", required=true) private var _job:String = ""
    @JsonProperty(value="force") private var _force:String = "false"
    @JsonProperty(value="args", required=false) private var _args:Map[String,String] = Map()

    def this(job:String, args:Map[String,String], force:String="false") = {
        this()
        _job = job
        _args = args
        _force = force
    }

    def job(implicit context:Context) : JobIdentifier = JobIdentifier.parse(context.evaluate(_job))
    def args(implicit context:Context) : Map[String,String] = _args.mapValues(context.evaluate)
    def force(implicit context: Context) : Boolean = context.evaluate(_force).toBoolean

    override def execute(executor:Executor) : Boolean = {
        executeJob(executor)
    }

    private def executeJob(executor: Executor) : Boolean = {
        implicit val context = executor.context
        logger.info(s"Running job '${this.job}' with args ${args.map(kv => kv._1 + "=" + kv._2).mkString(", ")}")

        val job = context.getJob(this.job)
        context.runner.execute(executor, job, args, force) match {
            case JobStatus.SUCCESS => true
            case JobStatus.SKIPPED => true
            case _ => false
        }
    }
}

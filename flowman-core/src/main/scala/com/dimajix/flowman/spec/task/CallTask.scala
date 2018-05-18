package com.dimajix.flowman.spec.task

import com.fasterxml.jackson.annotation.JsonProperty
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.JobIdentifier


class CallTask extends BaseTask {
    private val logger = LoggerFactory.getLogger(classOf[CallTask])

    @JsonProperty(value="job") private var _job:String = _
    @JsonProperty(value="args", required=true) private var _args:Map[String,String] = Map()

    def this(job:String, args:Map[String,String]) = {
        this()
        _job = job
        _args = args
    }

    def job(implicit context:Context) : JobIdentifier = if (Option(_job).exists(_.nonEmpty)) JobIdentifier.parse(context.evaluate(_job)) else null
    def args(implicit context:Context) : Map[String,String] = _args.mapValues(context.evaluate)

    override def execute(executor:Executor) : Boolean = {
        executeJob(executor)
    }

    private def executeJob(executor: Executor) : Boolean = {
        implicit val context = executor.context
        logger.info(s"Running job: '${this.job}'")

        val job = context.getJob(this.job)
        context.runner.execute(executor, job, args) match {
            case JobStatus.SUCCESS => true
            case JobStatus.SKIPPED => true
            case _ => false
        }
    }
}

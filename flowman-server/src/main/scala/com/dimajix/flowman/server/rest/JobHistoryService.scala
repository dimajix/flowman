/*
 * Copyright 2019-2021 Kaya Kupferschmidt
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dimajix.flowman.server.rest

import java.util.Locale

import scala.language.postfixOps

import akka.http.scaladsl.server
import akka.http.scaladsl.server.Route
import io.swagger.annotations.Api
import io.swagger.annotations.ApiImplicitParam
import io.swagger.annotations.ApiImplicitParams
import io.swagger.annotations.ApiOperation
import io.swagger.annotations.ApiResponse
import io.swagger.annotations.ApiResponses
import javax.ws.rs.Path

import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.history.JobColumn
import com.dimajix.flowman.history.JobOrder
import com.dimajix.flowman.history.JobQuery
import com.dimajix.flowman.history.StateStore
import com.dimajix.flowman.server.model
import com.dimajix.flowman.server.model.Converter
import com.dimajix.flowman.server.model.JobEnvironment
import com.dimajix.flowman.server.model.JobStateCounts
import com.dimajix.flowman.server.model.JobStateList


@Api(value = "/history", produces = "application/json", consumes = "application/json")
@Path("/history")
class JobHistoryService(history:StateStore) {
    import akka.http.scaladsl.server.Directives._

    import com.dimajix.flowman.server.model.JsonSupport._

    def routes : Route = (
        pathPrefix("job") {(
            pathPrefix(Segment) { job => (
                pathEnd {
                    getJobState(job)
                }
                ~
                path("env") {
                    getJobEnvironment(job)
                }
            )}
        )}
        ~
        pathPrefix("job-counts") {(
            pathEnd {
                parameters(('project.?, 'job.?, 'phase.?, 'status.?, 'grouping)) { (project,job,phase,status,grouping) =>
                    countJobs(project, job, phase, status, grouping)
                }
            }
        )}
        ~
        pathPrefix("jobs") {(
            pathEnd {
                parameters(('project.?, 'job.?, 'phase.?, 'status.?, 'limit.as[Int].?, 'offset.as[Int].?)) { (project,job,phase,status,limit,offset) =>
                    listJobStates(project, job, phase, status, limit, offset)
                }
            }
        )}
    )

    @Path("/jobs")
    @ApiOperation(value = "Retrieve general information about multiple job runs", nickname = "listJobStates", httpMethod = "GET")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "project", value = "Project name", required = false,
            dataType = "string", paramType = "query"),
        new ApiImplicitParam(name = "job", value = "Job name", required = false,
            dataType = "string", paramType = "query"),
        new ApiImplicitParam(name = "phase", value = "Execution phase", required = false,
            dataType = "string", paramType = "query"),
        new ApiImplicitParam(name = "status", value = "Execution status", required = false,
            dataType = "string", paramType = "query"),
        new ApiImplicitParam(name = "limit", value = "Maximum number of entries to return", required = false,
            dataType = "int", paramType = "query"),
        new ApiImplicitParam(name = "offset", value = "Starting offset of entries to return", required = false,
            dataType = "int", paramType = "query")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Job information", response = classOf[model.JobStateList])
    ))
    def listJobStates(project:Option[String], job:Option[String], phase:Option[String], status:Option[String], limit:Option[Int], offset:Option[Int]) : server.Route = {
        val query = JobQuery(
            project=split(project),
            job=split(job),
            phase=split(phase).map(Phase.ofString),
            status=split(status).map(Status.ofString)
        )
        val jobs = history.findJobs(query, Seq(JobOrder.BY_DATETIME.desc()), limit.getOrElse(1000), offset.getOrElse(0))
        val count = history.countJobs(query)
        complete(JobStateList(jobs.map(j => Converter.ofSpec(j)),count))
    }

    @Path("/job-counts")
    @ApiOperation(value = "Retrieve grouped job counts", nickname = "countJobs", httpMethod = "GET")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "project", value = "Project name", required = false,
            dataType = "string", paramType = "query"),
        new ApiImplicitParam(name = "job", value = "Job name", required = false,
            dataType = "string", paramType = "query"),
        new ApiImplicitParam(name = "phase", value = "Execution phase", required = false,
            dataType = "string", paramType = "query"),
        new ApiImplicitParam(name = "status", value = "Execution status", required = false,
            dataType = "string", paramType = "query"),
        new ApiImplicitParam(name = "grouping", value = "Grouping attribute", required = true,
            dataType = "int", paramType = "query")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Job information", response = classOf[model.JobStateList])
    ))
    def countJobs(project:Option[String], job:Option[String], phase:Option[String], status:Option[String], grouping:String) : server.Route = {
        val query = JobQuery(
            project=split(project),
            job=split(job),
            phase=split(phase).map(Phase.ofString),
            status=split(status).map(Status.ofString)
        )
        val g = grouping.toLowerCase(Locale.ROOT) match {
            case "project" => JobColumn.PROJECT
            case "job" => JobColumn.NAME
            case "name" => JobColumn.NAME
            case "status" => JobColumn.STATUS
            case "phase" => JobColumn.PHASE
        }
        val count = history.countJobs(query, g)
        complete(JobStateCounts(count))
    }

    @Path("/job")
    @ApiOperation(value = "Retrieve general information about a job run", nickname = "getJobState", httpMethod = "GET")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "job", value = "Job ID", required = true,
            dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Job information", response = classOf[model.JobState])
    ))
    def getJobState(jobId:String) : server.Route = {
        val query = JobQuery(id=Seq(jobId))
        val job = history.findJobs(query).headOption
        complete(job.map { j =>
            val metrics = history.getJobMetrics(j.id)
            Converter.ofSpec(j, metrics)
        })
    }

    @Path("/job/{job}/env")
    @ApiOperation(value = "Retrieve execution environment of a job run", nickname = "getJobEnvironment", httpMethod = "GET")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "job", value = "Job ID", required = true,
            dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Job environment", response = classOf[model.JobEnvironment])
    ))
    def getJobEnvironment(jobId:String) : server.Route = {
        complete{
            val env = history.getJobEnvironment(jobId)
            JobEnvironment(env)
        }
    }

    private def split(arg:Option[String]) : Seq[String] = {
        arg.toSeq.flatMap(_.split(',')).map(_.trim).filter(_.nonEmpty)
    }
}

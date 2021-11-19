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
import com.dimajix.flowman.history.JobOrder
import com.dimajix.flowman.history.JobQuery
import com.dimajix.flowman.history.StateStore
import com.dimajix.flowman.server.model
import com.dimajix.flowman.server.model.Converter


@Api(value = "/history", produces = "application/json", consumes = "application/json")
@Path("/history")
class JobHistoryService(history:StateStore) {
    import akka.http.scaladsl.server.Directives._
    import com.dimajix.flowman.server.model.JsonSupport._

    def routes : Route = (
        pathPrefix("job") {(
            path(Segment) { job =>
                getJobState(job)
            }
        )}
        ~
        pathPrefix("jobs") {(
            pathEnd {
                parameters(('project.?, 'job.?, 'phase.?, 'status.?)) { (project,job,phase,status) =>
                    listJobStates(project, job, phase, status)
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
            dataType = "string", paramType = "query")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Job information", response = classOf[model.JobState])
    ))
    def listJobStates(project:Option[String], job:Option[String], phase:Option[String], status:Option[String]) : server.Route = {
        val query = JobQuery(
            project=project, job=job,
            phase=phase.map(Phase.ofString),
            status=status.map(Status.ofString)
        )
        val jobs = history.findJobStates(query, Seq(JobOrder.BY_DATETIME.desc()), 1000, 0)
        complete(jobs.map(j => Converter.ofSpec(j)))
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
        val query = JobQuery(id=Some(jobId))
        val job = history.findJobStates(query).headOption
        complete(job.map { j =>
            val metrics = history.getJobMetrics(j.id)
            Converter.ofSpec(j, metrics)
        })
    }
}

/*
 * Copyright (C) 2019 The Flowman Authors
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

import io.swagger.annotations.Api
import io.swagger.annotations.ApiImplicitParam
import io.swagger.annotations.ApiImplicitParams
import io.swagger.annotations.ApiOperation
import io.swagger.annotations.ApiParam
import io.swagger.annotations.ApiResponse
import io.swagger.annotations.ApiResponses
import javax.inject.Inject
import javax.ws.rs.DefaultValue
import javax.ws.rs.GET
import javax.ws.rs.Path
import javax.ws.rs.PathParam
import javax.ws.rs.Produces
import javax.ws.rs.QueryParam
import javax.ws.rs.core.MediaType
import javax.ws.rs.core.Response

import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.history.JobColumn
import com.dimajix.flowman.history.JobOrder
import com.dimajix.flowman.history.JobQuery
import com.dimajix.flowman.server.model
import com.dimajix.flowman.server.model.Converter
import com.dimajix.flowman.server.model.JobEnvironment
import com.dimajix.flowman.server.model.JobStateCounts
import com.dimajix.flowman.server.model.JobStateList
import com.dimajix.flowman.spec.history.StateRepository


@Api(value = "jobs", produces = "application/json", consumes = "application/json")
@Path("/history")
class JobHistoryService @Inject()(history:StateRepository) {
    @Path("/jobs")
    @GET
    @Produces(Array(MediaType.APPLICATION_JSON))
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
    def listJobStates(
        @ApiParam(hidden = true) @QueryParam("project") project: Option[String],
        @ApiParam(hidden = true) @QueryParam("job") job: Option[String],
        @ApiParam(hidden = true) @QueryParam("phase") phase: Option[String],
        @ApiParam(hidden = true) @QueryParam("status") status: Option[String],
        @ApiParam(hidden = true) @QueryParam("limit") @DefaultValue("1000") limit: Int,
        @ApiParam(hidden = true) @QueryParam("offset") @DefaultValue("0") offset: Int
    ) : JobStateList  = {
        val query = JobQuery(
            project = split(project),
            job = split(job),
            phase = split(phase).map(Phase.ofString),
            status = split(status).map(Status.ofString)
        )
        val jobs = history.findJobs(query, Seq(JobOrder.BY_DATETIME.desc()), limit, offset)
        val count = history.countJobs(query)
        JobStateList(jobs.map(j => Converter.ofSpec(j)), count)
    }

    @Path("/job-counts")
    @GET
    @Produces(Array(MediaType.APPLICATION_JSON))
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
            dataType = "string", paramType = "query")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Job information", response = classOf[model.JobStateList])
    ))
    def countJobs(
        @ApiParam(hidden = true) @QueryParam("project") project:Option[String],
        @ApiParam(hidden = true) @QueryParam("job") job:Option[String],
        @ApiParam(hidden = true) @QueryParam("phase") phase:Option[String],
        @ApiParam(hidden = true) @QueryParam("status") status:Option[String],
        @ApiParam(hidden = true) @QueryParam("grouping") grouping:String
    ) : JobStateCounts = {
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
        JobStateCounts(count.toMap)
    }

    @Path("/job/{job}")
    @GET
    @Produces(Array(MediaType.APPLICATION_JSON))
    @ApiOperation(value = "Retrieve general information about a job run", nickname = "getJobState", httpMethod = "GET")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "job", value = "Job ID", required = true,
            dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Job information", response = classOf[model.JobState]),
        new ApiResponse(code = 404, message = "Job not found")
    ))
    def getJobState(
        @ApiParam(hidden = true) @PathParam("job") jobId:String
    ) : Response = {
        val query = JobQuery(id=Seq(jobId))
        val job = history.findJobs(query).headOption
        job.map { j =>
            val metrics = history.getJobMetrics(j.id)
            Response.ok(Converter.ofSpec(j, metrics)).build()
        }.getOrElse(Response.status(Response.Status.NOT_FOUND).build())
    }

    @Path("/job/{job}/env")
    @GET
    @Produces(Array(MediaType.APPLICATION_JSON))
    @ApiOperation(value = "Retrieve execution environment of a job run", nickname = "getJobEnvironment", httpMethod = "GET")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "job", value = "Job ID", required = true,
            dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Job environment", response = classOf[model.JobEnvironment]),
        new ApiResponse(code = 404, message = "Job not found")
    ))
    def getJobEnvironment(
        @ApiParam(hidden = true) @PathParam("job") jobId:String
    ) : Response = {
        try {
            val env = history.getJobEnvironment(jobId)
            Response.ok(JobEnvironment(env)).build()
        }
        catch {
            case _:NoSuchElementException => Response.status(Response.Status.NOT_FOUND).build()
        }
    }

    @Path("/job/{job}/documentation")
    @GET
    @Produces(Array(MediaType.APPLICATION_JSON))
    @ApiOperation(value = "Retrieve documentation of a job run", nickname = "getJobDocumentation", httpMethod = "GET")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "job", value = "Job ID", required = true,
            dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Job environment", response = classOf[model.ProjectDocumentation]),
        new ApiResponse(code = 404, message = "Job not found")
    ))
    def getJobDocumentation(
        @ApiParam(hidden = true) @PathParam("job") jobId:String
    ) : Response = {
        try {
            val doc = history.getJobDocumentation(jobId).getOrElse(throw new NoSuchElementException())
            Response.ok(Converter.ofSpec(doc)).build()
        }
        catch {
            case _: NoSuchElementException => Response.status(Response.Status.NOT_FOUND).build()
        }
    }

    private def split(arg:Option[String]) : Seq[String] = {
        arg.toSeq.flatMap(_.split(',')).map(_.trim).filter(_.nonEmpty)
    }
}

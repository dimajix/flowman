/*
 * Copyright 2021-2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.studio.rest.session

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import io.swagger.annotations.Api
import io.swagger.annotations.ApiImplicitParam
import io.swagger.annotations.ApiImplicitParams
import io.swagger.annotations.ApiOperation
import io.swagger.annotations.ApiParam
import io.swagger.annotations.ApiResponse
import io.swagger.annotations.ApiResponses
import javax.ws.rs.GET
import javax.ws.rs.POST
import javax.ws.rs.Path

import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.studio.model.Converter
import com.dimajix.flowman.studio.model
import com.dimajix.flowman.studio.service.SessionService


@Api(value = "session", produces = "application/json", consumes = "application/json")
@Path("/session/{session}/job")
@ApiImplicitParams(Array(
    new ApiImplicitParam(name = "session", value = "Session ID", required = true, dataType = "string", paramType = "path")
))
@ApiResponses(Array(
    new ApiResponse(code = 404, message = "Session or job not found"),
    new ApiResponse(code = 500, message = "Internal server error")
))
class JobEndpoint {
    import akka.http.scaladsl.server.Directives._

    import com.dimajix.flowman.studio.model.JsonSupport._

    def routes(session:SessionService) : server.Route = pathPrefix("job") {(
        pathEndOrSingleSlash {
            redirectToNoTrailingSlashIfPresent(StatusCodes.Found) {
                listJobs(session)
            }
        }
        ~
        pathPrefix(Segment) { jobName =>
            withJob(session, jobName) { job => (
                pathEndOrSingleSlash {
                    redirectToNoTrailingSlashIfPresent(StatusCodes.Found) {
                        getJob(job)
                    }
                }
                ~
                path("run") {
                    runJob(session, job)
                }
            )}
        }
    )}

    @GET
    @ApiOperation(value = "Return list of all jobs", nickname = "listJobs", httpMethod = "GET")
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "List of all jobs", response = classOf[model.JobList])
    ))
    def listJobs(@ApiParam(hidden = true) session: SessionService) : server.Route = {
        get {
            val result = model.JobList(session.listJobs())
            complete(result)
        }
    }

    @GET
    @Path("/{job}")
    @ApiOperation(value = "Get job", nickname = "getJob", httpMethod = "GET")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "job", value = "Job Name", required = true, dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Information about the job", response = classOf[model.Job])
    ))
    def getJob(@ApiParam(hidden = true) job:Job) : server.Route = {
        get {
            complete(Converter.of(job))
        }
    }

    @POST
    @Path("/{job}/run")
    @ApiOperation(value = "Run job", nickname = "runJob", httpMethod = "POST")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "job", value = "Job Name", required = true, dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Job run", response = classOf[model.JobTask])
    ))
    def runJob(@ApiParam(hidden = true) session:SessionService, @ApiParam(hidden = true) job:Job) : server.Route = {
        post {
            entity(as[model.RunJobRequest]) { jobRun =>
                val phase = Phase.ofString(jobRun.phase)
                val run = session.tasks.runJob(job, phase, jobRun.args, jobRun.force, jobRun.keepGoing, jobRun.dryRun)
                complete(Converter.of(run))
            }
        }
    }

    private def withJob(session:SessionService, jobName:String)(fn:(Job) => server.Route) : server.Route = {
        Try {
            session.getJob(jobName)
        } match {
            case Success(job) => fn(job)
            case Failure(_) => complete(StatusCodes.NotFound -> s"Job '$jobName' not found")
        }
    }
}

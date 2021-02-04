/*
 * Copyright 2019 Kaya Kupferschmidt
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

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Route
import io.swagger.annotations.Api
import io.swagger.annotations.ApiImplicitParam
import io.swagger.annotations.ApiImplicitParams
import io.swagger.annotations.ApiOperation
import io.swagger.annotations.ApiParam
import io.swagger.annotations.ApiResponse
import io.swagger.annotations.ApiResponses
import javax.ws.rs.Path
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.NoSuchJobException
import com.dimajix.flowman.execution.NoSuchProjectException
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.JobIdentifier
import com.dimajix.flowman.server.model
import com.dimajix.flowman.server.model.Converter
import com.dimajix.flowman.server.model.Job
import com.dimajix.flowman.server.model.Project
import com.dimajix.flowman.storage.Store


@Api(value = "/project", produces = "application/json", consumes = "application/json")
@Path("/project")
class ProjectService(store:Store) {
    private val logger = LoggerFactory.getLogger(classOf[ProjectService])

    import akka.http.scaladsl.server.Directives._
    import com.dimajix.flowman.server.model.JsonSupport._

    def routes : Route = pathPrefix("project") {(
        pathEndOrSingleSlash {
            listProjects()
        }
        ~
        pathPrefix(Segment) { project => (
            pathEndOrSingleSlash {
                infoProject(project)
            }
            ~
            pathPrefix("job") {(
                pathEndOrSingleSlash {
                    listJobs(project)
                }
                ~
                pathPrefix(Segment) { job => (
                    pathEndOrSingleSlash {
                        infoJob(project, job)
                    }
                    ~
                    path("run") {
                        runJob(project, job)
                    }
                )}
            )}
            ~
            pathPrefix("target") {(
                pathEndOrSingleSlash {
                    listTargets(project)
                }
                ~
                path(Segment) { target => (
                    pathEndOrSingleSlash {
                        infoTarget(project, target)
                    }
                    ~
                    path("build") {
                        buildTarget(project, target)
                    }
                )}
            )}
        )}
    )}

    @Path("/")
    @ApiOperation(value = "Retrieve a list of all projects", nickname = "getProjects", httpMethod = "GET")
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Project information", response = classOf[Seq[String]])
    ))
    def listProjects(): server.Route = {
        val result = store.listProjects()
        complete(result.map(Converter.ofSpec))
    }

    @Path("/{project}")
    @ApiOperation(value = "Retrieve general information about a project", nickname = "getProject", httpMethod = "GET")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "project", value = "name of project", required = true,
            dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Project information", response = classOf[model.Project])
    ))
    def infoProject(@ApiParam(hidden = true) project:String): server.Route = {
        Try {
            Converter.ofSpec(store.loadProject(project))
        }
        match {
            case Success(result) =>
                complete(result)
            case Failure(x:NoSuchProjectException) =>
                logger.error(s"Project ${x.project} not found")
                complete(StatusCodes.NotFound)
            case Failure(ex) =>
                complete(StatusCodes.InternalServerError)
        }
    }

    @Path("/{project}/job")
    @ApiOperation(value = "List all jobs within a project", nickname = "getJobs", httpMethod = "GET")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "project", value = "name of project", required = true,
            dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "List of jobs", response = classOf[model.Project])
    ))
    def listJobs(@ApiParam(hidden = true) project:String): server.Route = {
        Try {
            store.loadProject(project)
        } match {
            case Success(p) =>
                complete(p.jobs.keys.toSeq)
            case Failure(x:NoSuchProjectException) =>
                logger.error(s"Project ${x.project} not found")
                complete(StatusCodes.NotFound)
            case Failure(ex) =>
                complete(StatusCodes.InternalServerError)
        }
    }

    @Path("/{project}/job/{job}")
    @ApiOperation(value = "Retrieve information about an individual job within a project", nickname = "getJob", httpMethod = "GET")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "project", value = "name of project", required = true,
            dataType = "string", paramType = "path"),
        new ApiImplicitParam(name = "job", value = "name of job within the project", required = true,
            dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Information on job", response = classOf[model.Project])
    ))
    def infoJob(@ApiParam(hidden = true) project:String, @ApiParam(hidden = true) job:String): server.Route = {
        Try {
            val p = store.loadProject(project)
            val session = Session.builder()
                .withProject(p)
                .disableSpark()
                .build()
            val context = session.getContext(p)
            context.getJob(JobIdentifier(job))
        } match {
            case Success(j) =>
                complete(Converter.ofSpec(j))
            case Failure(x:NoSuchProjectException) =>
                logger.error(s"Project ${x.project} not found")
                complete(StatusCodes.NotFound)
            case Failure(x:NoSuchJobException) =>
                logger.error(s"Job ${x.job} not found")
                complete(StatusCodes.NotFound)
            case Failure(ex) =>
                complete(StatusCodes.InternalServerError)
        }
    }

    @Path("/{project}/job/{job}")
    @ApiOperation(value = "Run a single job within a project", nickname = "runJob", httpMethod = "POST")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "project", value = "name of project", required = true,
            dataType = "string", paramType = "path"),
        new ApiImplicitParam(name = "job", value = "name of job within the project", required = true,
            dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "List of jobs", response = classOf[model.Project])
    ))
    def runJob(@ApiParam(hidden = true) project:String, @ApiParam(hidden = true) job:String) : server.Route = {
        post {
            reject
        }
    }

    @Path("/{project}/target")
    @ApiOperation(value = "List all targets within a project", nickname = "getTargets", httpMethod = "GET")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "project", value = "name of project", required = true,
            dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "List of jobs", response = classOf[model.Project])
    ))
    def listTargets(@ApiParam(hidden = true)project:String): server.Route = {
        Try {
            store.loadProject(project)
        } match {
            case Success(p) =>
                complete(p.targets.keys.toSeq)
            case Failure(x:NoSuchProjectException) =>
                logger.error(s"Project ${x.project} not found")
                complete(StatusCodes.NotFound)
            case Failure(ex) =>
                complete(StatusCodes.InternalServerError)
        }
    }

    @Path("/{project}/target/{target}")
    @ApiOperation(value = "Retrieve information on a single target within a project", nickname = "getTarget", httpMethod = "GET")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "project", value = "name of project", required = true,
            dataType = "string", paramType = "path"),
        new ApiImplicitParam(name = "target", value = "name of target within the project", required = true,
            dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Information on target", response = classOf[model.Project])
    ))
    def infoTarget(@ApiParam(hidden = true) project:String, @ApiParam(hidden = true) target:String): server.Route = {
        reject
    }

    @Path("/{project}/target/{target}")
    @ApiOperation(value = "Build a single target within a project", nickname = "runJob", httpMethod = "POST")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "project", value = "name of project", required = true,
            dataType = "string", paramType = "path"),
        new ApiImplicitParam(name = "target", value = "name of target within the project", required = true,
            dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "List of jobs", response = classOf[model.Project])
    ))
    def buildTarget(@ApiParam(hidden = true) project:String, @ApiParam(hidden = true) target:String) : server.Route = {
        post {
            reject
        }
    }
}

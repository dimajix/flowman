package com.dimajix.flowman.server.rest

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

import com.dimajix.flowman.history.JobOrder
import com.dimajix.flowman.history.JobQuery
import com.dimajix.flowman.history.StateStore
import com.dimajix.flowman.server.model
import com.dimajix.flowman.server.model.Converter


@Api(value = "/job-history", produces = "application/json", consumes = "application/json")
@Path("/job-history")
class JobHistoryService(history:StateStore) {
    import akka.http.scaladsl.server.Directives._
    import com.dimajix.flowman.server.model.JsonSupport._

    def routes : Route = pathPrefix("job-history") {(
        pathEndOrSingleSlash {
            parameterMap { params =>
                listJobStates()
            }
        }
        ~
        pathPrefix(Segment) { project => (
            pathEndOrSingleSlash {
                parameterMap { params =>
                    listJobStates(project)
                }
            }
            ~
            pathPrefix(Segment) { job => (
                pathEndOrSingleSlash {
                    parameterMap { params =>
                        listJobStates(project, job)
                    }
                }
            )}
        )}
    )}

    @Path("/")
    @ApiOperation(value = "Retrieve general information about a job", nickname = "getAllJobStates", httpMethod = "GET")
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Job information", response = classOf[model.JobState])
    ))
    def listJobStates() : server.Route = {
        val query = JobQuery()
        val jobs = history.findJobs(query, Seq(JobOrder.BY_DATETIME.desc()), 100, 0)
        complete(jobs.map(Converter.ofSpec))
    }

    @Path("/{project}")
    @ApiOperation(value = "Retrieve general information about a job", nickname = "getAllProjectJobsStates", httpMethod = "GET")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "project", value = "Project name", required = true,
            dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Job information", response = classOf[model.JobState])
    ))
    def listJobStates(@ApiParam(hidden = true) project:String) : server.Route = {
        val query = JobQuery(project=Some(project))
        val jobs = history.findJobs(query, Seq(JobOrder.BY_DATETIME.desc()), 100, 0)
        complete(jobs.map(Converter.ofSpec))
    }

    @Path("/{project}/{job}")
    @ApiOperation(value = "Retrieve general information about a job", nickname = "getProjectJobState", httpMethod = "GET")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "project", value = "Project name", required = true,
            dataType = "string", paramType = "path"),
        new ApiImplicitParam(name = "job", value = "Job name", required = true,
            dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Job information", response = classOf[model.JobState])
    ))
    def listJobStates(@ApiParam(hidden = true) project:String,
                      @ApiParam(hidden = true) job:String) : server.Route = {
        val query = JobQuery(
            project=Some(project),
            name=Some(job)
        )
        val jobs = history.findJobs(query, Seq(JobOrder.BY_DATETIME.desc()), 100, 0)
        complete(jobs.map(Converter.ofSpec))
    }

    private def parseQuery(params:Map[String,String]) = {
        params.get("from")
        params.get("to")
        params.get("state")
        params.get("id")
        params.get("name")
        params.get("parent_name")
        params.get("parent_id")
        params.flatMap(kv => "p\\[(.+)\\]".r.unapplySeq(kv._1).flatMap(_.headOption).map(k => (k,kv._2)))
    }
}

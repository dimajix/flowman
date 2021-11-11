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
import com.dimajix.flowman.history.TargetOrder
import com.dimajix.flowman.history.TargetQuery
import com.dimajix.flowman.server.model
import com.dimajix.flowman.server.model.Converter


@Api(value = "/target-history", produces = "application/json", consumes = "application/json")
@Path("/target-history")
class TargetHistoryService(history:StateStore) {
    import akka.http.scaladsl.server.Directives._

    import com.dimajix.flowman.server.model.JsonSupport._

    def routes : Route = pathPrefix("target-history") {(
        pathEndOrSingleSlash {
            listTargetStates()
        }
        ~
        pathPrefix(Segment) { project => (
            pathEndOrSingleSlash {
                parameterMap { params =>
                    listTargetStates(project)
                }
            }
            ~
            pathPrefix(Segment) { target => (
                pathEndOrSingleSlash {
                    parameterMap { params =>
                        listTargetStates(project, target)
                    }
                }
            )}
        )}
    )}

    @Path("/")
    @ApiOperation(value = "Retrieve general information about a job", nickname = "getAllTaretStates", httpMethod = "GET")
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Target information", response = classOf[model.JobState])
    ))
    def listTargetStates() : server.Route = {
        val query = TargetQuery()
        val jobs = history.findTargetStates(query, Seq(TargetOrder.BY_DATETIME.desc()), 100, 0)
        complete(jobs.map(Converter.ofSpec))
    }

    @Path("/{project}")
    @ApiOperation(value = "Retrieve general information about a job", nickname = "getAllProjectTargetStates", httpMethod = "GET")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "project", value = "Project name", required = true,
            dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Target information", response = classOf[model.JobState])
    ))
    def listTargetStates(@ApiParam(hidden = true) project:String) : server.Route = {
        val query = TargetQuery(project=Some(project))
        val jobs = history.findTargetStates(query, Seq(TargetOrder.BY_DATETIME.desc()), 100, 0)
        complete(jobs.map(Converter.ofSpec))
    }

    @Path("/{project}/{target}")
    @ApiOperation(value = "Retrieve general information about a job", nickname = "getProjectTargetState", httpMethod = "GET")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "project", value = "Project name", required = true,
            dataType = "string", paramType = "path"),
        new ApiImplicitParam(name = "target", value = "Target name", required = true,
            dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Target information", response = classOf[model.JobState])
    ))
    def listTargetStates(@ApiParam(hidden = true) project:String,
                         @ApiParam(hidden = true) target:String) : server.Route = {
        val query = TargetQuery(
            project=Some(project),
            name=Some(target)
        )
        val jobs = history.findTargetStates(query, Seq(TargetOrder.BY_DATETIME.desc()), 100, 0)
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

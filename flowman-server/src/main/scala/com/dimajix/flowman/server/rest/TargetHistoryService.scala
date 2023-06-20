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

import scala.language.postfixOps

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

import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.graph.Category
import com.dimajix.flowman.history.Edge
import com.dimajix.flowman.history.Node
import com.dimajix.flowman.history.StateStore
import com.dimajix.flowman.history.TargetColumn
import com.dimajix.flowman.history.TargetOrder
import com.dimajix.flowman.history.TargetQuery
import com.dimajix.flowman.server.model
import com.dimajix.flowman.server.model.Converter
import com.dimajix.flowman.server.model.TargetStateCounts
import com.dimajix.flowman.server.model.TargetStateList


@Api(value = "/history", produces = "application/json", consumes = "application/json")
@Path("/history")
class TargetHistoryService(history:StateStore) {
    import akka.http.scaladsl.server.Directives._

    import com.dimajix.flowman.server.model.JsonSupport._

    def routes : Route = (
        pathPrefix("target") {(
            pathPrefix(Segment) { target => (
                pathEnd {
                    get {
                        getTargetState(target)
                    }
                }
                ~
                path("graph") {
                    get {
                        getTargetGraph(target)
                    }
                }
            )}
        )}
        ~
        pathPrefix("target-counts") {(
            pathEnd {
                get {
                    parameters(('project.?, 'job.?, 'jobId.?, 'target.?, 'phase.?, 'status.?, 'grouping)) { (project, job, jobId, target, phase, status, grouping) =>
                        countTargets(project, job, target, jobId, phase, status, grouping)
                    }
                }
            }
        )}
        ~
        pathPrefix("targets") {(
            pathEnd {
                get {
                    parameters(('project.?, 'job.?, 'jobId.?, 'target.?, 'phase.?, 'status.?, 'limit.as[Int].?, 'offset.as[Int].?)) { (project, job, jobId, target, phase, status, limit, offset) =>
                        listTargetStates(project, job, target, jobId, phase, status, limit, offset)
                    }
                }
            }
        )}
    )

    @Path("/targets")
    @ApiOperation(value = "Retrieve general information about a job", nickname = "getAllTaretStates", httpMethod = "GET")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "project", value = "Project name", required = false,
            dataType = "string", paramType = "query"),
        new ApiImplicitParam(name = "job", value = "Job name", required = false,
            dataType = "string", paramType = "query"),
        new ApiImplicitParam(name = "target", value = "Target name", required = false,
            dataType = "string", paramType = "query"),
        new ApiImplicitParam(name = "jobId", value = "Parent job id", required = false,
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
        new ApiResponse(code = 200, message = "Target information", response = classOf[model.TargetStateList])
    ))
    def listTargetStates(
        @ApiParam(hidden = true) project:Option[String],
        @ApiParam(hidden = true) job:Option[String],
        @ApiParam(hidden = true) target:Option[String],
        @ApiParam(hidden = true) jobId:Option[String],
        @ApiParam(hidden = true) phase:Option[String],
        @ApiParam(hidden = true) status:Option[String],
        @ApiParam(hidden = true) limit:Option[Int],
        @ApiParam(hidden = true) offset:Option[Int]
    ) : server.Route = {
        val query = TargetQuery(
            project=split(project),
            job=split(job),
            jobId=split(jobId),
            target=split(target),
            phase=split(phase).map(Phase.ofString),
            status=split(status).map(Status.ofString)
        )
        val targets = history.findTargets(query, Seq(TargetOrder.BY_DATETIME.desc()), limit.getOrElse(1000), offset.getOrElse(0))
        val count = history.countTargets(query)
        complete(TargetStateList(targets.map(Converter.ofSpec), count))
    }

    @Path("/target-counts")
    @ApiOperation(value = "Retrieve grouped target counts", nickname = "countTargets", httpMethod = "GET")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "project", value = "Project name", required = false,
            dataType = "string", paramType = "query"),
        new ApiImplicitParam(name = "target", value = "Target name", required = false,
            dataType = "string", paramType = "query"),
        new ApiImplicitParam(name = "job", value = "Parent job name", required = false,
            dataType = "string", paramType = "query"),
        new ApiImplicitParam(name = "jobId", value = "Parent job id", required = false,
            dataType = "string", paramType = "query"),
        new ApiImplicitParam(name = "phase", value = "Execution phase", required = false,
            dataType = "string", paramType = "query"),
        new ApiImplicitParam(name = "status", value = "Execution status", required = false,
            dataType = "string", paramType = "query"),
        new ApiImplicitParam(name = "grouping", value = "Grouping attribute", required = true,
            dataType = "string", paramType = "query")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Target information", response = classOf[model.TargetStateList])
    ))
    def countTargets(
        @ApiParam(hidden = true) project:Option[String],
        @ApiParam(hidden = true) job:Option[String],
        @ApiParam(hidden = true) target:Option[String],
        @ApiParam(hidden = true) jobId:Option[String],
        @ApiParam(hidden = true) phase:Option[String],
        @ApiParam(hidden = true) status:Option[String],
        @ApiParam(hidden = true) grouping:String
    ) : server.Route = {
        val query = TargetQuery(
            project=split(project),
            target=split(target),
            job=split(job),
            jobId=split(jobId),
            phase=split(phase).map(Phase.ofString),
            status=split(status).map(Status.ofString)
        )
        val g = grouping.toLowerCase(Locale.ROOT) match {
            case "project" => TargetColumn.PROJECT
            case "target" => TargetColumn.NAME
            case "name" => TargetColumn.NAME
            case "job" => TargetColumn.PARENT_NAME
            case "status" => TargetColumn.STATUS
            case "phase" => TargetColumn.PHASE
        }
        val count = history.countTargets(query, g)
        complete(TargetStateCounts(count))
    }

    @Path("/target")
    @ApiOperation(value = "Retrieve general information about a target run", nickname = "getTargetState", httpMethod = "GET")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "target", value = "Target ID", required = true,
            dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Target information", response = classOf[model.TargetState])
    ))
    def getTargetState(@ApiParam(hidden = true) targetId:String) : server.Route = {
        val query = TargetQuery(id=Seq(targetId))
        val target = history.findTargets(query).headOption
        complete(target.map(Converter.ofSpec))
    }

    @Path("/target/{target}/graph")
    @ApiOperation(value = "Retrieve a target graph", nickname = "getTargetGraph", httpMethod = "GET")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "target", value = "Target ID", required = true,
            dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Target graph", response = classOf[model.TargetState])
    ))
    def getTargetGraph(@ApiParam(hidden = true) targetId:String) : server.Route = {
        val state = history.getTargetState(targetId)
        val jobGraph = history.getJobGraph(state.jobId.get)
        val targetGraph = jobGraph.map { g =>
            val targetNode = g.nodes.filter(n => n.category == Category.TARGET && n.name == state.target).head
            def incomingFilter(edge:Edge) : Boolean = {
                val node = edge.input
                node.category != Category.TARGET
            }
            def outgoingFilter(edge: Edge): Boolean = {
                val node = edge.input
                node.category != Category.TARGET
            }

            g.subgraph(targetNode, incomingFilter, outgoingFilter)
        }
        complete(targetGraph.map(Converter.ofSpec))
    }

    private def split(arg:Option[String]) : Seq[String] = {
        arg.toSeq.flatMap(_.split(',')).map(_.trim).filter(_.nonEmpty)
    }
}

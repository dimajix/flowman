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
import com.dimajix.flowman.history.StateStore
import com.dimajix.flowman.history.TargetOrder
import com.dimajix.flowman.history.TargetQuery
import com.dimajix.flowman.server.model
import com.dimajix.flowman.server.model.Converter
import com.dimajix.flowman.server.model.TargetStateList


@Api(value = "/history", produces = "application/json", consumes = "application/json")
@Path("/history")
class TargetHistoryService(history:StateStore) {
    import akka.http.scaladsl.server.Directives._
    import com.dimajix.flowman.server.model.JsonSupport._

    def routes : Route = (
        pathPrefix("target") {(
            path(Segment) { target =>
                getTargetState(target)
            }
            )}
            ~
            pathPrefix("targets") {(
                pathEnd {
                    parameters(('project.?, 'job.?, 'target.?, 'phase.?, 'status.?, 'limit.as[Int].?, 'offset.as[Int].?)) { (project,job,target,phase,status,limit,offset) =>
                        listTargetStates(project, job, target, phase, status,limit,offset)
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
    def listTargetStates(project:Option[String], job:Option[String], target:Option[String], phase:Option[String], status:Option[String], limit:Option[Int], offset:Option[Int]) : server.Route = {
        val query = TargetQuery(
            project=project,
            job=job,
            target=target,
            phase=phase.map(Phase.ofString),
            status=status.map(Status.ofString)
        )
        val targets = history.findTargetStates(query, Seq(TargetOrder.BY_DATETIME.desc()), limit.getOrElse(1000), offset.getOrElse(0))
        val count = history.countTargetStates(query)
        complete(TargetStateList(targets.map(Converter.ofSpec), count))
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
    def getTargetState(targetId:String) : server.Route = {
        val query = TargetQuery(id=Some(targetId))
        val target = history.findTargetStates(query).headOption
        complete(target.map(Converter.ofSpec))
    }
}

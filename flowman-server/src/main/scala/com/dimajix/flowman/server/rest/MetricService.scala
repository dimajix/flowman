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
import com.dimajix.flowman.history.JobQuery
import com.dimajix.flowman.history.StateStore
import com.dimajix.flowman.server.model
import com.dimajix.flowman.server.model.Converter
import com.dimajix.flowman.server.model.MetricSeriesList


@Api(value = "/history", produces = "application/json", consumes = "application/json")
@Path("/history")
class MetricService(history:StateStore) {
    import akka.http.scaladsl.server.Directives._

    import com.dimajix.flowman.server.model.JsonSupport._

    def routes : Route = (
        pathPrefix("metrics") {(
            pathEnd {
                parameters(('project.?, 'job.?, 'phase.?, 'status.?, 'grouping?)) { (project,job,phase,status,grouping) =>
                    findJobMetrics(project, job, phase, status, grouping)
                }
            }
        )}
    )

    @Path("/metrics")
    @ApiOperation(value = "Retrieve metrics for jobs", nickname = "findJobMetrics", httpMethod = "GET")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "project", value = "Project name", required = false,
            dataType = "string", paramType = "query"),
        new ApiImplicitParam(name = "job", value = "Job name", required = false,
            dataType = "string", paramType = "query"),
        new ApiImplicitParam(name = "phase", value = "Execution phase", required = false,
            dataType = "string", paramType = "query"),
        new ApiImplicitParam(name = "status", value = "Execution status", required = false,
            dataType = "string", paramType = "query"),
        new ApiImplicitParam(name = "grouping", value = "Grouping attributes", required = false,
            dataType = "string", paramType = "query")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Job metrics", response = classOf[model.MetricSeriesList])
    ))
    def findJobMetrics(project:Option[String], job:Option[String], phase:Option[String], status:Option[String], grouping:Option[String]) : server.Route = {
        val query = JobQuery(
            project=split(project),
            job=split(job),
            phase=split(phase).map(Phase.ofString),
            status=split(status).map(Status.ofString)
        )
        val metrics = history.findJobMetrics(query, split(grouping))
        complete(MetricSeriesList(metrics.map(j => Converter.ofSpec(j))))
    }

    private def split(arg:Option[String]) : Seq[String] = {
        arg.toSeq.flatMap(_.split(',')).map(_.trim).filter(_.nonEmpty)
    }
}

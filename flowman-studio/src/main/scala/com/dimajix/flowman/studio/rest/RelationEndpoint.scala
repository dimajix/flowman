/*
 * Copyright 2021 Kaya Kupferschmidt
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

package com.dimajix.flowman.studio.rest

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import akka.http.scaladsl.model.HttpResponse
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
import javax.ws.rs.Path

import com.dimajix.flowman.model
import com.dimajix.flowman.studio.model.Converter
import com.dimajix.flowman.studio.model.Relation
import com.dimajix.flowman.studio.model.RelationList
import com.dimajix.flowman.studio.service.SessionService


@Api(value = "/session/{session}/relation", produces = "application/json", consumes = "application/json")
@Path("/session/{session}/relation")
@ApiImplicitParams(Array(
    new ApiImplicitParam(name = "session", value = "Session ID", required = true, dataType = "string", paramType = "path")
))
@ApiResponses(Array(
    new ApiResponse(code = 404, message = "Session or relation not found"),
    new ApiResponse(code = 500, message = "Internal server error")
))
class RelationEndpoint {
    import akka.http.scaladsl.server.Directives._

    import com.dimajix.flowman.studio.model.JsonSupport._

    def routes(session:SessionService) : server.Route = pathPrefix("relation") {(
        pathEndOrSingleSlash {
            redirectToNoTrailingSlashIfPresent(StatusCodes.Found) {
                listRelations(session)
            }
        }
        ~
        pathPrefix(Segment) { relationName => (
            pathEndOrSingleSlash {
                redirectToNoTrailingSlashIfPresent(StatusCodes.Found) {
                    getRelation(session, relationName)
                }
            })
        }
    )}

    @GET
    @ApiOperation(value = "Return list of all jobs", nickname = "listRelations", httpMethod = "GET")
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "List of all relations", response = classOf[RelationList])
    ))
    def listRelations(@ApiParam(hidden = true) session: SessionService) : server.Route = {
        get {
            val result = RelationList(
                session.listRelations()
            )
            complete(result)
        }
    }

    @GET
    @Path("/{relation}")
    @ApiOperation(value = "Get relation", nickname = "getRelation", httpMethod = "GET")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "relation", value = "Relation Name", required = true, dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Information about the relation", response = classOf[Relation])
    ))
    def getRelation(@ApiParam(hidden = true) session: SessionService, @ApiParam(hidden = true) relation:String) : server.Route = {
        get {
            withRelation(session, relation) { relation =>
                complete(Converter.of(relation))
            }
        }
    }


    private def withRelation(session:SessionService, relationName:String)(fn:(model.Relation) => server.Route) : server.Route = {
        Try {
            session.getRelation(relationName)
        } match {
            case Success(relation) => fn(relation)
            case Failure(_) => complete(HttpResponse(status = StatusCodes.NotFound))
        }
    }
}

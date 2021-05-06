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

package com.dimajix.flowman.kernel.rest

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives.complete
import akka.http.scaladsl.server.Directives.pathEndOrSingleSlash
import akka.http.scaladsl.server.Directives.pathPrefix
import io.swagger.annotations.Api
import javax.ws.rs.Path

import com.dimajix.flowman.kernel.service.SessionService
import com.dimajix.flowman.model.Relation


@Api(value = "relation", produces = "application/json", consumes = "application/json")
@Path("/session/{session}/relation")
class RelationEndpoint {
    def routes(session:SessionService) : server.Route = pathPrefix("relation") {(
        pathEndOrSingleSlash {(
            ???
            )}
        )}

    private def withRelation(session:SessionService, relationId:String)(fn:(Relation) => server.Route) : server.Route = {
        Try {
            session.getRelation(relationId)
        }
        match {
            case Success(relation) => fn(relation)
            case Failure(_) => complete(HttpResponse(status = StatusCodes.NotFound))
        }
    }
}

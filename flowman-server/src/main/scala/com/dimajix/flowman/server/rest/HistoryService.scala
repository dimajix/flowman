package com.dimajix.flowman.server.rest

import akka.http.scaladsl.server.Route

import com.dimajix.flowman.history.StateStore


class HistoryService(history:StateStore) {
    import akka.http.scaladsl.server.Directives._

    def routes : Route = pathPrefix("history") {(
        pathEndOrSingleSlash {
            reject
        }
        ~
        pathPrefix(Segment) { project => (
            pathPrefix("job") {(
                pathEndOrSingleSlash {
                    parameterMap { params =>
                        reject
                    }
                }
                ~
                pathPrefix(Segment) { job => (
                    pathEndOrSingleSlash {
                        parameterMap { params =>
                            reject
                        }
                    }
                )}
            )}
            ~
            pathPrefix("target") {(
                pathEndOrSingleSlash {
                    reject
                }
                ~
                pathPrefix(Segment) { target => (
                    pathEndOrSingleSlash {
                        parameterMap { params =>
                            reject
                        }
                    }
                )}
            )}
        )}
    )}

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

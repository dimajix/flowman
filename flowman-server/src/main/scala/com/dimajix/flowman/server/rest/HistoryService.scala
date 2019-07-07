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
            pathEndOrSingleSlash {
                reject
            }
            ~
            pathPrefix("job") {(
                pathEndOrSingleSlash {
                    reject
                }
                ~
                pathPrefix(Segment) { job => (
                    pathEndOrSingleSlash {
                        reject
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
                        reject
                    }
                )}
            )}
        )}
    )}
}

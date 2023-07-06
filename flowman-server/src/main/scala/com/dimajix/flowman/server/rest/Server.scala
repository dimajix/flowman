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

import java.net.URI

import io.swagger.jaxrs.config.BeanConfig
import io.swagger.jaxrs.listing.ApiListingResource
import io.swagger.jaxrs.listing.SwaggerSerializers
import org.eclipse.jetty.server.CustomRequestLog
import org.eclipse.jetty.server.Slf4jRequestLogWriter
import org.eclipse.jetty.server.handler.HandlerCollection
import org.eclipse.jetty.server.handler.RequestLogHandler
import org.eclipse.jetty.servlet.DefaultServlet
import org.eclipse.jetty.servlet.ServletContextHandler
import org.eclipse.jetty.servlet.ServletHolder
import org.glassfish.hk2.utilities.binding.AbstractBinder
import org.glassfish.jersey.jetty.JettyHttpContainerFactory
import org.glassfish.jersey.server.ResourceConfig
import org.glassfish.jersey.servlet.ServletContainer
import org.slf4j.LoggerFactory

import com.dimajix.common.Resources
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.history.JobQuery
import com.dimajix.flowman.model.Namespace
import com.dimajix.flowman.server.Configuration
import com.dimajix.flowman.server.config.JacksonConfig
import com.dimajix.flowman.server.config.OptionParamConverterProvider
import com.dimajix.flowman.spec.history.RepositoryStateStore
import com.dimajix.flowman.spec.history.StateRepository


class Server(
    conf:Configuration,
    session:Session
) {
    private val logger = LoggerFactory.getLogger(classOf[Server])

    private val repository = session.history match {
        case store: RepositoryStateStore => store.repository
        case _ => throw new IllegalArgumentException("Unsupported history backend")
    }

    def run(): Unit = {
        // Ensure that repository actually exists
        repository.create()

        logger.info("Connecting to history backend")
        session.history.countJobs(JobQuery())

        logger.info("Starting http server")
        initSwagger()

        val binder = new AbstractBinder {
            override def configure(): Unit = {
                bind(session.namespace.get).to(classOf[Namespace])
                bind(repository).to(classOf[StateRepository])
            }
        }

        val apiResources = new ResourceConfig()
            .registerInstances(binder)
            .register(classOf[OptionParamConverterProvider])
            .register(classOf[JacksonConfig])
            .register(classOf[JobHistoryService])
            .register(classOf[MetricService])
            .register(classOf[NamespaceService])
            .register(classOf[TargetHistoryService])
            .register(classOf[ApiListingResource])
            .register(classOf[SwaggerSerializers])

        val handler = new ServletContextHandler(ServletContextHandler.NO_SESSIONS)

        val apiServlet = new ServletHolder(new ServletContainer(apiResources))
        handler.addServlet(apiServlet, "/api/*")

        val swaggerIndexServlet = new ServletHolder(new DefaultServlet())
        swaggerIndexServlet.setInitParameter("resourceBase", Resources.getURL("swagger/index.html").toString)
        swaggerIndexServlet.setInitParameter("dirAllowed", "true")
        swaggerIndexServlet.setInitParameter("pathInfoOnly", "true")
        swaggerIndexServlet.setInitParameter("redirectWelcome", "false")
        handler.addServlet(swaggerIndexServlet, "/swagger/index.html")

        val swaggerUiServlet = new ServletHolder(new DefaultServlet())
        swaggerUiServlet.setInitParameter("resourceBase", Resources.getURL("META-INF/resources/webjars/swagger-ui/4.1.3/").toString)
        swaggerUiServlet.setInitParameter("dirAllowed","true")
        swaggerUiServlet.setInitParameter("pathInfoOnly","true")
        swaggerUiServlet.setInitParameter("redirectWelcome", "true")
        handler.addServlet(swaggerUiServlet, "/swagger/*")

        val serverUiServlet = new ServletHolder(new DefaultServlet())
        serverUiServlet.setInitParameter("resourceBase", Resources.getURL("META-INF/resources/webjars/flowman-server-ui/").toString)
        serverUiServlet.setInitParameter("dirAllowed", "true")
        serverUiServlet.setInitParameter("pathInfoOnly", "true")
        serverUiServlet.setInitParameter("redirectWelcome", "true")
        handler.addServlet(serverUiServlet, "/*")

        val requestLogHandler = new RequestLogHandler
        val requestLog = new CustomRequestLog(new Slf4jRequestLogWriter, CustomRequestLog.NCSA_FORMAT)
        requestLogHandler.setRequestLog(requestLog)

        // Start Jetty Server
        val handlers = new HandlerCollection
        handlers.setHandlers(Array(handler, requestLogHandler))

        val bindUri = new URI("http", null, conf.getBindHost(), conf.getBindPort(), null, null, null)
        val server = JettyHttpContainerFactory.createServer(bindUri, false)
        server.setHandler(handlers)
        server.start()
        server.join()
    }

    private def initSwagger() : Unit = {
        // Init Swagger
        val config2 = new BeanConfig()
        config2.setTitle("Flowman History Server")
        config2.setVersion("v1")
        config2.setSchemes(Array("http", "https"))
        config2.setHost("localhost:8080")
        config2.setBasePath("/api")
        config2.setUsePathBasedConfig(false)
        config2.setResourcePackage("com.dimajix.flowman.server.rest")
        config2.setPrettyPrint(true)
        config2.setScan(true);
    }
}

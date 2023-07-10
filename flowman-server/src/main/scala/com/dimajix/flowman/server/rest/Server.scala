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

import java.net.URL

import io.swagger.jaxrs.config.BeanConfig
import io.swagger.jaxrs.listing.ApiListingResource
import io.swagger.jaxrs.listing.SwaggerSerializers
import org.glassfish.hk2.utilities.binding.AbstractBinder
import org.glassfish.jersey.server.ResourceConfig
import org.glassfish.jersey.servlet.ServletContainer
import org.slf4j.LoggerFactory

import com.dimajix.flowman.common.jersey.JacksonConfig
import com.dimajix.flowman.common.jersey.OptionParamConverterProvider
import com.dimajix.flowman.common.jetty.JettyServer
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.history.JobQuery
import com.dimajix.flowman.model.Namespace
import com.dimajix.flowman.server.Configuration
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

        val server = JettyServer.builder()
            .addServlet(new ServletContainer(apiResources), "/api/*")
            .addStaticFile("com/dimajix/flowman/server", "/swagger/index.html", false)
            .addStaticFiles("META-INF/resources/webjars/swagger-ui/4.1.3/", "/swagger/*")
            .addStaticFiles("META-INF/resources/webjars/flowman-server-ui/", "/*")
            .bind(conf.getBindHost(), conf.getBindPort())
            .build()

        server.start()
        logger.info(s"Flowman History Server listening on ${server.bindUri()}")
        server.join()
    }

    private def initSwagger() : Unit = {
        // Init Swagger
        val config2 = new BeanConfig()
        config2.setTitle("Flowman History Server")
        config2.setVersion("v1")
        config2.setSchemes(Array("http", "https"))
        //config2.setHost(uri.getHost + ":" + uri.getPort)
        config2.setBasePath("/api")
        config2.setUsePathBasedConfig(false)
        config2.setResourcePackage("com.dimajix.flowman.server.rest")
        config2.setPrettyPrint(true)
        config2.setScan(true);
    }
}

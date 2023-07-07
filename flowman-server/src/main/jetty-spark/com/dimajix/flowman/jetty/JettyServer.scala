package com.dimajix.flowman.jetty

import java.net.URI

import javax.servlet.Servlet
import org.glassfish.jersey.server.spi.Container
import org.sparkproject.jetty.server.CustomRequestLog
import org.sparkproject.jetty.server.Handler
import org.sparkproject.jetty.server.HttpConfiguration
import org.sparkproject.jetty.server.HttpConnectionFactory
import org.sparkproject.jetty.server.SecureRequestCustomizer
import org.sparkproject.jetty.server.Server
import org.sparkproject.jetty.server.ServerConnector
import org.sparkproject.jetty.server.Slf4jRequestLogWriter
import org.sparkproject.jetty.server.SslConnectionFactory
import org.sparkproject.jetty.server.handler.HandlerCollection
import org.sparkproject.jetty.server.handler.RequestLogHandler
import org.sparkproject.jetty.servlet.DefaultServlet
import org.sparkproject.jetty.servlet.ServletContextHandler
import org.sparkproject.jetty.servlet.ServletHolder
import org.sparkproject.jetty.util.ssl.SslContextFactory
import org.sparkproject.jetty.util.thread.QueuedThreadPool

import com.dimajix.common.Resources


class JettyServerBuilder {
    private val handler = new ServletContextHandler(ServletContextHandler.NO_SESSIONS)
    private var bindPort = 80
    private var bindHost = ""

    def addServlet(container:Servlet, pathSpec:String) : JettyServerBuilder = {
        val apiServlet = new ServletHolder(container)
        handler.addServlet(apiServlet, pathSpec)
        this
    }

    def addStaticFile(file:String, pathSpec:String) : JettyServerBuilder = {
        val url = Resources.getURL(file).toString
        val swaggerIndexServlet = new ServletHolder(new DefaultServlet())
        swaggerIndexServlet.setInitParameter("resourceBase", url)
        swaggerIndexServlet.setInitParameter("dirAllowed", "true")
        swaggerIndexServlet.setInitParameter("pathInfoOnly", "true")
        swaggerIndexServlet.setInitParameter("redirectWelcome", "false")
        handler.addServlet(swaggerIndexServlet, pathSpec)
        this
    }

    def addStaticFiles(directory: String, pathSpec: String): JettyServerBuilder = {
        val url = Resources.getURL(directory).toString
        val swaggerIndexServlet = new ServletHolder(new DefaultServlet())
        swaggerIndexServlet.setInitParameter("resourceBase", url)
        swaggerIndexServlet.setInitParameter("dirAllowed", "true")
        swaggerIndexServlet.setInitParameter("pathInfoOnly", "true")
        swaggerIndexServlet.setInitParameter("redirectWelcome", "true")
        handler.addServlet(swaggerIndexServlet, pathSpec)
        this
    }

    def bind(address:String, port:Int) : JettyServerBuilder = {
        this.bindPort = port
        this.bindHost = address
        this
    }

    def build() : JettyServer = {
        val requestLogHandler = new RequestLogHandler
        val requestLogWriter = new Slf4jRequestLogWriter
        requestLogWriter.setLoggerName(classOf[JettyServer].toString)
        val requestLog = new CustomRequestLog(requestLogWriter, CustomRequestLog.NCSA_FORMAT)
        requestLogHandler.setRequestLog(requestLog)

        val handlers = new HandlerCollection
        handlers.setHandlers(Array(handler, requestLogHandler))

        val bindUri = new URI("http", null, bindHost, bindPort, null, null, null)
        new JettyServer(bindUri, null, handlers)
    }
}


object JettyServer {
    def builder() : JettyServerBuilder = new JettyServerBuilder
}

class JettyServer private[jetty](uri: URI,
                  sslContextFactory: SslContextFactory,
                  handler: Handler) {
    private val server = createServer(uri, sslContextFactory, handler)

    def bindUri() : URI = uri

    def start() : Unit = {
        server.start()
    }
    def join() : Unit = {
        server.join()
    }

    private def createServer(uri: URI,
                             sslContextFactory: SslContextFactory,
                             handler: Handler): Server = {
        if (uri == null) {
            throw new IllegalArgumentException("URI cannot be null")
        }
        val scheme = uri.getScheme
        var defaultPort = Container.DEFAULT_HTTP_PORT

        if (sslContextFactory == null) {
            if (!("http".equalsIgnoreCase(scheme))) {
                throw new IllegalArgumentException("Scheme must be 'http' when SSL is not used")
            }
        }
        else {
            if (!("https".equalsIgnoreCase(scheme))) {
                throw new IllegalArgumentException("Scheme must be 'https' when SSL is used")
            }
            defaultPort = Container.DEFAULT_HTTPS_PORT
        }
        val port: Int = if ((uri.getPort == -(1))) {
            defaultPort
        }
        else {
            uri.getPort
        }

        val pool = new QueuedThreadPool(200)
        pool.setDaemon(true)

        val server = new Server(pool)
        val config = new HttpConfiguration
        if (sslContextFactory != null) {
            config.setSecureScheme("https")
            config.setSecurePort(port)
            config.addCustomizer(new SecureRequestCustomizer)
            val https = new ServerConnector(server, new SslConnectionFactory(sslContextFactory, "http/1.1"), new HttpConnectionFactory(config))
            https.setPort(port)
            server.setConnectors(Array(https))
        }
        else {
            val http: ServerConnector = new ServerConnector(server, new HttpConnectionFactory(config))
            http.setPort(port)
            server.setConnectors(Array(http))
        }
        if (handler != null) {
            server.setHandler(handler)
        }

        server
    }
}

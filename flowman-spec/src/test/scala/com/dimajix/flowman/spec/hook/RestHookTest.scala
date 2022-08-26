/*
 * Copyright 2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.hook

import java.net.URL

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.Duration
import scala.concurrent.duration.SECONDS

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives.extractRequestContext
import akka.http.scaladsl.server.directives.RouteDirectives
import akka.stream.scaladsl.Sink
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.common.net.SocketUtils
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Lifecycle
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.model.Hook
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.Namespace
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.Prototype
import com.dimajix.flowman.model.TargetIdentifier
import com.dimajix.flowman.spec.ObjectMapper
import com.dimajix.flowman.spec.target.EmptyTargetSpec
import com.dimajix.flowman.types.StringType
import com.dimajix.spark.testing.LocalSparkSession


class RestHookTest extends AnyFlatSpec with Matchers with LocalSparkSession with RouteDirectives {
    implicit private val actorSystem: ActorSystem = ActorSystem("flowman")
    private var serverBinding: Http.ServerBinding = _
    private var serverUrl: URL = _
    private val serverRequests = mutable.ArrayBuffer[String]()

    override def beforeAll(): Unit = {
        implicit val executionContext: ExecutionContextExecutor = actorSystem.dispatcher

        super.beforeAll()

        val server = Http().bind("localhost", 0, akka.http.scaladsl.ConnectionContext.noEncryption())
            .to(Sink.foreach { connection =>
                connection.handleWith(
                    extractRequestContext { ctx =>
                        //println(s"Received request at ${ctx.request.uri.path}")
                        serverRequests.append(ctx.request.uri.path.toString())
                        complete("ok")
                    }
                )
            })
            .run()

        server.foreach { binding =>
            serverBinding = binding
            serverUrl = SocketUtils.toURL("http", binding.localAddress, allowAny = false)
        }

        Await.ready(server, Duration.Inf)
    }
    override def afterAll() : Unit = {
        Await.ready(serverBinding.terminate(Duration(5, SECONDS)), Duration.Inf)
        Await.ready(actorSystem.terminate(), Duration.Inf)

        super.afterAll()
    }

    "The RestHook" should "be parseable" in {
        val session = Session.builder()
            .disableSpark()
            .build()
        val context = session.context

        val yaml =
            """
              |kind: rest
              |url: http://${lala}
              |when:
              |  phase: [BUILD, VERIFY]
              |  category: job
              |""".stripMargin
        val spec = ObjectMapper.parse[HookSpec](yaml)
        spec shouldBe a[RestHookSpec]

        val hook = spec.instantiate(context)
        hook shouldBe a[RestHook]
        val restHook = hook.asInstanceOf[RestHook]
        restHook.condition.mapValues(_.map(_.regex)) should be (Map("phase" -> Seq("BUILD", "VERIFY"), "category" -> Seq("job")))
        restHook.url should be ("http://${lala}")
    }

    it should "work inside a namespace and job" in {
        val namespaceHook = new Prototype[Hook] {
            override def instantiate(context: Context, properties: Option[Hook.Properties]): Hook = RestHook(
                Hook.Properties(context),
                condition = Map("category" -> Seq("job".r, "target".r), "phase" -> Seq("VALIDATE".r, "BUILD".r), "status" -> Seq("SUCCESS".r)),
                url = serverUrl.toString + "/phase=$phase&category=$category&status=$status&job=$job"
            )
        }
        val jobHook = new Prototype[Hook] {
            override def instantiate(context: Context, properties: Option[Hook.Properties]): Hook = RestHook(
                Hook.Properties(context),
                condition = Map("category" -> Seq("lifecycle".r), "status" -> Seq("RUNNING".r, "SUCCESS".r)),
                url = serverUrl.toString + "/category=$category&status=$status&job=$job"
            )
        }
        val ns = Namespace(
            name = "default",
            hooks = Seq(namespaceHook)
        )
        val project = Project(
            name = "default",
            targets = Map("t0" -> EmptyTargetSpec("t0"))
        )
        val session = Session.builder()
            .withSparkSession(spark)
            .withNamespace(ns)
            .withEnvironment("env", "some_env")
            .withProject(project)
            .build()

        val job = Job.builder(session.getContext(project))
            .setName("some_job")
            .addHook(jobHook)
            .addTarget(TargetIdentifier("t0"))
            .addParameter("arg1", StringType)
            .build()

        val runner = session.runner
        runner.executeJob(job, Lifecycle.BUILD, Map("arg1" -> "some_arg"), force = true) should be(Status.SUCCESS)

        serverRequests should be (Seq(
            "/category=lifecycle&status=RUNNING&job=some_job",
            "/phase=VALIDATE&category=target&status=SUCCESS&job=some_job",
            "/phase=VALIDATE&category=job&status=SUCCESS&job=some_job",
            "/phase=BUILD&category=target&status=SUCCESS&job=some_job",
            "/phase=BUILD&category=job&status=SUCCESS&job=some_job",
            "/category=lifecycle&status=SUCCESS&job=some_job"
        ))
    }
}

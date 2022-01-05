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

package com.dimajix.flowman.hub.service

import java.io.File
import java.net.URL

import akka.actor.ActorSystem
import org.slf4j.LoggerFactory

import com.dimajix.flowman.common.ToolConfig


class LocalLauncher(studioUrl:URL, system:ActorSystem) extends Launcher {
    private val logger = LoggerFactory.getLogger(classOf[LocalLauncher])

    override def name: String = "local"

    override def description: String = "Default local launcher"

    override def launch(env:LaunchEnvironment) : Process = {
        val cmd = new File(ToolConfig.homeDirectory.getOrElse(throw new RuntimeException("FLOWMAN_HOME not set")), "bin/flowkernel").toString
        val args = Seq(
            "--kernel-id", env.id,
            "--kernel-secret", env.secret,
            "--bind-host", "localhost",
            "--bind-port", "0",
            "--studio-url", studioUrl.toString
        )
        val extraEnv = Seq[(String,String)]()

        logger.info(
            s"""Launching local process
               |  kernel-id: ${env.id}
               |  kernel-secret: ${env.secret}
               |  cmd: $cmd
               |  args: ${args.mkString(" ")}
               |  extraEnv: ${extraEnv.map(kv => kv._1 + "=" + kv._2).mkString("\n      ")}""".stripMargin)

        val builder = sys.process.Process.apply(cmd +: args, None, extraEnv:_*)
        new LocalProcess(builder, system)
    }
}

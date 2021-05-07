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

package com.dimajix.flowman.studio.service

import java.io.File

import akka.actor.ActorSystem

import com.dimajix.flowman.common.ToolConfig


class LocalLauncher(system:ActorSystem) extends Launcher {

    override def name: String = "local"

    override def description: String = "Default local launcher"

    override def launch() : Process = {
        val cmd = new File(ToolConfig.homeDirectory.get, "bin/flowkernel").toString
        val args = Seq(
            "--bind-host", "localhost",
            "--bind-port", "0",
            "--studio-url", "http://localhost:8080"
        )
        val extraEnv = Seq[(String,String)]()
        val builder = sys.process.Process.apply(cmd +: args, None, extraEnv:_*)
        new LocalProcess(builder, system)
    }
}

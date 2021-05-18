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

package com.dimajix.flowman.kernel.service

import java.lang.Thread.UncaughtExceptionHandler
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.ForkJoinWorkerThread

import scala.collection.mutable
import scala.concurrent.ExecutionContext

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution
import com.dimajix.flowman.hadoop.FileSystem
import com.dimajix.flowman.model.Project


object SessionManager {
    private val logger = LoggerFactory.getLogger(classOf[SessionManager])
    private class MyForkJoinWorkerThread(pool: ForkJoinPool) extends ForkJoinWorkerThread(pool) { // set the correct classloader here
        setContextClassLoader(Thread.currentThread.getContextClassLoader)
    }
    private object MyForkJoinWorkerThreadFactory extends ForkJoinPool.ForkJoinWorkerThreadFactory {
        override final def newThread(pool: ForkJoinPool) = new MyForkJoinWorkerThread(pool)
    }
    private val exceptionHandler = new UncaughtExceptionHandler {
        override def uncaughtException(thread: Thread, throwable: Throwable): Unit = {
            logger.error("Uncaught exception: ", throwable)
        }
    }
}

class SessionManager(rootSession:execution.Session) {
    import SessionManager._
    private val sessions = mutable.ListBuffer[SessionService]()
    private val threadPool = new ForkJoinPool(4, MyForkJoinWorkerThreadFactory, exceptionHandler, true)
    private implicit val executionContext = ExecutionContext.fromExecutorService(threadPool)

    /**
     * Returns a list of all active [[SessionService]]s
     * @return
     */
    def list() : Seq[SessionService] = {
        val result = mutable.ListBuffer[SessionService]()
        sessions.synchronized {
            result.append(sessions:_*)
        }
        result
    }

    /**
     * Returns the [[SessionService]] for a specific id. If no such session is known, [[None]] will be returned
     * instead
     * @param id
     * @return
     */
    def getSession(id:String) : Option[SessionService] = {
        var result:Option[SessionService] = None
        sessions.synchronized {
            result = sessions.find(_.id == id)
        }
        result
    }


    /**
     * Creates a new [[SessionService]] by loading a new project. The project is specified via its name, as returned
     * by [[rootSession]]
     * @param projectPath
     * @return
     */
    def createSession(projectName:String) : SessionService = {
        val project = rootSession.store.loadProject(projectName)
        createSession(project)
    }

    /**
     * Creates a new [[SessionService]] by loading a new project. The project is specified via a path, which needs
     * to point to a location resolvable by the Hadoop filesystem layer.
     * @param projectPath
     * @return
     */
    def createSession(projectPath:Path) : SessionService = {
        val project = loadProject(projectPath)
        createSession(project)
    }

    private def createSession(project:Project) : SessionService = {
        val session = rootSession.newSession(project)
        val svc = new SessionService(this, session)

        sessions.synchronized {
            sessions.append(svc)
        }

        svc
    }

    private[service] def removeSession(svc:SessionService) : Unit = {
        val id = svc.id
        sessions.synchronized {
            val index = sessions.indexWhere(_.id == id)
            if (index >= 0) {
                sessions.remove(index)
            }
        }
    }

    private def loadProject(projectPath:Path) : Project = {
        // Create Hadoop FileSystem instance
        val hadoopConfig = new Configuration()
        val fs = FileSystem(hadoopConfig)

        // Load Project. If no schema is specified, load from local file system
        val projectUri = projectPath.toUri
        if (projectUri.getAuthority == null && projectUri.getScheme == null)
            Project.read.file(fs.local(projectPath))
        else
            Project.read.file(fs.file(projectPath))
    }
}

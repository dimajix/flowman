/*
 * Copyright 2021-2023 Kaya Kupferschmidt
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

import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution
import com.dimajix.flowman.fs.File
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.storage.Store


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

class SessionManager(val rootSession:execution.Session) {
    import SessionManager._
    private val logger = LoggerFactory.getLogger(classOf[SessionManager])
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
    def createSession(store: Store, projectName:String) : SessionService = {
        val project = store.loadProject(projectName)
        createSession(store, project)
    }
    def createSession(store: Store, project:Project) : SessionService = {
        val svc = new SessionService(this, store, project)

        sessions.synchronized {
            sessions.append(svc)
        }

        svc
    }
    def createSession(store: Store, projectLocation: File): SessionService = {
        val project = Project.read.file(projectLocation)
        createSession(store, project)
    }
    def createSession(projectLocation:File) : SessionService = {
        createSession(rootSession.store, projectLocation)
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
}


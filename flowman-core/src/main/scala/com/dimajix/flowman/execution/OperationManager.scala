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

package com.dimajix.flowman.execution

import scala.collection.mutable

import javax.annotation.concurrent.GuardedBy


class OperationManager(parent:Option[OperationManager]=None) {
    @GuardedBy("opsLock")
    private val ops = new mutable.HashMap[String,Operation]()
    private val opsLock = new Object

    private val listener = new OperationListener {
        override def onOperationTerminated(event: OperationListener.OperationTerminatedEvent): Unit = {
            val op = event.operation
            op.removeListener(this)
            opsLock.synchronized {
                ops.remove(op.name)
            }
        }
    }

    def this(parent:OperationManager) = {
        this(Some(parent))
    }

    /**
     * Posts a new [[Operation]] to the manager. The operation is probably already be running.
     * @param op
     */
    def post(op:Operation) : Unit = opsLock.synchronized {
        parent.foreach(_.post(op))

        if (ops.contains(op.name))
            throw new IllegalArgumentException(s"Cannot post operation with name '${op.name}', as an operation with the same name already exists")

        if (op.isActive) {
            op.addListener(listener)
            ops.put(op.name, op)
        }
    }

    /**
     * Returns a list of all Operations.
     * @return
     */
    def listAll() : Seq[Operation] = opsLock.synchronized {
        ops.values.toSeq
    }

    /**
     * Returns a list only of active Operations.
     * @return
     */
    def listActive() : Seq[Operation] = opsLock.synchronized {
        ops.values.filter(_.isActive).toSeq
    }

    /**
     * Returns true if at least one operation is running.
     * @return
     */
    def isActive : Boolean = opsLock.synchronized {
        ops.values.exists(_.isActive)
    }

    /**
     * Triggers a stop on all running operations
     */
    def stop() : Unit = {
        listActive().foreach(_.stop())
    }

    def awaitTermination(): Unit = {
        listActive().foreach(_.awaitTermination())
    }
}

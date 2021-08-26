/*
 * Copyright 2018-2019 Kaya Kupferschmidt
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

import java.util.Locale

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import com.dimajix.flowman.common.ThreadUtils


sealed abstract class Status {
    val value: String

    override def toString: String = value
}

object Status {
    case object UNKNOWN extends Status { val value = "unknown" }
    case object RUNNING extends Status { val value = "running" }
    case object SUCCESS extends Status { val value = "success" }
    case object FAILED extends Status { val value = "failed" }
    case object ABORTED extends Status { val value = "killed" }
    case object SKIPPED extends Status { val value = "skipped" }

    def ofString(status:String) : Status = {
        status.toLowerCase(Locale.ROOT) match {
            case UNKNOWN.value => UNKNOWN
            case RUNNING.value => RUNNING
            case SUCCESS.value => SUCCESS
            case FAILED.value => FAILED
            case ABORTED.value => ABORTED
            case SKIPPED.value => SKIPPED
            case _ => throw new IllegalArgumentException(s"No status defined for '$status'")
        }
    }

    /**
      * This function determines a common status of multiple actions
      * @param seq
      * @param fn
      * @tparam T
      * @return
      */
    def ofAll[T](seq: Iterable[T], keepGoing:Boolean=false)(fn:T => Status) : Status = {
        val iter = seq.iterator
        var error = false
        var skipped = true
        val empty = !iter.hasNext
        while (iter.hasNext && (!error || keepGoing)) {
            val item = iter.next()
            val status = fn(item)
            error |= (status != Status.SUCCESS && status != Status.SKIPPED)
            skipped &= (status == Status.SKIPPED)
        }

        if (empty)
            Status.SUCCESS
        else if (error)
            Status.FAILED
        else if (skipped)
            Status.SKIPPED
        else
            Status.SUCCESS
    }

    /**
     * This function determines a common status of multiple actions, which are executed in parallel
     * @param seq
     * @param fn
     * @tparam T
     * @return
     */
    def parallelOfAll[T](seq: Seq[T], parallelism:Int, keepGoing:Boolean=false, prefix:String="ParallelExecution")(fn:T => Status) : Status = {
        // Allocate state variables for tracking overall Status
        val statusLock = new Object
        var error = false
        var skipped = true
        var empty = true

        ThreadUtils.parmap(seq, prefix, parallelism) { p =>
            if (!error || keepGoing) {
                val result = Try {
                    fn(p)
                }
                // Evaluate status
                statusLock.synchronized {
                    result match {
                        case Success(status) =>
                            empty = false
                            error |= (status != Status.SUCCESS && status != Status.SKIPPED)
                            skipped &= (status == Status.SKIPPED)
                            status
                        case Failure(_) =>
                            empty = false
                            error = true
                            Status.FAILED
                    }
                }
            }
        }

        // Evaluate overall status
        if (empty)
            Status.SUCCESS
        else if (error)
            Status.FAILED
        else if (skipped)
            Status.SKIPPED
        else
            Status.SUCCESS
    }
}

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

import org.slf4j.LoggerFactory

import com.dimajix.flowman.history.BatchToken
import com.dimajix.flowman.history.TargetToken
import com.dimajix.flowman.spec.target.BatchInstance
import com.dimajix.flowman.spec.target.TargetInstance


class SimpleRunner extends AbstractRunner {
    override protected val logger = LoggerFactory.getLogger(classOf[SimpleRunner])

    /**
      * Starts the run and returns a token, which can be anything
      *
      * @return
      */
    override protected def startBatch(job:BatchInstance, phase:Phase) : BatchToken = null

    /**
      * Marks a run as a success
      *
      * @param token
      */
    override protected def finishBatch(token:BatchToken, status:Status) : Unit = {}

    /**
      * Performs some checks, if the run is required. Returns faöse if the target is out of date needs to be rebuilt
      *
      * @param target
      * @return
      */
    protected override def checkTarget(target: TargetInstance, phase:Phase): Boolean = false

    /**
      * Starts the run and returns a token, which can be anything
      *
      * @param target
      * @return
      */
    override protected def startTarget(target:TargetInstance, phase:Phase, parent:Option[BatchToken]) : TargetToken = null

    /**
      * Marks a run as a success
      *
      * @param token
      */
    override protected def finishTarget(token:TargetToken, status:Status) : Unit = {}
}

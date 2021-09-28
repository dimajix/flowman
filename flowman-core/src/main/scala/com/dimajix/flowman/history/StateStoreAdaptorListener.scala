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

package com.dimajix.flowman.history

import com.dimajix.flowman.execution
import com.dimajix.flowman.execution.AbstractExecutionListener
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Token
import com.dimajix.flowman.history
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.JobInstance
import com.dimajix.flowman.model.JobResult
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetInstance
import com.dimajix.flowman.model.TargetResult


object StateStoreAdaptorListener {
    final case class StateStoreJobToken(token:history.JobToken) extends execution.JobToken
    final case class StateStoreTargetToken(token:history.TargetToken) extends execution.TargetToken
}
final class StateStoreAdaptorListener(store:StateStore) extends AbstractExecutionListener {
    import StateStoreAdaptorListener._

    override def startJob(excution:Execution, job:Job, instance: JobInstance, phase: Phase, parent:Option[Token]): execution.JobToken = {
        StateStoreJobToken(store.startJob(instance, phase))
    }
    override def finishJob(excution:Execution, token: execution.JobToken, result: JobResult): Unit = {
        val status = result.status
        val t = token.asInstanceOf[StateStoreJobToken].token
        store.finishJob(t, status)
    }
    override def startTarget(excution:Execution, target:Target, instance: TargetInstance, phase: Phase, parent: Option[execution.Token]): execution.TargetToken = {
        val t = parent.map(_.asInstanceOf[StateStoreJobToken].token)
        StateStoreTargetToken(store.startTarget(instance, phase, t))
    }
    override def finishTarget(excution:Execution, token: execution.TargetToken, result:TargetResult): Unit = {
        val status = result.status
        val t = token.asInstanceOf[StateStoreTargetToken].token
        store.finishTarget(t, status)
    }
}

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

import com.dimajix.flowman.common.ListenerBus


class OperationListenerBus extends ListenerBus[OperationListener, OperationListener.Event] {
    import OperationListener._
    /**
     * Post an event to the specified listener. `onPostEvent` is guaranteed to be called in the same
     * thread for all listeners.
     */
    override protected def doPostEvent(listener: OperationListener, event: OperationListener.Event): Unit = {
        event match {
            case ev:OperationTerminatedEvent =>
                listener.onOperationTerminated(ev)
        }
    }
}

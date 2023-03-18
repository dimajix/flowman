/*
 * Copyright (C) 2023 The Flowman Authors
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

package com.dimajix.flowman.model

import org.slf4j.Logger

import com.dimajix.flowman.execution.Execution


/**
  * This trait provides a context specific logger. This is required for Flowman kernel/client architecture where
  * logging is to be forwarded to each client. Therefore each Flowman session, context and execution provides its
  * own LoggingFactory, which will take care of the forwarding.
  */
trait Logging { this:Instance =>
    /**
      * Returns an appropriate logger, using getClass to find out the current class
      */
    protected lazy val logger = context.loggerFactory.getLogger(getClass.getName)

    /**
      * Returns a logger from the LoggerFactory provided by the execution. The method will ask for a logger for the
      * current class.
      * @param execution
      * @return
      */
    protected final def getLogger(execution: Execution) : Logger = execution.loggerFactory.getLogger(getClass.getName)

    /**
      * Returns a logger from the LoggerFactory provided by the execution.
      *
      * @param execution
      * @param clazz
      * @return
      */
    protected final def getLogger[T](execution: Execution, clazz:Class[T]) : Logger = execution.loggerFactory.getLogger(clazz.getName)
}

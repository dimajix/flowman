/*
 * Copyright (C) 2019 The Flowman Authors
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

package com.dimajix.flowman.metric

import java.time.Instant


final case class WallTimeMetric(override val name:String, override val labels:Map[String,String]) extends GaugeMetric {
    private var startTime = now()
    private var endTime:Option[Long] = None

    override def value: Double = endTime.getOrElse(now()) - startTime

    /**
      * Resets this metric
      */
    override def reset(): Unit = {
        startTime = now()
        endTime = None
    }

    def start() : Unit = {
        startTime = now()
        endTime = None
    }

    def stop() : Unit = {
        endTime = Some(now())
    }

    private def now() : Long = Instant.now().toEpochMilli
}

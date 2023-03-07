/*
 * Copyright 2018-2023 Kaya Kupferschmidt
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

package com.dimajix.common

import scala.collection.JavaConverters._


object JavaConverters {
    def asJava[T](seq: Iterable[T]): java.lang.Iterable[T] = seq.asJava

    def asJava[T](seq: Seq[T]): java.util.List[T] = seq.asJava

    def asJava[S, T](seq: Map[S, T]): java.util.Map[S, T] = seq.asJava

    def asJava[T](opt: Option[T]) : java.util.Optional[T] = {
        opt match {
            case Some(x) => java.util.Optional.of(x)
            case None => java.util.Optional.empty()
        }
    }


    def asScala[T](seq: java.lang.Iterable[T]): Iterable[T] = seq.asScala

    def asScala[T](seq: java.util.List[T]): Seq[T] = seq.asScala

    def asScala[S, T](seq: java.util.Map[S, T]): Map[S, T] = seq.asScala.toMap

    def asScala[T](opt: java.util.Optional[T]): Option[T] = {
        if (opt.isEmpty)
            None
        else
            Some(opt.get())
    }
}

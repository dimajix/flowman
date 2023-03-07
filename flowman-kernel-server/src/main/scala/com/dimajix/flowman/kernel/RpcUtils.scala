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

package com.dimajix.flowman.kernel;

import scala.util.control.NonFatal

import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import io.grpc.stub.StreamObserver

import com.dimajix.flowman.grpc.ExceptionUtils


object RpcUtils {
    def respondTo[T](responseObserver:StreamObserver[T])(callable: => T) {
        val response = try {
            callable;
        }
        catch {
            case e@(_: StatusException | _: StatusRuntimeException) =>
                responseObserver.onError(e)
                return;
            case NonFatal(t) =>
                val e = ExceptionUtils.asStatusException(Status.INTERNAL, t, true)
                responseObserver.onError(e)
                return;
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }


    def streamRequests[S,T](responseObserver: StreamObserver[T])(next: S => Unit)(result: => T) : StreamObserver[S] = {
        new StreamObserver[S]() {
            @Override
            def onNext(req: S): Unit = {
                try {
                    next(req)
                }
                catch {
                    case e@(_: StatusException | _: StatusRuntimeException) =>
                        responseObserver.onError(e)
                    case NonFatal(t) =>
                        val e = ExceptionUtils.asStatusException(Status.INTERNAL, t, true)
                        responseObserver.onError(e)
                }
            }

            @Override
            def onError(t: Throwable): Unit = {
            }

            @Override
            def onCompleted(): Unit = {
                respondTo(responseObserver)(result)
            }
        };
    }
}

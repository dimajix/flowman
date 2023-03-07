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

import java.util.List;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import lombok.val;
import scala.collection.immutable.Seq;

import com.dimajix.flowman.grpc.ExceptionUtils;


public class RpcUtils2 {
    public interface RpcCallable<T> {
        T call();
    }

    public interface RpcConsumer<T> {
        void consume(T t);
    }

    public static <T> void respondTo(StreamObserver<T> responseObserver, RpcCallable<T> callable) {
        T response;
        try {
            response = callable.call();
        }
        catch (StatusRuntimeException e) {
            responseObserver.onError(e);
            return;
        }
        catch (Throwable t) {
            val e = ExceptionUtils.asStatusException(Status.INTERNAL, t, true);
            responseObserver.onError(e);
            return;
        }
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }


    public static <S,T> StreamObserver<S> streamRequests(final StreamObserver<T> responseObserver, final RpcConsumer<S> next, final RpcCallable<T> result) {
        return new StreamObserver<S>() {
            @Override
            public void onNext(S r) {
                try {
                    next.consume(r);
                }
                catch (StatusRuntimeException e) {
                    responseObserver.onError(e);
                }
                catch(Throwable t) {
                    val e = ExceptionUtils.asStatusException(Status.INTERNAL, t, true);
                    responseObserver.onError(e);
                }
            }

            @Override
            public void onError(Throwable t) {
            }

            @Override
            public void onCompleted() {
                respondTo(responseObserver, result);
            }
        };
    }
}

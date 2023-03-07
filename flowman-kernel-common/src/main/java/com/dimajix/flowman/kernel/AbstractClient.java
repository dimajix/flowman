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

import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import lombok.val;

import com.dimajix.flowman.grpc.ExceptionUtils;
import com.dimajix.flowman.kernel.proto.workspace.UploadFilesResponse;


public class AbstractClient {
    protected interface RpcCallable<T> {
        T call();
    }

    protected interface RpcMethod<S,T> {
        StreamObserver<S> call(StreamObserver<T> responseObserver);
    }

    protected static <T> T call(RpcCallable<T> callable) {
        try {
            return callable.call();
        }
        catch (StatusRuntimeException ex) {
            throw ExceptionUtils.asRemoteException(ex);
        }
    }

    protected static <S,T> T stream(RpcMethod<S,T> method, Iterator<S> streamable) {
        val finishLatch = new CountDownLatch(1);
        val result = new AtomicReference<T>();
        val exception = new AtomicReference<RuntimeException>();
        val observer = new StreamObserver<T>() {
            @Override
            public void onNext(T value) {
                result.set(value);
            }

            @Override
            public void onError(Throwable t) {
                if (t instanceof RuntimeException)
                    exception.set((RuntimeException)t);
                else if (t instanceof StatusException)
                    exception.set(((StatusException)t).getStatus().asRuntimeException());
                else
                    exception.set(new RuntimeException(t));
                finishLatch.countDown();
            }

            @Override
            public void onCompleted() {
                finishLatch.countDown();
            }
        };

        val sink = method.call(observer);
        try {
            while(streamable.hasNext()) {
                val next = streamable.next();
                sink.onNext(next);
                if (finishLatch.getCount() == 0) {
                    // RPC completed or errored before we finished sending.
                    // Sending further requests won't error, but they will just be thrown away.
                    break;
                }
            }
        }
        catch(RuntimeException ex) {
            sink.onError(ex);
            throw ex;
        }

        // Mark the end of requests
        sink.onCompleted();

        // Receiving happens asynchronously
        var hasFinished = false;
        while(!hasFinished) {
            try {
                finishLatch.await(1, TimeUnit.MINUTES);
                hasFinished = true;
            }
            catch(InterruptedException ex) {
                // Do nothing, try again
            }
        }

        if (exception.get() != null)
            throw exception.get();

        return result.get();
    }
}

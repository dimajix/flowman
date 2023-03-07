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

package com.dimajix.flowman.grpc;

import java.util.Arrays;
import java.util.stream.Collectors;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.ProtoUtils;
import lombok.val;

import com.dimajix.flowman.kernel.proto.Exception;
import com.dimajix.flowman.kernel.proto.StackTraceElement;


public class ExceptionUtils {
    static private StackTraceElement wrap(java.lang.StackTraceElement se) {
        val builder = StackTraceElement.newBuilder();
        //if (se.getClassLoaderName() != null)
        //    builder.setClassLoaderName(se.getClassLoaderName());
        //if (se.getModuleName() != null)
        //    builder.setModuleName(se.getModuleName());
        //if (se.getModuleVersion() != null)
        //    builder.setModuleVersion(se.getModuleVersion());
        builder.setDeclaringClass(se.getClassName());
        builder.setMethodName(se.getMethodName());
        if (se.getFileName() != null)
            builder.setFileName(se.getFileName());
        builder.setLineNumber(se.getLineNumber());
        return builder.build();
    }

    static private java.lang.StackTraceElement unwrap(StackTraceElement se) {
        return new java.lang.StackTraceElement(
            //se.hasClassLoaderName() ? se.getClassLoaderName() : null,
            //se.hasModuleName() ? se.getModuleName() : null,
            //se.hasModuleVersion() ? se.getModuleVersion() : null,
            se.getDeclaringClass(),
            se.getMethodName(),
            se.hasFileName() ? se.getFileName() : null,
            se.getLineNumber()
        );
    }

    static public Exception wrap(Throwable t, boolean withStacktrace) {
        val builder = Exception.newBuilder()
            .setClassName(t.getClass().getSimpleName())
            .setFqName(t.getClass().getCanonicalName());
        // We disable stacktraces, because errors would be too big for the transport protocol
        if (withStacktrace && t.getStackTrace() != null && t.getStackTrace().length > 0) {
            val stacktrace = Arrays.stream(t.getStackTrace())
                .limit(16)
                .map(ExceptionUtils::wrap)
                .collect(Collectors.toList());
            builder.addAllStackTrace(stacktrace);
        }
        if (t.getMessage() != null) {
            builder.setMessage(t.getMessage());
        }
        if (t.getCause() != null && t.getCause() != t) {
            builder.setCause(wrap(t.getCause(), withStacktrace));
        }
        if (t.getSuppressed() != null && t.getSuppressed().length > 0) {
            builder.addAllSuppressed(Arrays.stream(t.getSuppressed()).map(e -> wrap(e, withStacktrace)).collect(Collectors.toList()));
        }

        return builder.build();
    }

    static public RemoteException unwrap(Exception ex) {
        val cause = ex.hasCause() ? unwrap(ex.getCause()) : null;
        RemoteException result = new RemoteException(ex.getFqName(), ex.hasMessage() ? ex.getMessage() : null, cause);

        // The stacktrace can only be set for exceptions which are thrown
        if (ex.getStackTraceCount() > 0) {
            try {
                throw result;
            } catch (RemoteException ex2) {
                result = ex2;
            }
            val st = ex.getStackTraceList().stream().map(ExceptionUtils::unwrap).toArray(java.lang.StackTraceElement[]::new);
            result.setStackTrace(st);
        }

        if (ex.getSuppressedCount() > 0) {
            ex.getSuppressedList().stream().map(ExceptionUtils::unwrap).forEach(result::addSuppressed);
        }
        return result;
    }

    static public StatusException asStatusException(Status status, Throwable t, boolean withStacktrace) {
        val trailers = new Metadata();
        trailers.put(ProtoUtils.keyForProto(Exception.getDefaultInstance()), ExceptionUtils.wrap(t, withStacktrace));

        return status
            .withCause(t)
            .withDescription(t.getMessage())
            .asException(trailers);
    }

    static public RuntimeException asRemoteException(StatusRuntimeException ex) {
        val trailers = ex.getTrailers();
        if (trailers == null) {
            return ex;
        }

        val exception = trailers.get(ProtoUtils.keyForProto(Exception.getDefaultInstance()));
        if (exception == null) {
            return ex;
        }

        return unwrap(exception);
    }
}

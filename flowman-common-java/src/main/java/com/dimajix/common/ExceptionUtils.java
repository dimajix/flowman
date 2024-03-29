/*
 * Copyright (C) 2020 The Flowman Authors
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

package com.dimajix.common;


public final class ExceptionUtils {
    private ExceptionUtils() {
    }

    public static String reasons(Throwable ex) {
        String msg = ex.toString();
        Throwable cause = ex.getCause();
        if (cause == null)
            return msg;
        else
            return msg + "\n    caused by: " + reasons(cause);
    }

    public static boolean isNonFatal(Throwable t) {
        return !isFatal(t);
    }
    public static boolean isFatal(Throwable t) {
        return
            t instanceof VirtualMachineError || t instanceof ThreadDeath ||
                t instanceof InterruptedException || t instanceof LinkageError;
    }
}

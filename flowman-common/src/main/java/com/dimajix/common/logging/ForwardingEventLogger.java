/*
 * Copyright 2023 Kaya Kupferschmidt
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

package com.dimajix.common.logging;

import java.util.function.Consumer;


public class ForwardingEventLogger extends EventLogger {
    private final Consumer<LogEvent> sink;
    private boolean traceEnabled = false;
    private boolean debugEnabled = false;
    private boolean infoEnabled = true;
    private boolean warnEnabled = true;
    private boolean errorEnabled = true;

    public ForwardingEventLogger(String name, Consumer<LogEvent> sink) {
        super(name);
        this.sink = sink;
    }

    @Override
    protected void trace(LogEvent event) {
        sink.accept(event);
    }

    @Override
    protected void debug(LogEvent event) {
        sink.accept(event);
    }

    @Override
    protected void info(LogEvent event) {
        sink.accept(event);
    }

    @Override
    protected void warn(LogEvent event) {
        sink.accept(event);
    }

    @Override
    protected void error(LogEvent event) {
        sink.accept(event);
    }

    @Override
    public boolean isTraceEnabled() {
        return traceEnabled;
    }

    @Override
    public boolean isDebugEnabled() {
        return debugEnabled;
    }

    @Override
    public boolean isInfoEnabled() {
        return infoEnabled;
    }

    @Override
    public boolean isWarnEnabled() {
        return warnEnabled;
    }

    @Override
    public boolean isErrorEnabled() {
        return errorEnabled;
    }
}

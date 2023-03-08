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

import java.time.Instant;

import org.slf4j.event.Level;
import org.slf4j.helpers.FormattingTuple;
import org.slf4j.helpers.MarkerIgnoringBase;
import org.slf4j.helpers.MessageFormatter;


abstract public class EventLogger extends MarkerIgnoringBase {
    protected final String name;

    protected EventLogger(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public void trace(String msg) {
        if (isTraceEnabled()) {
            trace(new LogEvent(Level.TRACE, name, msg, Instant.now().toEpochMilli(), null));
        }
    }

    @Override
    public void trace(String format, Object arg) {
        if (isTraceEnabled()) {
            FormattingTuple ft = MessageFormatter.format(format, arg);
            trace(new LogEvent(Level.TRACE, name, ft.getMessage(), Instant.now().toEpochMilli(), ft.getThrowable()));
        }
    }

    @Override
    public void trace(String format, Object arg1, Object arg2) {
        if (isTraceEnabled()) {
            FormattingTuple ft = MessageFormatter.format(format, arg1, arg2);
            trace(new LogEvent(Level.TRACE, name, ft.getMessage(), Instant.now().toEpochMilli(), ft.getThrowable()));
        }
    }

    @Override
    public void trace(String format, Object... arguments) {
        if (isTraceEnabled()) {
            FormattingTuple ft = MessageFormatter.format(format, arguments);
            trace(new LogEvent(Level.TRACE, name, ft.getMessage(), Instant.now().toEpochMilli(), ft.getThrowable()));
        }
    }

    @Override
    public void trace(String msg, Throwable t) {
        if (isTraceEnabled()) {
            trace(new LogEvent(Level.TRACE, name, msg, Instant.now().toEpochMilli(), t));
        }
    }

    abstract protected void trace(LogEvent event);


    @Override
    public void debug(String msg) {
        if (isDebugEnabled()) {
            debug(new LogEvent(Level.DEBUG, name, msg, Instant.now().toEpochMilli(), null));
        }
    }

    @Override
    public void debug(String format, Object arg) {
        if (isDebugEnabled()) {
            FormattingTuple ft = MessageFormatter.format(format, arg);
            debug(new LogEvent(Level.DEBUG, name, ft.getMessage(), Instant.now().toEpochMilli(), ft.getThrowable()));
        }
    }

    @Override
    public void debug(String format, Object arg1, Object arg2) {
        if (isDebugEnabled()) {
            FormattingTuple ft = MessageFormatter.format(format, arg1, arg2);
            debug(new LogEvent(Level.DEBUG, name, ft.getMessage(), Instant.now().toEpochMilli(), ft.getThrowable()));
        }
    }

    @Override
    public void debug(String format, Object... arguments) {
        if (isDebugEnabled()) {
            FormattingTuple ft = MessageFormatter.format(format, arguments);
            debug(new LogEvent(Level.DEBUG, name, ft.getMessage(), Instant.now().toEpochMilli(), ft.getThrowable()));
        }
    }

    @Override
    public void debug(String msg, Throwable t) {
        if (isDebugEnabled()) {
            debug(new LogEvent(Level.DEBUG, name, msg, Instant.now().toEpochMilli(), t));
        }
    }

    abstract protected void debug(LogEvent event);


    @Override
    public void info(String msg) {
        if (isInfoEnabled()) {
            info(new LogEvent(Level.INFO, name, msg, Instant.now().toEpochMilli(), null));
        }
    }

    @Override
    public void info(String format, Object arg) {
        if (isInfoEnabled()) {
            FormattingTuple ft = MessageFormatter.format(format, arg);
            info(new LogEvent(Level.INFO, name, ft.getMessage(), Instant.now().toEpochMilli(), ft.getThrowable()));
        }
    }

    @Override
    public void info(String format, Object arg1, Object arg2) {
        if (isInfoEnabled()) {
            FormattingTuple ft = MessageFormatter.format(format, arg1, arg2);
            info(new LogEvent(Level.INFO, name, ft.getMessage(), Instant.now().toEpochMilli(), ft.getThrowable()));
        }
    }

    @Override
    public void info(String format, Object... arguments) {
        if (isInfoEnabled()) {
            FormattingTuple ft = MessageFormatter.format(format, arguments);
            info(new LogEvent(Level.INFO, name, ft.getMessage(), Instant.now().toEpochMilli(), ft.getThrowable()));
        }
    }

    @Override
    public void info(String msg, Throwable t) {
        if (isInfoEnabled()) {
            info(new LogEvent(Level.INFO, name, msg, Instant.now().toEpochMilli(), t));
        }
    }

    abstract protected void info(LogEvent event);


    @Override
    public void warn(String msg) {
        if (isWarnEnabled()) {
            warn(new LogEvent(Level.WARN, name, msg, Instant.now().toEpochMilli(), null));
        }
    }

    @Override
    public void warn(String format, Object arg) {
        if (isWarnEnabled()) {
            FormattingTuple ft = MessageFormatter.format(format, arg);
            warn(new LogEvent(Level.WARN, name, ft.getMessage(), Instant.now().toEpochMilli(), ft.getThrowable()));
        }
    }

    @Override
    public void warn(String format, Object arg1, Object arg2) {
        if (isWarnEnabled()) {
            FormattingTuple ft = MessageFormatter.format(format, arg1, arg2);
            warn(new LogEvent(Level.WARN, name, ft.getMessage(), Instant.now().toEpochMilli(), ft.getThrowable()));
        }
    }

    @Override
    public void warn(String format, Object... arguments) {
        if (isWarnEnabled()) {
            FormattingTuple ft = MessageFormatter.format(format, arguments);
            warn(new LogEvent(Level.WARN, name, ft.getMessage(), Instant.now().toEpochMilli(), ft.getThrowable()));
        }
    }

    @Override
    public void warn(String msg, Throwable t) {
        if (isWarnEnabled()) {
            warn(new LogEvent(Level.WARN, name, msg, Instant.now().toEpochMilli(), t));
        }
    }

    abstract protected void warn(LogEvent event);

    @Override
    public void error(String msg) {
        if (isErrorEnabled()) {
            error(new LogEvent(Level.ERROR, name, msg, Instant.now().toEpochMilli(), null));
        }
    }

    @Override
    public void error(String format, Object arg) {
        if (isErrorEnabled()) {
            FormattingTuple ft = MessageFormatter.format(format, arg);
            error(new LogEvent(Level.ERROR, name, ft.getMessage(), Instant.now().toEpochMilli(), ft.getThrowable()));
        }
    }

    @Override
    public void error(String format, Object arg1, Object arg2) {
        if (isErrorEnabled()) {
            FormattingTuple ft = MessageFormatter.format(format, arg1, arg2);
            error(new LogEvent(Level.ERROR, name, ft.getMessage(), Instant.now().toEpochMilli(), ft.getThrowable()));
        }
    }

    @Override
    public void error(String format, Object... arguments) {
        if (isErrorEnabled()) {
            FormattingTuple ft = MessageFormatter.format(format, arguments);
            error(new LogEvent(Level.ERROR, name, ft.getMessage(), Instant.now().toEpochMilli(), ft.getThrowable()));
        }
    }

    @Override
    public void error(String msg, Throwable t) {
        if (isErrorEnabled()) {
            error(new LogEvent(Level.ERROR, name, msg, Instant.now().toEpochMilli(), t));
        }
    }

    abstract protected void error(LogEvent event);
}

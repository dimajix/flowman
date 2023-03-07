/*
 * Copyright 2020-2023 Kaya Kupferschmidt
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

package com.dimajix.flowman.common;

import java.io.IOException;
import java.net.URL;
import java.util.Locale;
import java.util.Properties;

import lombok.val;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.properties.PropertiesConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Logging {
    private static Logger logger = null;
    private static String level = null;
    private static boolean color = true;

    private static Logger getLogger() {
        if (logger == null) {
            logger = LoggerFactory.getLogger(Logging.class);
        }
        return logger;
    }

    public static void init() {
        if (!isCustomLog4j2()) {
            initDefault();
        }
    }

    public static void setColorEnabled(boolean enabled) {
        color = enabled;
        if (!isCustomLog4j2()) {
            initDefault();
        }
    }

    private static void initDefault() {
        val loader = Thread.currentThread().getContextClassLoader();
        val url = loader.getResource("com/dimajix/flowman/log4j2-defaults.properties");
        LoggingImplLog4j2.init(url, color);
    }

    private static Properties loadProperties(URL url) {
        try {
            val urlConnection = url.openConnection();
            urlConnection.setUseCaches(false);
            val inputStream = urlConnection.getInputStream();
            try {
                val loaded = new Properties();
                loaded.load(inputStream);
                return loaded;
            }
            finally {
                inputStream.close();
            }
        } catch (IOException e) {
            getLogger().warn("Could not read configuration file from URL [" + url + "].", e);
            getLogger().warn("Ignoring configuration file [" + url + "].");
            return null;
        }
    }

    public static void setLogging(String logLevel) {
        getLogger().debug("Setting global log level to " + logLevel);
        level = logLevel.toUpperCase(Locale.ENGLISH);

        reconfigureLogging();
    }

    private static void reconfigureLogging() {
        // Create new logging properties with all levels being replaced
        if (level != null) {
            LoggingImplLog4j2.reconfigure(level);
        }
    }

    private static boolean isCustomLog4j2() {
        val log4jConfigFile = System.getProperty("log4j.configurationFile");
        val log4j2ConfigFile = System.getProperty("log4j2.configurationFile");
        return (log4jConfigFile != null || log4j2ConfigFile != null);
    }


/**
 * Utility class for isolating log4j 2.x dependencies. Otherwise a ClassNotFoundException may be thrown if log4j 2.x
 * is not on the classpath, even if the code is not executed.
 */
    private static class LoggingImplLog4j2 {
        public static void init(URL configUrl, boolean color) {
            val props = Logging.loadProperties(configUrl);
            // Disable ANSI magic by adding new entries to the log4j config
            if (!color) {
                for (val key : props.keySet()) {
                    if (key instanceof String) {
                        val skey = (String) key;
                        val parts = skey.split(".");
                        if (parts.length >= 3 && parts[0].equals("appender") && parts[2].equals("layout"))
                            props.setProperty("appender." + parts[1] + ".layout.disableAnsi", "true");
                    }
                }
            }
            val config = new PropertiesConfigurationBuilder()
                .setRootProperties(props)
                .build();
            val context = (LoggerContext)LogManager.getContext(false);
            context.start(config);
        }

        public static void reconfigure(String level) {
            val context =  (LoggerContext) LogManager.getContext(false);
            val l = Level.getLevel(level);
            val config = context.getConfiguration();
            config.getRootLogger().setLevel(l);
            config.getLoggers().values().forEach(x -> x.setLevel(l));
            context.updateLoggers();
        }
    }
}

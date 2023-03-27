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

package com.dimajix.flowman.common;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Locale;
import java.util.Properties;

import lombok.val;
import org.apache.log4j.PropertyConfigurator;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.properties.PropertiesConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.StaticLoggerBinder;


public final class Logging {
    private static Logger logger = null;
    private static Properties log4j1Properties = null;
    private static String level = null;
    private static boolean color = true;

    private Logging() {
    }

    private static Logger getLogger() {
        if (logger == null) {
            logger = LoggerFactory.getLogger(Logging.class);
        }
        return logger;
    }

    public static void init() {
        val log4jConfig = System.getProperty("log4j.configuration");
        if (isCustomLog4j2()) {
            // Do nothing, config is already correctly loaded
        }
        else if (log4jConfig != null) {
            URL url;
            try {
                url = new URL(log4jConfig);
            }
            catch (MalformedURLException ex) {
                try {
                    url = new File(log4jConfig).toURI().toURL();
                }
                catch (MalformedURLException ex2) {
                    url = null;
                }
            }

            if (url != null) {
                initlog4j1(url);
            }
        }
        else {
            initDefault();
        }
    }

    public static void setColorEnabled(boolean enabled) {
        color = enabled;
        if (isLog4j2() && !isCustomLog4j2()) {
            initDefault();
        }
    }

    private static void initDefault() {
        val loader = Thread.currentThread().getContextClassLoader();
        val log4j2cfg = loader.getResource("META-INF/flowman/conf/log4j2.properties");
        if (log4j2cfg != null) {
            LoggingImplLog4j2.init(log4j2cfg, color);
        }
        else {
            val log4jcfg = loader.getResource("META-INF/flowman/conf/log4j.properties");
            if (log4jcfg != null) {
                initlog4j1(log4jcfg);
            }
            else {
                if (isLog4j2()) {
                    val url = loader.getResource("com/dimajix/flowman/log4j2-defaults.properties");
                    LoggingImplLog4j2.init(url, color);
                } else {
                    val url = loader.getResource("com/dimajix/flowman/log4j-defaults.properties");
                    initlog4j1(url);
                }
            }
        }
    }

    private static void initlog4j1(URL configUrl) {
        log4j1Properties = loadProperties(configUrl);
        reconfigureLogging();
        getLogger().debug("Loaded logging configuration from " + configUrl);
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

    public static void setSparkLogging(String logLevel) {
        // Adjust Spark logging level
        getLogger().debug("Setting Spark log level to " + logLevel);

        val upperCased = logLevel.toUpperCase(Locale.ENGLISH);
        val l = org.apache.log4j.Level.toLevel(upperCased);
        org.apache.log4j.Logger.getLogger("org").setLevel(l);
        org.apache.log4j.Logger.getLogger("akka").setLevel(l);
        org.apache.log4j.Logger.getLogger("hive").setLevel(l);
    }

    public static void setLogging(String logLevel) {
        getLogger().debug("Setting global log level to " + logLevel);
        level = logLevel.toUpperCase(Locale.ENGLISH);

        reconfigureLogging();
    }

    private static void reconfigureLogging() {
        // Create new logging properties with all levels being replaced
        if (log4j1Properties != null) {
            LoggingImplLog4j1.reconfigure(log4j1Properties, level);
        }
        else if (isLog4j2()) {
            if (level != null) {
                LoggingImplLog4j2.reconfigure(level);
            }
        }

        if (level != null) {
            val l = org.apache.log4j.Level.toLevel(level);
            org.apache.log4j.Logger.getRootLogger().setLevel(l);
        }
    }

    private static boolean isLog4j2() {
        // This distinguishes the log4j 1.2 binding, currently
        // org.slf4j.impl.Log4jLoggerFactory, from the log4j 2.0 binding, currently
        // org.apache.logging.slf4j.Log4jLoggerFactory
        val binderClass = StaticLoggerBinder.getSingleton().getLoggerFactoryClassStr();
        return "org.apache.logging.slf4j.Log4jLoggerFactory".equals(binderClass);
    }

    private static boolean isCustomLog4j2() {
        val log4jConfigFile = System.getProperty("log4j.configurationFile");
        val log4j2ConfigFile = System.getProperty("log4j2.configurationFile");
        return isLog4j2() && (log4jConfigFile != null || log4j2ConfigFile != null);
    }


    private static class LoggingImplLog4j1 {
        public static void reconfigure(Properties props, String level) {
            val newProps = new Properties();
            props.forEach((k,v) -> newProps.setProperty((String)k, (String)v));
            if (level != null) {
                for (val key : newProps.keySet()) {
                    if (key instanceof String) {
                        val skey = (String)key;
                        if (skey.startsWith("log4j.logger."))
                            newProps.setProperty(skey, level);
                    }
                }
            }
            PropertyConfigurator.configure(newProps);
        }
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

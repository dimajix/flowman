/*
 * Copyright (C) 2023 The Flowman Authors
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

package com.dimajix.flowman.tools;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import lombok.val;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.LoggingEvent;
import org.yaml.snakeyaml.Yaml;

import com.dimajix.flowman.kernel.ClientFactory;
import com.dimajix.flowman.kernel.HistoryClient;
import com.dimajix.flowman.kernel.KernelClient;
import com.dimajix.flowman.kernel.SessionClient;
import com.dimajix.flowman.kernel.WorkspaceClient;
import com.dimajix.flowman.templating.Velocity;
import static com.dimajix.common.ExceptionUtils.reasons;
import static com.dimajix.common.text.ParserUtils.splitSetting;


public class RemoteTool {
    protected final Logger logger = LoggerFactory.getLogger(getClass());
    private final KernelClient _kernel;
    private final HistoryClient _history;
    private SessionClient _session= null;
    private String _projectLocation = null;
    private WorkspaceClient _workspace = null;
    private final Map<String,String> config;
    private final Map<String,String> environment;
    private final List<String> profiles;
    private Map<String,String> userConfig = Collections.emptyMap();
    private Map<String,String> userEnvironment = Collections.emptyMap();

    public RemoteTool(URI kernelUri, Map<String,String> config, Map<String,String> environment, List<String> profiles) {
        this.config = config;
        this.environment = environment;
        this.profiles = profiles;
        this._kernel =  ClientFactory.createClient(kernelUri);
        this._history = this._kernel.getHistory();
        this._workspace = openWorkspace(extractWorkspace(kernelUri));
        loadUserSettings();
    }

    public void shutdown() {
        _kernel.shutdown();
    }

    private static Optional<String> extractWorkspace(URI uri) {
        String path = uri.getPath().trim();
        while (path.length() > 0 && path.charAt(0) == '/')
            path = path.substring(1);

        if (path.isEmpty())
            return Optional.empty();
        else
            return Optional.of(path);
    }

    public WorkspaceClient  getWorkspace() { return _workspace; }
    public KernelClient getKernel() { return _kernel; }
    public SessionClient getSession() { return _session; }
    public HistoryClient getHistory() { return _history; }

    public ExecutionContext createContext() {
        return new ExecutionContext(getKernel(), getWorkspace(), getSession(), getHistory());
    }

    public String getPrompt() {
        val ctx = _session.getContext();
        val prefix = ctx.getProject().isPresent() ? ctx.getProject().get() : "";
        val suffix =
            ctx.getJob().isPresent() ?
                "/" + ctx.getJob().get().getName()
            : ctx.getTest().isPresent() ?
                "/" + ctx.getTest().get().getName()
            :
                "";
        return prefix + suffix;
    }

    public WorkspaceClient openWorkspace(Optional<String> workspaceName)  {
        if (workspaceName.isPresent()) {
            _workspace = _kernel.createWorkspace(workspaceName.get(), true);
        }
        else {
            _workspace = _kernel.createWorkspace();
        }
        return _workspace;
    }

    public SessionClient reloadSession() {
        return newSession(_projectLocation);
    }

    public SessionClient newSession(String projectLocation) {
        Map<String,String> allConfigs = new HashMap<>();
        allConfigs.putAll(userConfig);
        allConfigs.putAll(config);
        Map<String,String> allEnvironment = new HashMap<>();
        allEnvironment.putAll(userEnvironment);
        allEnvironment.putAll(environment);

        val isAbsolute = false; //FileSystem.getProtocol(projectLocation).nonEmpty || Tool.resolvePath(projectLocation).isAbsolute()
        val newSession =
            isAbsolute ?
                _kernel.createSession(projectLocation, allConfigs, allEnvironment, profiles)
            :
                _kernel.createSession(_workspace.getWorkspaceId(), projectLocation, allConfigs, allEnvironment, profiles);

        if (_session != null) {
            _session.shutdown();
        }
        _session = newSession;
        _projectLocation = projectLocation;

        newSession.subscribeLog(RemoteTool::logEvent);

        return _session;
    }

    private static void logEvent(LoggingEvent event) {
        val logger = LoggerFactory.getLogger(event.getLoggerName());
        switch(event.getLevel()) {
            case TRACE:
                val ex1 = event.getThrowable();
                if (ex1 != null)
                    logger.trace(event.getMessage(), ex1);
                else
                    logger.trace(event.getMessage());
                break;
            case DEBUG:
                val ex2 = event.getThrowable();
                if (ex2 != null)
                    logger.debug(event.getMessage(), ex2);
                else
                    logger.debug(event.getMessage());
                break;
            case INFO:
                val ex3 = event.getThrowable();
                if (ex3 != null)
                    logger.info(event.getMessage(), ex3);
                else
                    logger.info(event.getMessage());
                break;
            case WARN:
                val ex4 = event.getThrowable();
                if (ex4 != null)
                    logger.warn(event.getMessage(), ex4);
                else
                    logger.warn(event.getMessage());
                break;
            case ERROR:
                val ex5 = event.getThrowable();
                if (ex5 != null)
                    logger.error(event.getMessage(), ex5);
                else
                    logger.error(event.getMessage());
                break;
        }
    }

    private void loadUserSettings() {
        val settingsFile = new File(".flowman-env.yml");
        if (settingsFile.exists()) {
            try {
                logger.info("Loading user overrides from " + settingsFile.getAbsoluteFile().getCanonicalPath());
                val yaml = new Yaml();
                try (val inputStream = new FileInputStream(settingsFile)) {
                    Map<String,Object> yamlMap = yaml.loadAs(inputStream, Map.class);
                    val rawUserEnvironment = getKeyValuesArray(yamlMap, "environment");
                    val rawUserConfig = getKeyValuesArray(yamlMap, "config");

                    val vc = Velocity.newContext();
                    rawUserEnvironment.entrySet().forEach(e -> vc.put(e.getKey(), e.getValue()));

                    userEnvironment = rawUserEnvironment.entrySet().stream()
                        .map(kv -> Maps.immutableEntry(kv.getKey(), Velocity.evaluate(vc, "userEnvironment", kv.getValue())))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                    userConfig = rawUserConfig.entrySet().stream()
                        .map(kv -> Maps.immutableEntry(kv.getKey(), Velocity.evaluate(vc, "userConfig", kv.getValue())))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                }
            }
            catch (IOException ex) {
                logger.warn("Cannot load user settings '.flowman-env.yml':\n  "+ reasons(ex));
            }
        }
    }

    private static Map<String,String> getKeyValuesArray(Map<String,Object> root, String path) {
        val env = root.get(path);
        if (env != null && env instanceof List) {
            val array = (List)env;
            val result = new HashMap<String,String>();
            for (val entry : array) {
                if (entry instanceof String) {
                    val me = splitSetting((String)entry);
                    result.put(me.getKey(), me.getValue());
                }
            }
            return result;
        }
        else {
            return Collections.emptyMap();
        }
    }
}

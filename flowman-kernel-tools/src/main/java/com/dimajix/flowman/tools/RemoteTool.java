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

package com.dimajix.flowman.tools;

import java.net.URI;
import java.util.List;
import java.util.Map;

import lombok.val;

import com.dimajix.flowman.kernel.ClientFactory;
import com.dimajix.flowman.kernel.KernelClient;
import com.dimajix.flowman.kernel.SessionClient;
import com.dimajix.flowman.kernel.WorkspaceClient;


public class RemoteTool {
    private final KernelClient _kernel;
    private SessionClient _session= null;
    private WorkspaceClient _workspace = null;
    private final Map<String,String> config;
    private final Map<String,String> environment;
    private final List<String> profiles;

    public RemoteTool(URI kernelUri, Map<String,String> config, Map<String,String> environment, List<String> profiles) {
        this.config = config;
        this.environment = environment;
        this.profiles = profiles;
        this._kernel =  ClientFactory.createClient(kernelUri);
        this._workspace = openWorkspace(extractWorkspace(kernelUri));
    }

    private static String extractWorkspace(URI uri) {
        String path = uri.getPath().trim();
        while (path.length() > 0 && path.charAt(0) == '/')
            path = path.substring(1);

        if (path.isEmpty())
            return "default";
        else
            return path;
    }

    public WorkspaceClient  getWorkspace() { return _workspace; }
    public KernelClient getKernel() { return _kernel; }
    public SessionClient getSession() { return _session; }

    public String getContext() {
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

    public WorkspaceClient openWorkspace(String workspaceName)  {
        _workspace = _kernel.createWorkspace(workspaceName, true);
        return _workspace;
    }

    public SessionClient newSession(String projectLocation) {
        val isAbsolute = true; //FileSystem.getProtocol(projectLocation).nonEmpty || Tool.resolvePath(projectLocation).isAbsolute()
        val newSession =
            isAbsolute ?
                _kernel.createSession(projectLocation, config, environment, profiles)
            :
                _kernel.createSession(_workspace.getWorkspaceName(), projectLocation, config, environment, profiles);

        if (_session != null) {
            _session.shutdown();
        }
        _session = newSession;
        return _session;
    }
}

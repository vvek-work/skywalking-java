/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.skywalking.apm.agent.core.context;

import org.apache.skywalking.apm.agent.core.boot.BootService;
import org.apache.skywalking.apm.agent.core.boot.DefaultImplementor;
import org.apache.skywalking.apm.agent.core.boot.ServiceManager;
import org.apache.skywalking.apm.agent.core.conf.Config;
import org.apache.skywalking.apm.agent.core.conf.dynamic.ConfigurationDiscoveryService;
import org.apache.skywalking.apm.agent.core.conf.dynamic.watcher.IgnoreSuffixPatternsWatcher;
import org.apache.skywalking.apm.agent.core.conf.dynamic.watcher.SpanLimitWatcher;
import org.apache.skywalking.apm.agent.core.sampling.SamplingService;
import org.apache.skywalking.apm.util.StringUtil;

import java.util.Arrays;

@DefaultImplementor
public class ContextManagerExtendService implements BootService {

    private volatile String[] ignoreSuffixArray = new String[0];

    private IgnoreSuffixPatternsWatcher ignoreSuffixPatternsWatcher;

    private SpanLimitWatcher spanLimitWatcher;

    @Override
    public void prepare() {
    }

    @Override
    public void boot() {
        ignoreSuffixArray = Config.Agent.IGNORE_SUFFIX.split(",");
        ignoreSuffixPatternsWatcher = new IgnoreSuffixPatternsWatcher("agent.ignore_suffix", this);
        spanLimitWatcher = new SpanLimitWatcher("agent.span_limit_per_segment");

        ConfigurationDiscoveryService configurationDiscoveryService = ServiceManager.INSTANCE.findService(
            ConfigurationDiscoveryService.class);
        configurationDiscoveryService.registerAgentConfigChangeWatcher(spanLimitWatcher);
        configurationDiscoveryService.registerAgentConfigChangeWatcher(ignoreSuffixPatternsWatcher);

        handleIgnoreSuffixPatternsChanged();
    }

    @Override
    public void onComplete() {

    }

    @Override
    public void shutdown() {

    }

    public AbstractTracerContext createTraceContext(String operationName, boolean forceSampling) {
        AbstractTracerContext context;
        /*
         * Don't trace anything if the backend is not available.
         */

        int suffixIdx = operationName.lastIndexOf(".");
        if (suffixIdx > -1 && Arrays.stream(ignoreSuffixArray)
                                    .anyMatch(a -> a.equals(operationName.substring(suffixIdx)))) {
            context = new IgnoredTracerContext();
        } else {
            SamplingService samplingService = ServiceManager.INSTANCE.findService(SamplingService.class);
            if (forceSampling || samplingService.trySampling(operationName)) {
                context = new TracingContext(operationName, spanLimitWatcher);
            } else {
                context = new IgnoredTracerContext();
            }
        }

        return context;
    }

    public void handleIgnoreSuffixPatternsChanged() {
        if (StringUtil.isNotBlank(ignoreSuffixPatternsWatcher.getIgnoreSuffixPatterns())) {
            ignoreSuffixArray = ignoreSuffixPatternsWatcher.getIgnoreSuffixPatterns().split(",");
        }
    }
}

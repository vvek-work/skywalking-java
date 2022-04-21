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

package org.apache.skywalking.apm.agent.core.conf.dynamic;

import com.google.common.collect.Lists;
import lombok.Getter;
import org.apache.skywalking.apm.agent.core.boot.BootService;
import org.apache.skywalking.apm.agent.core.boot.DefaultImplementor;
import org.apache.skywalking.apm.agent.core.boot.DefaultNamedThreadFactory;
import org.apache.skywalking.apm.agent.core.conf.Config;
import org.apache.skywalking.apm.agent.core.logging.api.ILog;
import org.apache.skywalking.apm.agent.core.logging.api.LogManager;
import org.apache.skywalking.apm.network.common.v3.KeyStringValuePair;
import org.apache.skywalking.apm.network.language.agent.v3.ConfigurationDiscoveryServiceGrpc;
import org.apache.skywalking.apm.network.trace.component.command.ConfigurationDiscoveryCommand;
import org.apache.skywalking.apm.util.RunnableWithExceptionProtection;
import org.apache.skywalking.apm.util.StringUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

@DefaultImplementor
public class ConfigurationDiscoveryService implements BootService {

    /**
     * UUID of the last return value.
     */
    private String uuid;
    private final Register register = new Register();

    private volatile int lastRegisterWatcherSize;

    private volatile ScheduledFuture<?> getDynamicConfigurationFuture;
    private volatile ConfigurationDiscoveryServiceGrpc.ConfigurationDiscoveryServiceBlockingStub configurationDiscoveryServiceBlockingStub;

    private static final ILog LOGGER = LogManager.getLogger(ConfigurationDiscoveryService.class);

    @Override
    public void prepare() throws Throwable {
    }

    @Override
    public void boot() throws Throwable {
        getDynamicConfigurationFuture = Executors.newSingleThreadScheduledExecutor(
            new DefaultNamedThreadFactory("ConfigurationDiscoveryService")
        ).scheduleAtFixedRate(
            new RunnableWithExceptionProtection(
                this::getAgentDynamicConfig,
                t -> LOGGER.error("Sync config from OAP error.", t)
            ),
            Config.Collector.GET_AGENT_DYNAMIC_CONFIG_INTERVAL,
            Config.Collector.GET_AGENT_DYNAMIC_CONFIG_INTERVAL,
            TimeUnit.SECONDS
        );
    }

    @Override
    public void onComplete() throws Throwable {

    }

    @Override
    public void shutdown() throws Throwable {
        if (getDynamicConfigurationFuture != null) {
            getDynamicConfigurationFuture.cancel(true);
        }
    }

    /**
     * Register dynamic configuration watcher.
     *
     * @param watcher dynamic configuration watcher
     */
    public void registerAgentConfigChangeWatcher(AgentConfigChangeWatcher watcher) {
        WatcherHolder holder = new WatcherHolder(watcher);
        if (register.containsKey(holder.getKey())) {
            throw new IllegalStateException("Duplicate register, watcher=" + watcher);
        }
        register.put(holder.getKey(), holder);
    }

    /**
     * Process ConfigurationDiscoveryCommand and notify each configuration watcher.
     *
     * @param configurationDiscoveryCommand Describe dynamic configuration information
     */
    public void handleConfigurationDiscoveryCommand(ConfigurationDiscoveryCommand configurationDiscoveryCommand) {
        final String responseUuid = configurationDiscoveryCommand.getUuid();

        if (responseUuid != null && Objects.equals(this.uuid, responseUuid)) {
            return;
        }

        List<KeyStringValuePair> config = readConfig(configurationDiscoveryCommand);

        config.forEach(property -> {
            String propertyKey = property.getKey();
            WatcherHolder holder = register.get(propertyKey);
            if (holder != null) {
                AgentConfigChangeWatcher watcher = holder.getWatcher();
                String newPropertyValue = property.getValue();
                if (StringUtil.isBlank(newPropertyValue)) {
                    if (watcher.value() != null) {
                        // Notify watcher, the new value is null with delete event type.
                        watcher.notify(
                            new AgentConfigChangeWatcher.ConfigChangeEvent(
                                null, AgentConfigChangeWatcher.EventType.DELETE
                            ));
                    } else {
                        // Don't need to notify, stay in null.
                    }
                } else {
                    if (!newPropertyValue.equals(watcher.value())) {
                        watcher.notify(new AgentConfigChangeWatcher.ConfigChangeEvent(
                            newPropertyValue, AgentConfigChangeWatcher.EventType.MODIFY
                        ));
                    } else {
                        // Don't need to notify, stay in the same config value.
                    }
                }
            } else {
                LOGGER.warn("Config {} from OAP, doesn't match any watcher, ignore.", propertyKey);
            }
        });
        this.uuid = responseUuid;

        LOGGER.trace("Current configurations after the sync, configurations:{}", register.toString());
    }

    /**
     * Read the registered dynamic configuration, compare it with the dynamic configuration information returned by the
     * service, and complete the dynamic configuration that has been deleted on the OAP.
     *
     * @param configurationDiscoveryCommand Describe dynamic configuration information
     * @return Adapted dynamic configuration information
     */
    private List<KeyStringValuePair> readConfig(ConfigurationDiscoveryCommand configurationDiscoveryCommand) {
        Map<String, KeyStringValuePair> commandConfigs = configurationDiscoveryCommand.getConfig()
                                                                                      .stream()
                                                                                      .collect(Collectors.toMap(
                                                                                          KeyStringValuePair::getKey,
                                                                                          Function.identity()
                                                                                      ));
        List<KeyStringValuePair> configList = Lists.newArrayList();
        for (final String name : register.keys()) {
            KeyStringValuePair command = commandConfigs.getOrDefault(name, KeyStringValuePair.newBuilder()
                                                                                             .setKey(name)
                                                                                             .build());
            configList.add(command);
        }
        return configList;
    }

    /**
     * get agent dynamic config through gRPC.
     */
    private void getAgentDynamicConfig() {
    }

    /**
     * Local dynamic configuration center.
     */
    public static class Register {
        private final Map<String, WatcherHolder> register = new HashMap<>();

        private boolean containsKey(String key) {
            return register.containsKey(key);
        }

        private void put(String key, WatcherHolder holder) {
            register.put(key, holder);
        }

        public WatcherHolder get(String name) {
            return register.get(name);
        }

        public Set<String> keys() {
            return register.keySet();
        }

        @Override
        public String toString() {
            ArrayList<String> registerTableDescription = new ArrayList<>(register.size());
            register.forEach((key, holder) -> {
                AgentConfigChangeWatcher watcher = holder.getWatcher();
                registerTableDescription.add(new StringBuilder().append("key:")
                                                                .append(key)
                                                                .append("value(current):")
                                                                .append(watcher.value()).toString());
            });
            return registerTableDescription.stream().collect(Collectors.joining(",", "[", "]"));
        }
    }

    @Getter
    private static class WatcherHolder {
        private final AgentConfigChangeWatcher watcher;
        private final String key;

        public WatcherHolder(AgentConfigChangeWatcher watcher) {
            this.watcher = watcher;
            this.key = watcher.getPropertyKey();
        }
    }
}

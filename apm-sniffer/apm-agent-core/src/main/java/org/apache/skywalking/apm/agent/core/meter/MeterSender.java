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

package org.apache.skywalking.apm.agent.core.meter;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.skywalking.apm.agent.core.boot.BootService;
import org.apache.skywalking.apm.agent.core.boot.DefaultImplementor;
import org.apache.skywalking.apm.agent.core.conf.Config;
import org.apache.skywalking.apm.agent.core.logging.api.ILog;
import org.apache.skywalking.apm.agent.core.logging.api.LogManager;
import org.apache.skywalking.apm.network.common.v3.Commands;
import org.apache.skywalking.apm.network.language.agent.v3.MeterData;
import org.apache.skywalking.apm.network.language.agent.v3.MeterReportServiceGrpc;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.apache.skywalking.apm.agent.core.conf.Config.Collector.GRPC_UPSTREAM_TIMEOUT;

/**
 * MeterSender collects the values of registered meter instances, and sends to the backend.
 */
@DefaultImplementor
public class MeterSender implements BootService {
    private static final ILog LOGGER = LogManager.getLogger(MeterSender.class);

    private volatile MeterReportServiceGrpc.MeterReportServiceStub meterReportServiceStub;

    @Override
    public void prepare() {
    }

    @Override
    public void boot() {

    }

    public void send(Map<MeterId, BaseMeter> meterMap, MeterService meterService) {
        if (true) {
            StreamObserver<MeterData> reportStreamObserver = null;
            try {
                reportStreamObserver = meterReportServiceStub.withDeadlineAfter(
                        GRPC_UPSTREAM_TIMEOUT, TimeUnit.SECONDS
                ).collect(new StreamObserver<Commands>() {
                    @Override
                    public void onNext(Commands commands) {
                    }

                    @Override
                    public void onError(Throwable throwable) {

                        if (LOGGER.isErrorEnable()) {
                            LOGGER.error(throwable, "Send meters to collector fail with a grpc internal exception.");
                        }
                    }

                    @Override
                    public void onCompleted() {

                    }
                });

                final StreamObserver<MeterData> reporter = reportStreamObserver;
                transform(meterMap, meterData -> reporter.onNext(meterData));
            } catch (Throwable e) {
                if (!(e instanceof StatusRuntimeException)) {
                    LOGGER.error(e, "Report meters to backend fail.");
                    return;
                }
                final StatusRuntimeException statusRuntimeException = (StatusRuntimeException) e;
                if (statusRuntimeException.getStatus().getCode() == Status.Code.UNIMPLEMENTED) {
                    LOGGER.warn("Backend doesn't support meter, it will be disabled");

                    meterService.shutdown();
                }
            } finally {
                if (reportStreamObserver != null) {
                    reportStreamObserver.onCompleted();
                }
            }
        }
    }

    protected void transform(final Map<MeterId, BaseMeter> meterMap,
                             final Consumer<MeterData> consumer) {
        // build and report meters
        boolean hasSendMachineInfo = false;
        for (BaseMeter meter : meterMap.values()) {
            final MeterData.Builder dataBuilder = meter.transform();
            if (dataBuilder == null) {
                continue;
            }

            // only send the service base info at the first data
            if (!hasSendMachineInfo) {
                dataBuilder.setService(Config.Agent.SERVICE_NAME);
                dataBuilder.setServiceInstance(Config.Agent.INSTANCE_NAME);
                dataBuilder.setTimestamp(System.currentTimeMillis());
                hasSendMachineInfo = true;
            }

            consumer.accept(dataBuilder.build());
        }
    }

    @Override
    public void onComplete() {

    }

    @Override
    public void shutdown() {

    }
}

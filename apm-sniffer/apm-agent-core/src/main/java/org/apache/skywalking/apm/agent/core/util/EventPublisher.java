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

package org.apache.skywalking.apm.agent.core.util;

import org.apache.skywalking.apm.agent.core.logging.api.ILog;
import org.apache.skywalking.apm.agent.core.logging.api.LogManager;
import org.zeromq.SocketType;
import org.zeromq.ZMQ;

import java.util.concurrent.LinkedBlockingQueue;

public class EventPublisher implements Runnable {

    private static final ILog LOGGER = LogManager.getLogger(EventPublisher.class);

    private static final LinkedBlockingQueue<String> BLOCKING_QUEUE = new LinkedBlockingQueue<>();

    private final ZMQ.Context context;

    private final ZMQ.Socket publisher;

    private static boolean IS_RUNNING;

    public EventPublisher() {
        context = ZMQ.context(1);
        publisher = context.socket(SocketType.PUSH);
        publisher.setSendTimeOut(0);
        publisher.setTCPKeepAlive(1);
        publisher.setLinger(0);
        publisher.connect("tcp://localhost:9449"); // TODO: 24/03/22 : add it to config file
        IS_RUNNING = true;
        LOGGER.info("publisher socket has been created");
    }

    @Override
    public void run() {
        while (IS_RUNNING) {
            try {
                if (!BLOCKING_QUEUE.isEmpty()) {
                    this.publish(BLOCKING_QUEUE.take());
                }
            } catch (Exception exception) {
                LOGGER.error("failed to publish event, {}", exception);
            }
        }
        LOGGER.info("event publisher has been stopped");
    }

    /**
     * queue apm metric for publisher
     *
     * @param event metric event
     */
    public static void queue(String event) {
        try {
            BLOCKING_QUEUE.add(event);

            if (LOGGER.isDebugEnable()) {
                LOGGER.debug("event Queued: " + event);
            }
        } catch (Exception exception) {
            LOGGER.error("failed to queue event, {}", exception);
        }
    }

    /**
     * Publish event to PULL socket @AIOps
     *
     * @param event apm metric data
     */
    private void publish(String event) {
        publisher.send(event);

        if (LOGGER.isDebugEnable()) {
            LOGGER.debug("event published: " + event);
        }
    }

    /**
     * Close Socket and Context
     */
    public void close() {
        publisher.close();
        context.close();

        LOGGER.info("socket closed");
    }

    /**
     * Shutdown the Event Publisher Thread
     */
    public static void shutdown() {
        LOGGER.info("shutdown request received");
        IS_RUNNING = false;
    }
}

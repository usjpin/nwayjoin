/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package usjpin.flink.examples;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.function.SerializableFunction;
import usjpin.flink.*;

import java.util.*;
import java.time.Instant;

/**
 * Purchase Attribution Joiner example using NWayJoiner.
 * 
 * This example demonstrates how to join click events with purchase events,
 * attributing purchases to clicks that happened before the purchase.
 * The state retention is set to 30 minutes.
 */
public class PurchaseAttributionJoiner {

    public static void main(String[] args) throws Exception {
        // Sets up the execution environment, which is the main entry point
        // to building Flink applications.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Create data streams for clicks and purchases
        DataStream<ClickEvent> clickStream = env.addSource(new ClickEventSource());
        DataStream<PurchaseEvent> purchaseStream = env.addSource(new PurchaseEventSource());

        // Define how to extract join keys from events (using user ID & product ID)
        SerializableFunction<ClickEvent, String> clickKeyExtractor = (ClickEvent event) -> event.getUserId() + "|" + event.getProductId();
        SerializableFunction<PurchaseEvent, String> purchaseKeyExtractor = (PurchaseEvent event) -> event.getUserId() + "|" + event.getProductId();

        // Configure the streams
        StreamConfig<ClickEvent> clickStreamConfig = StreamConfig.<ClickEvent>builder()
                .withStream("clicks", clickStream, ClickEvent.class)
                .joinKeyExtractor(clickKeyExtractor)
                .build();

        StreamConfig<PurchaseEvent> purchaseStreamConfig = StreamConfig.<PurchaseEvent>builder()
                .withStream("purchases", purchaseStream, PurchaseEvent.class)
                .joinKeyExtractor(purchaseKeyExtractor)
                .build();

        // 30 minutes retention in milliseconds
        long thirtyMinutesMs = 30 * 60 * 1000L;

        // Configure the joiner
        JoinerConfig<AttributionResult> joinerConfig = JoinerConfig.<AttributionResult>builder()
                .addStreamConfig(clickStreamConfig)
                .addStreamConfig(purchaseStreamConfig)
                .outClass(AttributionResult.class)
                .stateRetentionMs(thirtyMinutesMs)
                .joinLogic(new PurchaseAttributionLogic())
                .build();

        // Create the joined stream
        DataStream<AttributionResult> attributionResults = NWayJoiner.create(joinerConfig);

        // Print the results
        attributionResults.print();

        // Execute program, beginning computation.
        env.execute("PurchaseAttributionJoiner Example");
    }

    // Event classes and source implementations

    public static class ClickEvent {
        private final String userId;
        private final String productId;
        private final long timestamp;

        public ClickEvent(String userId, String productId, long timestamp) {
            this.userId = userId;
            this.productId = productId;
            this.timestamp = timestamp;
        }

        public String getUserId() {
            return userId;
        }

        public String getProductId() {
            return productId;
        }

        public long getTimestamp() {
            return timestamp;
        }

        @Override
        public String toString() {
            return "Click{userId='" + userId + "', productId='" + productId + "', timestamp=" + timestamp + "}";
        }
    }

    public static class PurchaseEvent {
        private final String userId;
        private final String productId;
        private final long timestamp;
        private final double amount;

        public PurchaseEvent(String userId, String productId, long timestamp, double amount) {
            this.userId = userId;
            this.productId = productId;
            this.timestamp = timestamp;
            this.amount = amount;
        }

        public String getUserId() {
            return userId;
        }

        public String getProductId() {
            return productId;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public double getAmount() {
            return amount;
        }

        @Override
        public String toString() {
            return "Purchase{userId='" + userId + "', productId='" + productId + 
                   "', timestamp=" + timestamp + ", amount=" + amount + "}";
        }
    }

    public static class AttributionResult {
        private final String clickUserId;
        private final String purchaseUserId;
        private final String clickedProductId;
        private final String purchasedProductId;
        private final long clickTimestamp;
        private final long purchaseTimestamp;
        private final double purchaseAmount;

        public AttributionResult(
                String clickUserId,
                String purchaseUserId,
                String clickedProductId,
                String purchasedProductId,
                long clickTimestamp,
                long purchaseTimestamp,
                double purchaseAmount) {
            this.clickUserId = clickUserId;
            this.purchaseUserId = purchaseUserId;
            this.clickedProductId = clickedProductId;
            this.purchasedProductId = purchasedProductId;
            this.clickTimestamp = clickTimestamp;
            this.purchaseTimestamp = purchaseTimestamp;
            this.purchaseAmount = purchaseAmount;
        }

        @Override
        public String toString() {
            return "Attribution{clickUserId='" + clickUserId +
                   "', purchaseUserId='" + purchaseUserId +
                   "', clickedProduct='" + clickedProductId + 
                   "', purchasedProduct='" + purchasedProductId + 
                   "', clickTime=" + Instant.ofEpochMilli(clickTimestamp) + 
                   ", purchaseTime=" + Instant.ofEpochMilli(purchaseTimestamp) + 
                   ", timeDiff=" + (purchaseTimestamp - clickTimestamp) / 1000 + "s" +
                   ", amount=" + purchaseAmount + "}";
        }
    }

    // Source implementations to generate test data
    public static class ClickEventSource implements SourceFunction<ClickEvent> {
        private boolean running = true;
        private final Random random = new Random();
        private final String[] userIds = {"user1", "user2", "user3", "user4", "user5"};
        private final String[] productIds = {"product1", "product2", "product3", "product4", "product5"};

        @Override
        public void run(SourceContext<ClickEvent> ctx) throws Exception {
            while (running) {
                String userId = userIds[random.nextInt(userIds.length)];
                String productId = productIds[random.nextInt(productIds.length)];
                long timestamp = System.currentTimeMillis();

                ctx.collect(new ClickEvent(userId, productId, timestamp));

                // Sleep between 500ms and 3s
                Thread.sleep(500 + random.nextInt(2500));
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    public static class PurchaseEventSource implements SourceFunction<PurchaseEvent> {
        private boolean running = true;
        private final Random random = new Random();
        private final String[] userIds = {"user1", "user2", "user3", "user4", "user5"};
        private final String[] productIds = {"product1", "product2", "product3", "product4", "product5"};

        @Override
        public void run(SourceContext<PurchaseEvent> ctx) throws Exception {
            while (running) {
                String userId = userIds[random.nextInt(userIds.length)];
                String productId = productIds[random.nextInt(productIds.length)];
                long timestamp = System.currentTimeMillis();
                double amount = 10.0 + random.nextDouble() * 90.0; // Random amount between 10 and 100

                ctx.collect(new PurchaseEvent(userId, productId, timestamp, amount));

                // Sleep between 2s and 8s (purchases happen less frequently than clicks)
                Thread.sleep(2000 + random.nextInt(6000));
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    static class PurchaseAttributionLogic implements JoinLogic<AttributionResult> {
        @Override
        public AttributionResult apply(JoinerState joinerState) {
            Map<String, SortedSet<JoinableEvent<?>>> state = joinerState.getState();

            // Check if we have both clicks and purchases for this user
            if (!state.containsKey("clicks") || !state.containsKey("purchases")) {
                return null;
            }

            SortedSet<JoinableEvent<?>> clickEvents = state.get("clicks");
            SortedSet<JoinableEvent<?>> purchaseEvents = state.get("purchases");

            // Get the latest purchase
            JoinableEvent<?> latestPurchaseEvent = purchaseEvents.last();
            PurchaseEvent purchase = (PurchaseEvent) latestPurchaseEvent.getEvent();
            long purchaseTimestamp = latestPurchaseEvent.getTimestamp();

            // Find the most recent click that happened before the purchase
            JoinableEvent<?> attributedClickEvent = null;
            ClickEvent attributedClick = null;
            long clickTimestamp = 0;

            for (JoinableEvent<?> clickEvent : clickEvents) {
                if (clickEvent.getTimestamp() < purchaseTimestamp) {
                    ClickEvent click = (ClickEvent) clickEvent.getEvent();
                    if (attributedClick == null || clickEvent.getTimestamp() > clickTimestamp) {
                        attributedClickEvent = clickEvent;
                        attributedClick = click;
                        clickTimestamp = clickEvent.getTimestamp();
                    }
                }
            }

            // If we found a click that happened before the purchase, create an attribution result
            if (attributedClick != null) {
                // Clean up the attributed click & purchase event
                clickEvents.remove(attributedClickEvent);
                purchaseEvents.remove(latestPurchaseEvent);

                return new AttributionResult(
                        attributedClick.getUserId(),
                        purchase.getUserId(),
                        attributedClick.getProductId(),
                        purchase.getProductId(),
                        attributedClick.getTimestamp(),
                        purchase.getTimestamp(),
                        purchase.getAmount()
                );
            }

            return null;
        }
    }
}

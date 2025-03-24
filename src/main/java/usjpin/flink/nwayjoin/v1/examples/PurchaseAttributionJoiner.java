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

 package usjpin.flink.nwayjoin.v1.examples;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.function.SerializableFunction;
import usjpin.flink.nwayjoin.v1.*;

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
                .cleanupIntervalMs(300_000L) // Set to 5 minutes
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
            return "Purchase{userId='" + userId + "', productId='" + productId + "', timestamp=" + timestamp + ", amount=" + amount + "}";
        }
    }

    public static class AttributionResult {
        private final String clickUserId;
        private final String purchaseUserId;
        private final String clickProductId;
        private final String purchaseProductId;
        private final long clickTimestamp;
        private final long purchaseTimestamp;
        private final double purchaseAmount;

        public AttributionResult(
                String clickUserId,
                String purchaseUserId,
                String clickProductId,
                String purchaseProductId,
                long clickTimestamp,
                long purchaseTimestamp,
                double purchaseAmount) {
            this.clickUserId = clickUserId;
            this.purchaseUserId = purchaseUserId;
            this.clickProductId = clickProductId;
            this.purchaseProductId = purchaseProductId;
            this.clickTimestamp = clickTimestamp;
            this.purchaseTimestamp = purchaseTimestamp;
            this.purchaseAmount = purchaseAmount;
        }

        @Override
        public String toString() {
            return "Attribution{" +
                    "clickUser='" + clickUserId + '\'' +
                    ", purchaseUser='" + purchaseUserId + '\'' +
                    ", clickProduct='" + clickProductId + '\'' +
                    ", purchaseProduct='" + purchaseProductId + '\'' +
                    ", clickTime=" + Instant.ofEpochMilli(clickTimestamp) +
                    ", purchaseTime=" + Instant.ofEpochMilli(purchaseTimestamp) +
                    ", amount=" + purchaseAmount +
                    '}';
        }
    }

    static class ClickEventSource implements SourceFunction<ClickEvent> {
        private boolean running = true;
        private final Random random = new Random();
        
        @Override
        public void run(SourceContext<ClickEvent> ctx) throws Exception {
            String[] userIds = {"user1", "user2", "user3", "user4", "user5"};
            String[] productIds = {"product1", "product2", "product3", "product4", "product5"};
            
            int count = 0;
            while (running && count < 100) {
                String userId = userIds[random.nextInt(userIds.length)];
                String productId = productIds[random.nextInt(productIds.length)];
                long timestamp = System.currentTimeMillis();
                
                ctx.collect(new ClickEvent(userId, productId, timestamp));
                
                Thread.sleep(500);
                count++;
            }
        }
        
        @Override
        public void cancel() {
            running = false;
        }
    }
    
    static class PurchaseEventSource implements SourceFunction<PurchaseEvent> {
        private boolean running = true;
        private final Random random = new Random();
        
        @Override
        public void run(SourceContext<PurchaseEvent> ctx) throws Exception {
            String[] userIds = {"user1", "user2", "user3", "user4", "user5"};
            String[] productIds = {"product1", "product2", "product3", "product4", "product5"};
            
            // Start after a delay to allow some clicks to happen first
            Thread.sleep(2000);
            
            int count = 0;
            while (running && count < 100) {
                String userId = userIds[random.nextInt(userIds.length)];
                String productId = productIds[random.nextInt(productIds.length)];
                long timestamp = System.currentTimeMillis();
                double amount = 10.0 + random.nextDouble() * 90.0;
                
                ctx.collect(new PurchaseEvent(userId, productId, timestamp, amount));
                
                Thread.sleep(1000);
                count++;
            }
        }
        
        @Override
        public void cancel() {
            running = false;
        }
    }

    static class PurchaseAttributionLogic implements JoinLogic<AttributionResult> {
        @Override
        public void apply(NWayJoinerContext<AttributionResult> context) {
            JoinerState state = context.getState();
            Map<String, NavigableSet<JoinableEvent<?>>> stateMap = state.getState();

            // Check if we have both clicks and purchases for this user
            if (!stateMap.containsKey("clicks") || !stateMap.containsKey("purchases")) {
                return;
            }

            NavigableSet<JoinableEvent<?>> clickEvents = stateMap.get("clicks");
            NavigableSet<JoinableEvent<?>> purchaseEvents = stateMap.get("purchases");
            
            if (purchaseEvents.isEmpty()) {
                return;
            }

            // Process each purchase event
            Iterator<JoinableEvent<?>> purchaseIterator = purchaseEvents.iterator();
            while (purchaseIterator.hasNext()) {
                JoinableEvent<?> purchaseEvent = purchaseIterator.next();
                PurchaseEvent purchase = (PurchaseEvent) purchaseEvent.getEvent();
                long purchaseTimestamp = purchaseEvent.getTimestamp();
                
                // Find the most recent click before this purchase
                JoinableEvent<?> attributedClickEvent = state.getMostRecentEventBefore("clicks", purchaseTimestamp);
                
                if (attributedClickEvent != null) {
                    ClickEvent attributedClick = (ClickEvent) attributedClickEvent.getEvent();
                    
                    // Create attribution result
                    AttributionResult result = new AttributionResult(
                            attributedClick.getUserId(),
                            purchase.getUserId(),
                            attributedClick.getProductId(),
                            purchase.getProductId(),
                            attributedClick.getTimestamp(),
                            purchase.getTimestamp(),
                            purchase.getAmount()
                    );
                    
                    // Emit the result
                    context.emit(result);
                    
                    // Remove the processed events
                    clickEvents.remove(attributedClickEvent);
                    purchaseIterator.remove();
                }
            }
            
            // Clean up empty sets
            if (clickEvents.isEmpty()) stateMap.remove("clicks");
            if (purchaseEvents.isEmpty()) stateMap.remove("purchases");
            
            // Update the state
            context.updateState(state);
        }
    }
}

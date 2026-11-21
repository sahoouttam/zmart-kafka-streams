package com.zmart.stream.processor;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.zmart.stream.processor.AdvancedTopics.*;

public class ZMartKafkaStreamsAdvanced {
    
    private static final Logger log = LoggerFactory.getLogger(ZMartKafkaStreamsAdvanced.class);

    private static final String CC_MASK = "xxxx-xxxx-xxxx-";
    private static final double HIGH_VALUE_THRESHOLD = 500.0;

    private record PurchaseWithCustomer(Purchase purchase, Customer customer) {}

    public Topology topology(Properties properties) {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Purchase> events = builder.stream(PURCHASE_EVENTS,
                Consumed.with(STRING, PURCHASE_SERDE)
                        .withTimestampExtractor(new PurchaseTimestampExtractor()));
        Map<String, KStream<String, Purchase>> branches = events.split(Named.as("validate-"))
                        .branch((key, purchase) -> !isValid(purchase),
                            Branched.<String, Purchase>withConsumer(invalid -> 
                                invalid.to(PURCHASES_DLQ, Produced.with(
                                    STRING, PURCHASE_SERDE)), "invalid"))
                        .defaultBranch(Branched.as("valid"));
        KStream<String, Purchase> validStream = branches.get("validate-valid");

        KStream<String, Purchase> maskedStream = validStream.mapValues(this::mask);
        maskedStream.to(PURCHASES_MASKED, Produced.with(STRING, PURCHASE_SERDE));
        maskedStream.split(Named.as("dept-"))
                    .branch((key, purchase) -> "cafe".equals(purchase.department()),
                        Branched.<String, Purchase>withConsumer(
                            stream -> stream.to(PURCHASES_CAFE, 
                                Produced.with(STRING, PURCHASE_SERDE))))
                    .branch((key, purchase) -> "electronics".equals(purchase.department()),
                        Branched.<String, Purchase>withConsumer(
                            stream -> stream.to(PURCHASES_ELECTRONICS, 
                                Produced.with(STRING, PURCHASE_SERDE))))
                    .defaultBranch(Branched.<String, Purchase>withConsumer(
                        stream -> stream.to(PURCHASES_OTHER, 
                            Produced.with(STRING, PURCHASE_SERDE))));
        maskedStream.filter((key, purchase) -> purchase.total() >= HIGH_VALUE_THRESHOLD)
                    .to(PURCHASES_HIGH_VALUE, Produced.with(STRING, PURCHASE_SERDE));
        
        maskedStream.groupByKey(Grouped.with(STRING, PURCHASE_SERDE))
                    .aggregate(() -> new Reward(null, 0, 0), 
                            (customerId, purchase, aggregate) -> new Reward(
                                customerId, 
                                aggregate.totalPoints() + (long) purchase.total(), 
                                aggregate.purchaseCount() + 1),
                            Materialized.<String, Reward, KeyValueStore<Bytes, byte[]>>as(REWARDS_STORE)
                                    .withKeySerde(STRING)
                                    .withValueSerde(REWARD_SERDE))
                            .toStream()
                            .to(REWARD_TOTALS, Produced.with(STRING, REWARD_SERDE));
        
        maskedStream.selectKey((key, purchase) -> purchase.storeId())
                    .groupByKey(Grouped.with(STRING, PURCHASE_SERDE))
                    .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
                    .count()
                    .toStream()
                    .map((windowedKey, count) -> KeyValue.pair(
                        windowedKey.key() + "@" + windowedKey.window().start(), count))
                    .to(STORE_SALES_5M, Produced.with(STRING, LONG));
        
        KTable<String, Customer> customers = builder.table(
                CUSTOMERS, Consumed.with(STRING, CUSTOMER_SERDE));
        GlobalKTable<String, Store> stores = builder.globalTable(
                STORES, Consumed.with(STRING, STORE_SERDE));
        maskedStream.join(customers, PurchaseWithCustomer::new)
                    .join(stores, 
                            (customerId, purchaseWithCustomer) -> 
                                purchaseWithCustomer.purchase.storeId(),
                            (purchaseWithCustomer, store) -> new EnrichedPurchase(
                                purchaseWithCustomer.customer.customerId(),
                                purchaseWithCustomer.customer.name(),
                                purchaseWithCustomer.customer.tier(),
                                store.city(),
                                purchaseWithCustomer.purchase.item(),
                                purchaseWithCustomer.purchase.total()))
                    .to(PURCHASES_ENRICHED, Produced.with(STRING, ENRICHED_SERDE));
        return builder.build();
    }

    private boolean isValid(Purchase purchase) {
        return purchase != null && purchase.quantity() > 0 && 
                purchase.price() > 0 && purchase.creditCardNumber() != null && 
                !purchase.creditCardNumber().isBlank();
    }

    private Purchase mask(Purchase purchase) {
        String card = purchase.creditCardNumber();
        String last4 = card.length() >= 4 ? card.substring(card.length() - 4) : card;
        return purchase.withCreditCardNumber(CC_MASK + last4);
    }

    public static void main(String[] args) throws Exception {
        TopicSetup.createIfMissing("localhost:9092", AdvancedTopics.all());
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "zmart-purchases-advanced");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        properties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
            LogAndContinueExceptionHandler.class);
        ZMartKafkaStreamsAdvanced zMartKafkaStreamsaAdvanced = new ZMartKafkaStreamsAdvanced();
        Topology topology = zMartKafkaStreamsaAdvanced.topology(properties);
        log.info("Topology description {}", topology.describe());
        CountDownLatch latch = new CountDownLatch(1);

        try (KafkaStreams kafkaStreams = new KafkaStreams(topology, properties)) {
            /*kafkaStreams.setUncaughtExceptionHandler(ex -> {
                log.error("Stream thread died", ex);
                latch.countDown();
                return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            }); */
            Runtime.getRuntime().addShutdownHook(new Thread(latch::countDown));
            kafkaStreams.start();
            startRewardQueryConsole(kafkaStreams);
            log.info("Advanced app started. Type a customerId to query rewards. Ctrl+C to stop.");
            latch.await();
        }
    }

    private static void startRewardQueryConsole(KafkaStreams streams) {
        Thread t = new Thread(() -> {
            Scanner in = new Scanner(System.in);
            while (in.hasNextLine()) {
                String customerId = in.nextLine().trim();
                if (customerId.isEmpty()) continue;
                if (streams.state() != KafkaStreams.State.RUNNING) {
                    System.out.println("Not ready yet, state=" + streams.state());
                    continue;
                }
                try {
                    ReadOnlyKeyValueStore<String, Reward> store = streams.store(
                            StoreQueryParameters.fromNameAndType(REWARDS_STORE, QueryableStoreTypes.keyValueStore()));
                    System.out.println(customerId + " -> " + store.get(customerId));
                } catch (Exception e) {
                    System.out.println("Query failed: " + e.getMessage());
                }
            }
        }, "reward-query-console");
        t.setDaemon(true);
        t.start();
    }
}

package com.zmart.stream.processor;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public final class AdvancedTopics {
    private AdvancedTopics() {}

    public static final String PURCHASE_EVENTS = "purchase-events"; 
    public static final String CUSTOMERS = "customers";             
    public static final String STORES = "stores";                   

    public static final String PURCHASES_MASKED = "purchases-masked";
    public static final String PURCHASES_CAFE = "purchases-cafe";
    public static final String PURCHASES_ELECTRONICS = "purchases-electronics";
    public static final String PURCHASES_OTHER = "purchases-other";
    public static final String PURCHASES_HIGH_VALUE = "purchases-high-value";
    public static final String PURCHASES_DLQ = "purchases-dlq";
    public static final String PURCHASES_ENRICHED = "purchases-enriched";
    public static final String REWARD_TOTALS = "reward-totals";      
    public static final String STORE_SALES_5M = "store-sales-5m";    

    public static final String REWARDS_STORE = "rewards-store";

    public static final Serde<String> STRING = Serdes.String();
    public static final Serde<Long> LONG = Serdes.Long();
    public static final Serde<Purchase> PURCHASE_SERDE = new JsonSerde<>(Purchase.class);
    public static final Serde<Customer> CUSTOMER_SERDE = new JsonSerde<>(Customer.class);
    public static final Serde<Store> STORE_SERDE = new JsonSerde<>(Store.class);
    public static final Serde<Reward> REWARD_SERDE = new JsonSerde<>(Reward.class);
    public static final Serde<EnrichedPurchase> ENRICHED_SERDE = new JsonSerde<>(EnrichedPurchase.class);

    public static String[] all() {
        return new String[]{PURCHASE_EVENTS, CUSTOMERS, STORES,
                PURCHASES_MASKED, PURCHASES_CAFE, PURCHASES_ELECTRONICS, PURCHASES_OTHER,
                PURCHASES_HIGH_VALUE, PURCHASES_DLQ, PURCHASES_ENRICHED,
                REWARD_TOTALS, STORE_SALES_5M};
    }
}
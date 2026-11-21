package com.zmart.stream.processor;

public record EnrichedPurchase(String customerId, String customerName, String tier, 
                                String storeCity, String item, double total) {}

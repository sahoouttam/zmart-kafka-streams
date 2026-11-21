package com.zmart.stream.processor;

public record PurchasedItem(String customerId, String item, int quantity, 
                        double price, long purchaseDate) {}

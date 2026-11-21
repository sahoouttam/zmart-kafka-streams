package com.zmart.stream.processor;

import java.util.List;

public record RetailPurchase(String zipCode, String creditCardNumber, 
                                List<PurchasedItem> purchasedItems) {}

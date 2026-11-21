package com.zmart.stream.processor;

public record Purchase(String customerId, String item, int quantity, double price,
                           String storeId, String department, String creditCardNumber,
                           long timestamp) { 
    
    public double total() {
        return quantity * price;
    }  

    public Purchase withCreditCardNumber(String card) {
        return new Purchase(customerId, item, quantity, 
                        price, storeId, department, card, timestamp);
    }
}

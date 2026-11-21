package com.zmart.stream.processor;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class PurchaseTimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        if (record.value() instanceof Purchase purchase && purchase.timestamp() > 0) {
            return purchase.timestamp();
        }
        return record.timestamp();
    }
    
}

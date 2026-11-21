package com.zmart.stream.processor;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TopicSetup {
    private static final Logger log = LoggerFactory.getLogger(TopicSetup.class);

    private TopicSetup() {}

    public static void createIfMissing(String bootstrap, String... topics) {
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap))) {
            List<NewTopic> newTopics = java.util.Arrays.stream(topics)
                    .map(t -> new NewTopic(t, 1, (short) 1))
                    .toList();
            admin.createTopics(newTopics).values().forEach((name, future) -> {
                try {
                    future.get();
                    log.info("Created topic {}", name);
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof TopicExistsException) {
                        log.info("Topic {} already exists", name);
                    } else {
                        throw new IllegalStateException("Failed creating topic " + name, e.getCause());
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
    }
}

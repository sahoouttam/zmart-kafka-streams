# zmart-kafka-streams

A retail purchase stream processor built with **Kafka Streams** (Java 17, Kafka 3.8).

Two applications on the same codebase:

| App | Main class | What it shows |
|-----|-----------|---------------|
| **Basic** | `ZMartKafkaStreamsApp` | Stateless processing: card masking, `flatMap`, per-purchase rewards |
| **Advanced** | `ZMartKafkaStreamsAdvanced` | Dead-letter topic, branching, stateful aggregation, windowing, stream-table joins, exactly-once, interactive queries |

Uses **JSON (Jackson) serdes** (no Protobuf or schema registry to run locally) and has
`TopologyTestDriver` tests that run without a broker.

---

## Basic app

Reads basket-style purchases from `transactions` and fans them out:

```
                         ┌─► purchases   (card number masked)
transactions ─► mask ────┼─► patterns    (one record per item, keyed by zip code)
  (RetailPurchase)       └─► rewards     (total + reward points per purchase)
```

| Step | Kafka Streams API |
|------|-------------------|
| Mask card (`1234-5678-9012-3456` → `xxxx-xxxx-xxxx-3456`) | `mapValues` |
| Explode a basket into one record per item, keyed by zip code | `flatMap` + `mapValues` |
| Reward points = `(int) total * 4`, `total = Σ quantity × price` | `mapValues` |

---

## Advanced app

Reads single-item purchase events from `purchase-events` (keyed by `customerId`):

```
purchase-events
  ├─ invalid ───────────────────────────────────────────► purchases-dlq
  └─ valid ─► mask card ─► purchases-masked
                 ├─ by department ──────────────────────► purchases-cafe | -electronics | -other
                 ├─ total ≥ 500 ─────────────────────────► purchases-high-value
                 ├─ groupByKey + aggregate (state store) ► reward-totals
                 ├─ selectKey(storeId) + 5-min windows ──► store-sales-5m
                 └─ join customers (KTable)
                         + stores (GlobalKTable) ────────► purchases-enriched
```

| Feature | Kafka Streams API |
|---------|-------------------|
| Invalid purchases (quantity or price ≤ 0, missing card) go to a DLQ | `split` / `branch` |
| Route by department, flag high-value purchases | `split`, `filter` |
| Running reward total per customer (1 point per currency unit) in a named state store | `groupByKey`, `aggregate`, `Materialized` |
| Purchases per store in 5-minute tumbling windows, using the purchase's own timestamp as event time | `selectKey`, `TimeWindows.ofSizeWithNoGrace`, custom `TimestampExtractor` |
| Enrich with customer and store data | `KStream`–`KTable` join, `KStream`–`GlobalKTable` join |
| No duplicates on failure | `exactly_once_v2` |
| Malformed JSON is logged and skipped | `LogAndContinueExceptionHandler` |
| Query a customer's rewards from the live state store | interactive queries |

### Topics

| Topic | Direction | Key → Value |
|-------|-----------|-------------|
| `purchase-events` | in | customerId → `Purchase` |
| `customers` | in | customerId → `Customer` |
| `stores` | in | storeId → `Store` |
| `purchases-masked`, `-cafe`, `-electronics`, `-other`, `-high-value` | out | customerId → `Purchase` |
| `purchases-dlq` | out | customerId → invalid `Purchase` |
| `reward-totals` | out | customerId → `Reward` |
| `store-sales-5m` | out | `<storeId>@<windowStartMs>` → count |
| `purchases-enriched` | out | customerId → `EnrichedPurchase` |

---

## Project layout

```
src/main/java/com/zmart/stream/processor/
├── ZMartKafkaStreamsApp.java        # basic topology + main
├── ZMartKafkaStreamsAdvanced.java   # advanced topology + main + reward query console
├── AdvancedTopics.java              # topic names, serdes, state store name
├── PurchaseTimestampExtractor.java  # event time from the purchase's own timestamp
├── TopicSetup.java                  # creates topics on startup if missing
├── JsonSerde.java                   # generic Jackson serde
└── (domain records)                 # RetailPurchase, Purchase, Customer, Store, Reward, ...
src/test/java/...                    # TopologyTestDriver tests (no broker needed)
```

## Requirements

- Java 17+
- Maven 3.8+
- A local Kafka broker on `localhost:9092` (only for the end-to-end run)

## Run the tests (no broker needed)

```bash
mvn clean test
```

## Run it

Start your Kafka broker (and ZooKeeper if your Kafka version needs it). The apps create their
topics on startup.

```bash
# basic app
mvn compile exec:java

# advanced app
mvn compile exec:java -Dexec.mainClass=com.zmart.stream.processor.ZMartKafkaStreamsAdvanced
```

> The pom sets a default `exec.mainClass` property rather than hard-coding the plugin's
> `<mainClass>`, so the command-line flag above can override it.

### Try the advanced app

Send reference data first, then purchases (keyed messages, `:` as the key separator):

```bash
kafka-console-producer --bootstrap-server localhost:9092 --topic customers --property parse.key=true --property key.separator=:
c1:{"customerId":"c1","name":"Asha","tier":"GOLD"}

kafka-console-producer --bootstrap-server localhost:9092 --topic stores --property parse.key=true --property key.separator=:
s1:{"storeId":"s1","city":"Bengaluru"}

kafka-console-producer --bootstrap-server localhost:9092 --topic purchase-events --property parse.key=true --property key.separator=:
c1:{"customerId":"c1","item":"coffee","quantity":2,"price":10.5,"storeId":"s1","department":"cafe","creditCardNumber":"1234-5678-9012-3456","timestamp":1767225600000}
c1:{"customerId":"c1","item":"laptop","quantity":1,"price":999,"storeId":"s1","department":"electronics","creditCardNumber":"1234-5678-9012-3456","timestamp":1767225660000}
c1:{"customerId":"c1","item":"bad","quantity":0,"price":5,"storeId":"s1","department":"cafe","creditCardNumber":"1234-5678-9012-3456","timestamp":1767225700000}
```

Expected:

| Topic | Result |
|-------|--------|
| `purchases-masked` | 2 records, card `xxxx-xxxx-xxxx-3456` |
| `purchases-cafe` / `purchases-electronics` | the coffee / the laptop |
| `purchases-high-value` | the laptop |
| `purchases-dlq` | the `quantity: 0` record |
| `reward-totals` | `c1`: 21 points, then 1020 |
| `store-sales-5m` | `s1@1767225600000` → 1, then 2 |
| `purchases-enriched` | 2 records with `Asha` / `Bengaluru` |

While the advanced app runs, type `c1` in its console to read the rewards state store.

(On Windows use the `.bat` scripts in `bin\windows\`; quote the key separator argument.)

---

## Design notes

- **JSON over Protobuf**: no code generation or registry to run locally. The serde is isolated in
  `JsonSerde`, so swapping formats would not touch the topologies.
- **Event time**: windows use the purchase's own `timestamp` through a `TimestampExtractor`, so
  late or replayed data lands in the right window.
- **Inner joins**: a purchase with no matching customer or store is dropped. A left join plus a
  DLQ for unmatched records would be the production choice.
- **Exactly-once** needs `transaction.state.log.replication.factor=1` and `min.isr=1` on a
  single-broker setup (the default `server.properties` already sets both).
- **Error handling**: malformed records are skipped and logged. Invalid *business* records
  (bad quantity, missing card) go to the DLQ instead of being dropped.
- **Graceful shutdown**: a shutdown hook releases a `CountDownLatch`, so `KafkaStreams.close()`
  runs on Ctrl+C or if a stream thread dies.

## Roadmap

| Status | Feature |
|:------:|---------|
| ✅ | Stateless topology: mask, `flatMap` patterns, rewards |
| ✅ | `TopologyTestDriver` tests |
| ✅ | Validation + dead-letter topic (`split` / `branch`) |
| ✅ | Routing by department, high-value purchase filter |
| ✅ | Stateful rewards: running total per customer in a state store |
| ✅ | Windowed aggregation (purchases per store, 5-minute tumbling windows) |
| ✅ | Joins: `KStream`–`KTable` (customers) and `GlobalKTable` (stores) |
| ✅ | Exactly-once processing, interactive queries |
| ⬜ | Left joins with a DLQ for unmatched records |
| ⬜ | Late-record handling with a grace period and `suppress` |
| ⬜ | Metrics and an HTTP endpoint for interactive queries |

## Tech

Java 17 · Kafka Streams 3.8 · Jackson 2.17 · JUnit 5 · Maven
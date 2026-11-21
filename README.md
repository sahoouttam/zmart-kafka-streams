# zmart-kafka-streams

A retail purchase stream processor built with **Kafka Streams** (Java 17, Kafka 3.8).

Uses **JSON (Jackson) serdes** (no Protobuf or schema registry to run locally), has tests that run
without a broker, and is being extended with stateful processing, windowing and joins
(see the [roadmap](#roadmap)).

## What it does

Reads retail purchases from `transactions` and fans them out into three topics:

```
                         ┌─► purchases   (card number masked)
transactions ─► mask ────┼─► patterns    (one record per item, keyed by zip code)
  (RetailPurchase)       └─► rewards     (total + reward points per purchase)
```

| Step | Operation | Kafka Streams API |
|------|-----------|-------------------|
| Mask card number (`1234-5678-9012-3456` → `xxxx-xxxx-xxxx-3456`) | stateless map | `mapValues` |
| Explode a basket into one record per item, keyed by zip code | 1→N | `flatMap` + `mapValues` |
| Reward points = `(int) total * 4` where `total = Σ quantity × price` | stateless map | `mapValues` |

Bad input is logged and skipped (`LogAndContinueExceptionHandler`) so one malformed record
cannot crash the stream thread.

## Project layout

```
src/main/java/com/zmart/stream/processor/
├── ZMartKafkaStreamsApp.java   # topology + main (graceful shutdown)
├── TopicSetup.java             # creates topics on startup if missing
├── JsonSerde.java              # generic Jackson serde
└── model/                      # RetailPurchase, PurchasedItem, Pattern, RewardAccumulator
src/test/java/.../ZMartKafkaStreamsAppTest.java   # TopologyTestDriver tests (no broker needed)
```

## Requirements

- Java 17+
- Maven 3.8+
- A local Kafka broker on `localhost:9092`

## Run the tests (no broker needed)

```bash
mvn clean test
```

The tests use `TopologyTestDriver`, so they run in-process.

## Run it locally

1. Start your Kafka broker (ZooKeeper first if your Kafka version needs it).
2. Start the app. It creates the `transactions`, `purchases`, `patterns` and `rewards` topics itself:

```bash
mvn compile exec:java
# custom broker:
mvn compile exec:java -Dexec.args="host:9092"
```

3. Send a purchase (one line) with the console producer:

```bash
kafka-console-producer --bootstrap-server localhost:9092 --topic transactions
```

```json
{"zipCode":"560001","creditCardNumber":"1234-5678-9012-3456","purchasedItems":[{"customerId":"c1","item":"coffee","quantity":2,"price":10.5,"purchaseDate":1000},{"customerId":"c1","item":"bagel","quantity":1,"price":9.0,"purchaseDate":2000}]}
```

4. Read the results (add `--from-beginning` if you started the consumer late):

```bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic purchases
kafka-console-consumer --bootstrap-server localhost:9092 --topic patterns --property print.key=true
kafka-console-consumer --bootstrap-server localhost:9092 --topic rewards
```

Expected:

```
purchases: {"zipCode":"560001","creditCardNumber":"xxxx-xxxx-xxxx-3456", ... }
patterns : 560001  {"item":"coffee","amount":10.5,"date":1000}
           560001  {"item":"bagel","amount":9.0,"date":2000}
rewards  : {"customerId":"c1","purchaseTotal":30.0,"totalRewardPoints":120}
```

(On Windows use the `.bat` scripts in `bin\windows\`.)

## Design notes

- **JSON over Protobuf**: no code generation or schema registry to run locally. The serde is
  isolated in `JsonSerde`, so swapping to Protobuf or Avro would not touch the topology.
- **Immutable records** for the domain model; masking builds a new `RetailPurchase`.
- **Error handling**: deserialization failures are skipped and logged. A dead-letter topic is
  on the roadmap for production use.
- **Graceful shutdown**: a shutdown hook releases a `CountDownLatch`, so `KafkaStreams.close()`
  runs on Ctrl+C or if a stream thread dies.

## Roadmap

| Status | Feature |
|:------:|---------|
| ✅ | Stateless topology: mask, `flatMap` patterns, rewards |
| ✅ | `TopologyTestDriver` tests |
| ⬜ | Validation + dead-letter topic (`split` / `branch`) |
| ⬜ | Routing by department, high-value purchase filter |
| ⬜ | Stateful rewards: running total per customer in a state store |
| ⬜ | Windowed aggregation (purchases per store, 5-minute tumbling windows) |
| ⬜ | Joins: `KStream`–`KTable` (customers) and `GlobalKTable` (stores) |
| ⬜ | Exactly-once processing, interactive queries |

## Tech

Java 17 · Kafka Streams 3.8 · Jackson 2.17 · JUnit 5 · Maven
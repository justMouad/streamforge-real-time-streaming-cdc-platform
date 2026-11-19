# Retail Real-Time Data Pipeline

A local streaming data platform that simulates retail activity, processes events in real time, and captures changes from an operational PostgreSQL database.

The environment runs with Docker Compose and uses generated data. It does not require access to a real retail system or personal information.

## 1. Project Overview

The platform demonstrates two complementary data-ingestion patterns:

- **Event Streaming:** Synthetic sales and inventory events are published to Kafka and processed by Spark Structured Streaming.
- **Change Data Capture (CDC):** Updates made to PostgreSQL are read from its write-ahead log by Debezium, published to Kafka, and applied to Apache Iceberg.

Spark performs event parsing, validation, event-time aggregation, late-event handling, and CDC processing. Iceberg stores the resulting curated data.

## 2. Architecture

### Event Streaming Flow

    Synthetic Data Generator
              |
              v
       Kafka: retail.events
              |
              v
    Spark Structured Streaming
       |          |           |
       |          |           +--> Invalid / Late Records (DLQ)
       |          |
       |          +--> Event-Time Window Aggregations
       |
       v
       Apache Iceberg

### CDC Flow

    PostgreSQL
    (products table)
          |
          | Write-Ahead Log (Logical Decoding)
          v
       Debezium
          |
          v
      Kafka Connect
          |
          v
    Kafka: retail.cdc.public.products
          |
          v
    Spark CDC Processor
    (Parse, Flatten, Compact, Merge)
          |
          v
       Apache Iceberg

### Main Components

| Component | Responsibility |
|---|---|
| Python Producer | Generates and publishes synthetic retail events |
| Apache Kafka | Stores and distributes event and CDC messages |
| Kafka Connect | Runs the Debezium connector |
| Debezium | Captures PostgreSQL row-level changes |
| PostgreSQL | Holds the operational `products` table |
| Apache Spark Structured Streaming | Processes streaming events and CDC messages |
| Apache Iceberg | Stores curated tables and supports atomic table operations |
| Schema Registry | Runs alongside Kafka; the current event payload format is JSON |

Kafka uses **KRaft**, so ZooKeeper is not required.

## 3. Event Format

The producer sends two kinds of JSON messages to the `retail.events` topic.

| Event Type | Kafka Key | Main Fields |
|---|---|---|
| `sale` | `order_id` | `category`, `quantity`, `unit_price`, `ts` |
| `stock` | `sku` | `category`, `warehouse`, `quantity_delta`, `ts` |

Messages are keyed by their business identifier. Kafka therefore preserves ordering for a given key within its partition.

The generator supports a seed, making test data reproducible.

## 4. Requirements

Install the following before starting:

- Docker Desktop with Docker Compose
- Python and pip
- Git (if cloning the repository)

The commands below are intended to be run from the project root.

## 5. Start the Infrastructure

Launch the containers:

```bash
docker compose up -d
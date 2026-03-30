# kafka-loader

`kafka-loader` is a small infrastructure utility for generating controlled producer-side Kafka traffic from JSONL data files.

The application reads a properties file, loads input records from a local `.jsonl` file, and continuously sends them to one or more Kafka topics. Load is controlled by `eps.per.topic` and `threads.per.topic`, which makes the tool useful for smoke testing, connectivity checks, SSL validation, and repeatable producer-side load generation on Linux hosts.

At the moment the following connection modes are supported:

- `PLAINTEXT`
- `SSL` with server authentication only
- `SSL` with mutual TLS (mTLS)

The goal is to provide a simple and predictable Kafka producer that can be configured from a file and run as a standalone Linux binary without requiring a full Python runtime on the target host.

Main configuration parameters:

- Kafka bootstrap servers and target topics
- path to the source JSONL file
- producer rate per topic
- number of worker threads per topic
- producer settings such as `acks`, `linger.ms`, `batch.size`, `buffer.memory`, and compression type
- SSL truststore and keystore paths when secure Kafka access is required

In practice the loader reads non-empty lines from the input file into memory and reuses them in a loop while producing to Kafka. This makes it convenient for repeatable traffic generation with a fixed dataset during infrastructure checks, environment validation, and test runs.

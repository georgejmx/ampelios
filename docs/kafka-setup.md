# Kafka Setup

_WIP_

Ampelios is designed to consume events from Kafka. There is a sample config for doing so

## Instructions

1. Spin up Kafka + Ampelios + Bento

```bash
cd infra
docker compose -f docker-compose-kafka.yml up
```

2. Run a sample bento config to verify that events were loaded into kafka

```yml
input:
  kafka_franz:
    seed_brokers: ["localhost:9092"]
    topics: ["events-topic"]
    consumer_group: test-consumer
```

then do a `bento -c tmp.yml`

...

1) Kafka broker started
1) Kafka topic event-topic created
1) Bento loads ./init-data/events.csv into event-topic

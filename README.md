# KafkaEsperStorm

![alt text](https://github.com/tank113/KafkaEsperStorm/blob/master/kafka_storm_arch.png?raw=true)

This is the architecture of Kafka and Storm that process the real-time streaming data, process it (measuring trends in the weather data) and visualize it in real-time.

Kafka is used as Spouts and Complex Event processing module acts as Bolts in the Storm architecture.

The results are stored in the influxDB Bolt to finally visualize it in Grafana

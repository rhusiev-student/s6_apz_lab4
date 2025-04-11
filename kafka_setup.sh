podman-compose up -d
podman exec --workdir /opt/kafka/bin/ -it kafka1 bash -c "./kafka-topics.sh --create --topic my-kool-topic --bootstrap-server kafka1:9092 --partitions 3 --replication-factor 3 && ./kafka-topics.sh --describe --topic my-kool-topic --bootstrap-server kafka1:9092"

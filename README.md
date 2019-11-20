# A pure ruby client for Apache Pulsar

## Example

见`examples`中的例子

## Dev

```shell
PB_OUT="./lib/protobuf/"

mkdir -p ${PB_OUT}

protoc -I ${PULSAR_GIT}/pulsar-common/src/main/proto/ --ruby_out ${PB_OUT} PulsarApi.proto
mv ${PB_OUT}PulsarApi_pb.rb ${PB_OUT}pulsar_api.pb.rb

protoc -I ${PULSAR_GIT}/pulsar-common/src/main/proto/ --ruby_out ${PB_OUT} PulsarMarkers.proto
mv ${PB_OUT}PulsarMarkers_pb.rb ${PB_OUT}pulsar_markers.pb.rb
```

## Future

- [x] Connection establishment
- [x] Producer
  - [x] Message Delivery
  - [x] [Delayed Message Delivery][1]
  - [x] Get SendReceipt
  - [x] Close Producer
- [x] Consumer
  - [x] Flow control
  - [x] Ack
  - [x] Message Redelivery
- [ ] Keep alive
  - [x] handle ping command
  - [ ] send ping command
- [ ] Service discovery
  - [ ] Topic lookup
  - [ ] Partitioned topics discovery
- [ ] Log Optimization
- [ ] Connection pool
- [ ] Unit Test
- [x] Thread safe

[1]: https://github.com/apache/pulsar/wiki/PIP-26%3A-Delayed-Message-Delivery "PIP 26: Delayed Message Delivery"

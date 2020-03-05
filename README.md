# A pure ruby client for Apache Pulsar

Respecting [Pulsar binary protocol specification][2]

## Example

See `examples`.

## Dev

I am using Ruby 2.4 develop this gem, if not work with other Ruby version, PR is welcome.

```shell
#!/bin/sh

PB_PATH="./lib/protobuf/"
PB_IN='pulsar_api.proto'
PB_OUT_F='pulsar_api.pb.rb'

mkdir -p ${PB_PATH}

wget -O ${PB_PATH}${PB_IN} https://raw.githubusercontent.com/apache/pulsar/master/pulsar-common/src/main/proto/PulsarApi.proto

# protoc -I ${PULSAR_GIT}/pulsar-common/src/main/proto/ --ruby_out ${PB_PATH} PulsarApi.proto
protoc -I ${PB_PATH} --ruby_out ${PB_PATH} ${PB_IN}
mv ${PB_PATH}pulsar_api_pb.rb ${PB_PATH}${PB_OUT_F}

# fix pulsar.proto.ProtocolVersion error: invalid name `v0' for constant
if [ $(uname) = 'Darwin' ]
then
    suffix='.pbbak'
fi

for i in $(seq 0 15)
do
    sed -i ${suffix} "s;value :v$i, $i;value :V$i, $i;g" ${PB_PATH}${PB_OUT_F}
done

rm -f ${PB_PATH}${PB_OUT_F}.pbbak

protoc -I ${PULSAR_GIT}/pulsar-common/src/main/proto/ --ruby_out ${PB_PATH} PulsarMarkers.proto
mv ${PB_PATH}PulsarMarkers_pb.rb ${PB_PATH}pulsar_markers.pb.rb
```

## Dependency

When macOS user install google-protobuf, there maybe a suffix with `universal-darwin` save to Gemfile.lock. Please use google-protobuf without `universal-darwin` suffix. See issuse #6 for detail. Thanks for wppurking's report!

## Features

- [ ] Connection
  - [x] Establishment
  - [ ] TLS connection
  - [ ] Authentication
- [ ] Producer
  - [x] Message Delivery
  - [x] [Delayed Message Delivery][1]
  - [x] Get SendReceipt
  - [x] Close Producer
  - [x] Partitioned topics
  - [ ] Batch Message Delivery
  - [ ] Message Compression
  - [ ] [Deliver message after AR transaction commit][3]
- [ ] Consumer
  - [x] Flow control
  - [x] Ack
  - [x] Message Redelivery
  - [x] Listen
  - [x] Partitioned topics
  - [x] Topic with regexp (in same namespace)
  - [ ] Reader
  - [ ] [Dead Letter Topic][4]
  - [ ] [Key Shared][6]
- [x] Keep alive
  - [x] handle ping command
  - [x] send ping command
- [x] Service discovery
  - [x] Topic lookup
- [x] Log Optimization
- [x] Connection pool
- [ ] Unit Test
- [x] Thread safe
- [ ] Schema
  - [ ] Get
  - [ ] Create
- [ ] Admin API
  - [x] Create Namespace
  - [x] List namespaces in current tenant
  - [x] Destroy Namespace
  - [x] List Namespace Topics
  - [x] Create Topic
  - [x] Delete Topic
  - [x] Peek Messages

## WIP

Catch up [Pulsar client feature matrix][5], current working on:

- Dead Letter Topic

[1]: https://github.com/apache/pulsar/wiki/PIP-26%3A-Delayed-Message-Delivery "PIP 26: Delayed Message Delivery"
[2]: https://pulsar.apache.org/docs/en/develop-binary-protocol/ "Pulsar binary protocol specification"
[3]: https://github.com/Envek/after_commit_everywhere "after commit everywhere"
[4]: https://github.com/apache/pulsar/wiki/PIP-22:-Pulsar-Dead-Letter-Topic "PIP 22: Pulsar Dead Letter Topic"
[5]: https://github.com/apache/pulsar/wiki/Client-Features-Matrix "Pulsar client feature matrix"
[6]: https://pulsar.apache.org/docs/en/concepts-messaging/#key_shared "consumer key_shared mode"

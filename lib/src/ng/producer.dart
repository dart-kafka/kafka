import 'dart:async';

import 'package:logging/logging.dart';

import 'common.dart';
import 'messages.dart';
import 'metadata.dart';
import 'produce_api.dart';
import 'serialization.dart';
import 'session.dart';

final Logger _logger = new Logger('Producer');

/// Produces messages to Kafka cluster.
///
/// Automatically discovers leader brokers for each topic-partition to
/// send messages to.
abstract class Producer<K, V> {
  factory Producer(Serializer<K> keySerializer, Serializer<V> valueSerializer,
      Session session) {
    return new _ProducerImpl(keySerializer, valueSerializer, session);
  }

  /// Sends [record] to Kafka cluster.
  Future<ProduceResult> send(ProducerRecord<K, V> record);
}

class ProducerRecord<K, V> {
  final String topic;
  final int partition;
  final K key;
  final V value;

  ProducerRecord(this.topic, this.partition, this.key, this.value);
}

class ProduceResult {
  final TopicPartition topicPartition;
  final int offset;

  ProduceResult(this.topicPartition, this.offset);

  @override
  toString() => 'ProduceResult{${topicPartition}, offset: $offset}';
}

class _ProducerImpl<K, V> implements Producer<K, V> {
  final Session session;
  final Serializer<K> keySerializer;
  final Serializer<V> valueSerializer;

  _ProducerImpl(this.keySerializer, this.valueSerializer, this.session);

  @override
  Future<ProduceResult> send(ProducerRecord<K, V> record) async {
    var key = keySerializer.serialize(record.key);
    var value = valueSerializer.serialize(record.value);
    var message = new Message(value, key: key);
    var messages = {
      record.topic: {
        record.partition: [message]
      }
    };
    var req = new ProduceRequest(1, 1000, messages);
    var metadata = new Metadata(session);
    var meta = await metadata.fetchTopics([record.topic]);
    var leaderId = meta.first.partitions
        .firstWhere((_) => _.id == record.partition)
        .leader;
    var brokers = await metadata.listBrokers();
    var broker = brokers.firstWhere((_) => _.id == leaderId);
    var result = await session.send(req, broker.host, broker.port);
    return new Future.value(new ProduceResult(
        new TopicPartition(record.topic, record.partition),
        result.results.first.offset));
  }
}

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
  final int timestamp;

  ProducerRecord(this.topic, this.partition, this.key, this.value,
      {this.timestamp});
}

class ProduceResult {
  final TopicPartition topicPartition;
  final int offset;
  final int timestamp;

  ProduceResult(this.topicPartition, this.offset, this.timestamp);

  @override
  toString() =>
      'ProduceResult{${topicPartition}, offset: $offset, timestamp: $timestamp}';
}

class _ProducerImpl<K, V> implements Producer<K, V> {
  final Session session;
  final Serializer<K> keySerializer;
  final Serializer<V> valueSerializer;

  _ProducerImpl(this.keySerializer, this.valueSerializer, this.session);

  @override
  Future<ProduceResult> send(ProducerRecord<K, V> record) async {
    final key = keySerializer.serialize(record.key);
    final value = valueSerializer.serialize(record.value);
    final timestamp =
        record.timestamp ?? new DateTime.now().millisecondsSinceEpoch;
    final message = new Message(value, key: key, timestamp: timestamp);
    final messages = {
      record.topic: {
        record.partition: [message]
      }
    };
    final req = new ProduceRequest(1, 1000, messages);
    final metadata = new Metadata(session);
    final meta = await metadata.fetchTopics([record.topic]);
    final leaderId = meta[record.topic].partitions[record.partition].leader;
    final broker = meta.brokers[leaderId];
    final response = await session.send(req, broker.host, broker.port);
    final result = response.results.first;
    return new Future.value(new ProduceResult(
        result.topicPartition, result.offset, result.timestamp));
  }
}

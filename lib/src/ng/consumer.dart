import 'dart:async';

import 'serialization.dart';
import 'session.dart';
import 'common.dart';

abstract class KConsumer<K, V> {
  Future<KConsumerRecords> poll();
  Future subscribe(List<String> topics);
  Future unsubscribe();

  factory KConsumer(
      Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer,
      {KSession session}) {
    return new _KConsumerImpl(keyDeserializer, valueDeserializer, session);
  }
}

class _KConsumerImpl<K, V> implements KConsumer<K, V> {
  final KSession session;
  final Deserializer<K> keyDeserializer;
  final Deserializer<V> valueDeserializer;

  _KConsumerImpl(this.keyDeserializer, this.valueDeserializer, KSession session)
      : session = (session is KSession) ? session : KAFKA_DEFAULT_SESSION;

  @override
  Future<KConsumerRecords> poll() {
    // TODO: implement poll
  }

  @override
  Future subscribe(List<String> topics) {
    // TODO: implement subscribe
  }

  @override
  Future unsubscribe() {
    // TODO: implement unsubscribe
  }
}

class KConsumerRecord<K, V> {
  final String topic;
  final int partition;
  final int offset;
  final K key;
  final V value;

  KConsumerRecord(
      this.topic, this.partition, this.offset, this.key, this.value);
}

class KConsumerRecords<K, V> {
  final Map<TopicPartition, List<KConsumerRecord>> records;

  KConsumerRecords(this.records);
}

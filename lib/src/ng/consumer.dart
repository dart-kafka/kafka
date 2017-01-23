import 'dart:async';

import 'serialization.dart';
import 'session.dart';

abstract class KConsumer<K, V> {
  Future subscribe(List<String> topics);
  Future poll();
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
  Future poll() {
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

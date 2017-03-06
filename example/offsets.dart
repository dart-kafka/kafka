import 'dart:async';

import 'package:kafka/ng.dart';

Future main() async {
  var session = new Session([new ContactPoint('127.0.0.1:9092')]);
  var master = new OffsetMaster(session);
  var offsets = await master.fetchEarliest([
    new TopicPartition('dartKafkaTest', 0),
    new TopicPartition('dartKafkaTest', 1),
    new TopicPartition('dartKafkaTest', 2),
  ]);
  print(offsets);
  await session.close(); // Always close session in the end.
}

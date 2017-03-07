import 'dart:async';

import 'package:kafka/ng.dart';

Future main() async {
  var session = new Session([new ContactPoint('127.0.0.1:9092')]);
  var master = new OffsetMaster(session);
  var offsets = await master.fetchEarliest([
    new TopicPartition('simple_topic', 0),
    new TopicPartition('simple_topic', 1),
    new TopicPartition('simple_topic', 2),
  ]);
  print(offsets);
  await session.close(); // Always close session in the end.
}

import 'dart:async';

import 'package:kafka/kafka.dart';

Future main() async {
  var session = Session(['127.0.0.1:9092']);
  var master = OffsetMaster(session);
  var offsets = await master.fetchEarliest([
    TopicPartition('simple_topic', 0),
    TopicPartition('simple_topic', 1),
    TopicPartition('simple_topic', 2),
  ]);
  print(offsets);
  await session.close(); // Always close session in the end.
}

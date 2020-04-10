import 'dart:async';

import 'package:kafka/kafka.dart';
import 'package:logging/logging.dart';

Future main() async {
  Logger.root.level = Level.ALL;
  Logger.root.onRecord.listen(print);

  var session = Session(['127.0.0.1:9092']);
  var consumer = Consumer<String, String>(
      'simple_consumer', StringDeserializer(), StringDeserializer(), session);

  await consumer.subscribe(['simple_topic']);
  var queue = consumer.poll();
  while (await queue.moveNext()) {
    var records = queue.current;
    for (var record in records.records) {
      print(
          "[${record.topic}:${record.partition}], offset: ${record.offset}, ${record.key}, ${record.value}, ts: ${record.timestamp}");
    }
    await consumer.commit();
  }
  await session.close();
}

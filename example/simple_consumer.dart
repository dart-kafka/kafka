import 'dart:async';

import 'package:kafka/ng.dart';
import 'package:logging/logging.dart';

Future main() async {
  Logger.root.level = Level.ALL;
  Logger.root.onRecord.listen(print);

  var session = new Session([new ContactPoint('127.0.0.1:9092')]);
  var consumer = new Consumer<String, String>('simple_consumer',
      new StringDeserializer(), new StringDeserializer(), session);

  await consumer.subscribe(['simple_topic']);
  var queue = consumer.poll();
  while (await queue.moveNext()) {
    var records = queue.current;
    for (var record in records.records) {
      print(
          "[${record.topic}:${record.partition}] ${record.key}, ${record.value}");
    }
    await consumer.commit();
  }
  await session.close();
}

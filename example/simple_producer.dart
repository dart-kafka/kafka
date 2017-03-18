import 'dart:async';
import 'package:kafka/kafka.dart';
import 'package:logging/logging.dart';

Future main() async {
  // Logger.root.level = Level.ALL;
  // Logger.root.onRecord.listen(print);

  var config = new ProducerConfig(bootstrapServers: ['127.0.0.1:9092']);
  var producer = new Producer<String, String>(
      new StringSerializer(), new StringSerializer(), config);

  for (var i = 0; i < 10; i++) {
    // Loop through a list of partitions.
    for (var p in [0, 1, 2]) {
      var rec =
          new ProducerRecord('simple_topic', p, 'key:${p},$i', 'value:${p},$i');
      producer.add(rec);
      rec.result.then(print);
    }
  }
  await producer.close();
}

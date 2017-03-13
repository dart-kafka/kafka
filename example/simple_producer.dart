import 'dart:async';
import 'package:kafka/ng.dart';

Future main() async {
  var session = new Session([new ContactPoint('127.0.0.1:9092')]);
  var producer = new Producer<String, String>(
      new StringSerializer(), new StringSerializer(), session);
  List<Future> results = [];
  for (var i = 0; i < 10; i++) {
    // Loop through a list of partitions.
    for (var p in [0, 1, 2]) {
      var future = producer.send(new ProducerRecord(
          'simple_topic', p, 'key:${p},$i', 'value:${p},$i'));
      var res = await future;
      print(res);
    }
  }
  await Future.wait(results);
  await session.close();
}

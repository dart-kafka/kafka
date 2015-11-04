library kafka.test.producer;

import 'package:test/test.dart';
import 'package:kafka/kafka.dart';
import 'setup.dart';

main() {
  group('Producer', () {
    KafkaSession _session;
    String _topicName = 'dartKafkaTest';

    setUp(() async {
      var host = await getDefaultHost();
      _session = new KafkaSession([new KafkaHost(host, 9092)]);
    });

    tearDown(() async {
      await _session.close();
    });

    test('it can produce messages to multiple brokers', () async {
      var producer = new Producer(_session, 1, 100);
      producer.addMessages(_topicName, 0, [new Message('test1'.codeUnits)]);
      producer.addMessages(_topicName, 1, [new Message('test2'.codeUnits)]);
      producer.addMessages(_topicName, 2, [new Message('test3'.codeUnits)]);
      var result = await producer.send();
      expect(result.hasErrors, isFalse);
    });
  });
}

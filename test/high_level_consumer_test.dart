library kafka.test.testing;

import 'package:kafka/kafka.dart';
import 'package:test/test.dart';
import 'setup.dart';

void main() {
  group('HighLevelConsumer: ', () {
    KafkaSession session;
    ConsumerGroup group;
    HighLevelConsumer consumer;

    setUp(() async {
      var host = await getDefaultHost();
      var now = new DateTime.now().millisecondsSinceEpoch;
      var groupName = 'group-' + now.toString();
      var topic = 'test-topic-' + now.toString();
      session = new KafkaSession([new ContactPoint(host, 9092)]);
      group = new ConsumerGroup(session, groupName);
      consumer = new HighLevelConsumer(session, [topic].toSet(), group);

      var producer = new Producer(session, -1, 1000);
      var message = 'helloworld';
      var messages = [
        new ProduceEnvelope(topic, 0, [new Message(message.codeUnits)])
      ];
      await producer.produce(messages);
    });

    test('it consumes messages', () async {
      var message;
      await for (var env in consumer.stream) {
        message = new String.fromCharCodes(env.message.value);
        env.commit('');
        break;
      }
      expect(message, 'helloworld');
    });
  });
}

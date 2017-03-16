import 'package:test/test.dart';
import 'package:kafka/kafka.dart';

void main() {
  group('Kafka.NG Produce API: ', () {
    String _topic = 'dartKafkaTest' +
        (new DateTime.now()).millisecondsSinceEpoch.toString();
    Broker broker;
    Session session = new Session(['127.0.0.1:9092']);
    int partition;

    setUp(() async {
      var data = await session.metadata.fetchTopics([_topic]);
      partition = data[_topic].partitions[0].id;
      var leaderId = data[_topic].partitions[0].leader;
      broker = data.brokers[leaderId];
    });

    tearDownAll(() async {
      await session.close();
    });

    test('it publishes messages to Kafka topic', () async {
      var req = new ProduceRequest(1, 1000, {
        _topic: {
          partition: [new Message('hello world'.codeUnits)]
        }
      });

      var res = await session.send(req, broker.host, broker.port);
      expect(res.results, hasLength(1));
      expect(res.results.first.topic, equals(_topic));
      expect(res.results.first.error, equals(0));
      expect(res.results.first.offset, greaterThanOrEqualTo(0));
    });

    // test('it publishes GZip encoded messages to Kafka topic', () async {
    //   var request = new ProduceRequest(1, 1000, [
    //     new ProduceEnvelope(
    //         _topicName,
    //         0,
    //         [
    //           new Message('hello world'.codeUnits),
    //           new Message('peace and love'.codeUnits)
    //         ],
    //         compression: KafkaCompression.gzip)
    //   ]);
    //   ProduceResponse response = await _session.send(_broker, request);
    //   expect(response.results, hasLength(1));
    //   expect(response.results.first.topicName, equals(_topicName));
    //   expect(response.results.first.errorCode, equals(0));
    //   expect(response.results.first.offset, greaterThanOrEqualTo(0));
    // });
  });
}

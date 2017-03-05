import 'package:test/test.dart';
import 'package:kafka/ng.dart';

void main() {
  group('Kafka.NG Produce API: ', () {
    String _topicName = 'dartKafkaTest' +
        (new DateTime.now()).millisecondsSinceEpoch.toString();
    Broker broker;
    KSession session = new KSession([new ContactPoint('127.0.0.1:9092')]);
    KMetadata metadata = new KMetadata(session);
    int partitionId;

    setUp(() async {
      var data = await metadata.fetchTopics([_topicName]);

      partitionId = data.first.partitions.first.partitionId;
      var leaderId = data.first.partitions.first.leader;
      var brokers = await metadata.listBrokers();
      broker = brokers.firstWhere((_) => _.id == leaderId);
    });

    tearDownAll(() async {
      await session.close();
    });

    test('it publishes messages to Kafka topic', () async {
      var req = new ProduceRequestV2(1, 1000, {
        _topicName: {
          partitionId: [new Message('hello world'.codeUnits)]
        }
      });

      var res = await session.send(req, broker.host, broker.port);
      expect(res.results, hasLength(1));
      expect(res.results.first.topic, equals(_topicName));
      expect(res.results.first.errorCode, equals(0));
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

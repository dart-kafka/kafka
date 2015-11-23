library kafka.test.api.fetch;

import 'package:test/test.dart';
import 'package:kafka/kafka.dart';
import 'package:kafka/protocol.dart';
import '../setup.dart';

void main() {
  group('FetchApi', () {
    String _topicName = 'dartKafkaTest';
    Broker _host;
    KafkaSession _session;
    FetchRequest _request;
    String _message;
    int _offset;

    setUp(() async {
      var ip = await getDefaultHost();
      _session = new KafkaSession([new ContactPoint(ip, 9092)]);
      var metadata = await _session.getMetadata();
      var leaderId =
          metadata.getTopicMetadata(_topicName).getPartition(0).leader;
      _host = metadata.getBroker(leaderId);

      ProduceRequest produce = new ProduceRequest(1, 1000);
      var now = new DateTime.now();
      _message = 'test:' + now.toIso8601String();
      produce.addMessages(_topicName, 0, [new Message(_message.codeUnits)]);
      var response = await _session.send(_host, produce);
      _offset = response.topics.first.partitions.first.offset;
      _request = new FetchRequest(100, 1);
    });

    tearDown(() async {
      await _session.close();
    });

    test('it fetches messages from Kafka topic', () async {
      _request.add(_topicName, 0, _offset);
      var response = await _session.send(_host, _request);

      expect(response.topics, hasLength(1));
      expect(response.topics[_topicName].first.messages,
          hasLength(greaterThanOrEqualTo(1)));
      var value =
          response.topics[_topicName].first.messages.messages[_offset].value;
      var text = new String.fromCharCodes(value);
      expect(text, equals(_message));
    });
  });
}

library kafka.test.api.fetch;

import 'package:test/test.dart';
import 'package:kafka/kafka.dart';
import 'package:kafka/protocol.dart';
import '../setup.dart';

void main() {
  group('FetchApi:', () {
    String _topicName = 'dartKafkaTest';
    Broker _host;
    KafkaSession _session;
    FetchRequest _request;
    String _message;
    int _offset;

    setUp(() async {
      var ip = await getDefaultHost();
      _session = new KafkaSession([new ContactPoint(ip, 9092)]);
      var metadata = await _session.getMetadata([_topicName].toSet());
      var leaderId =
          metadata.getTopicMetadata(_topicName).getPartition(0).leader;
      _host = metadata.getBroker(leaderId);
    });

    tearDown(() async {
      await _session.close();
    });

    test('it fetches messages from Kafka topic', () async {
      var now = new DateTime.now();
      _message = 'test:' + now.toIso8601String();
      ProduceRequest produce = new ProduceRequest(1, 1000, [
        new ProduceEnvelope(_topicName, 0, [new Message(_message.codeUnits)])
      ]);

      ProduceResponse produceResponse = await _session.send(_host, produce);
      _offset = produceResponse.results.first.offset;
      _request = new FetchRequest(100, 1);
      _request.add(_topicName, 0, _offset);
      FetchResponse response = await _session.send(_host, _request);

      expect(response.results, hasLength(1));
      expect(response.results.first.messageSet,
          hasLength(greaterThanOrEqualTo(1)));
      var value = response.results.first.messageSet.messages[_offset].value;
      var text = new String.fromCharCodes(value);
      expect(text, equals(_message));
    });

    test('it fetches GZip encoded messages from Kafka topic', () async {
      var now = new DateTime.now();
      _message = 'test:' + now.toIso8601String();
      ProduceRequest produce = new ProduceRequest(1, 1000, [
        new ProduceEnvelope(
            _topicName,
            0,
            [
              new Message('hello world'.codeUnits),
              new Message('peace and love'.codeUnits)
            ],
            compression: KafkaCompression.gzip)
      ]);

      ProduceResponse produceResponse = await _session.send(_host, produce);
      _offset = produceResponse.results.first.offset;
      _request = new FetchRequest(100, 1);
      _request.add(_topicName, 0, _offset);
      FetchResponse response = await _session.send(_host, _request);

      expect(response.results, hasLength(1));
      expect(response.results.first.messageSet, hasLength(equals(2)));
      var value = response.results.first.messageSet.messages[_offset].value;
      var text = new String.fromCharCodes(value);
      expect(text, equals('hello world'));

      value = response.results.first.messageSet.messages[_offset + 1].value;
      text = new String.fromCharCodes(value);
      expect(text, equals('peace and love'));
    });
  });
}

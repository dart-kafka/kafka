import 'package:kafka/ng.dart';
import 'package:test/test.dart';

void main() {
  group('FetchApi:', () {
    String _topicName = 'dartKafkaTest';
    Broker _host;
    KSession _session;
    String _message;

    setUp(() async {
      _session =
          new KSession(contactPoints: [new ContactPoint('127.0.0.1:9092')]);
      var metadata = new KMetadata(session: _session);
      var meta = await metadata.fetchTopics([_topicName]);
      var leaderId = meta
          .firstWhere((_) => _.topicName == _topicName)
          .partitions
          .first
          .leader;
      var brokers = await metadata.listBrokers();
      _host = brokers.firstWhere((_) => _.id == leaderId);
    });

    tearDown(() async {
      await _session.close();
    });

    test('it fetches messages from Kafka topic', () async {
      var now = new DateTime.now();
      _message = 'test:' + now.toIso8601String();
      var producer = new KProducer(
          new StringSerializer(), new StringSerializer(),
          session: _session);
      var result = await producer
          .send(new ProducerRecord(_topicName, 0, 'key', _message));

      var offset = result.offset;
      FetchRequestV0 request = new FetchRequestV0(100, 1,
          {new TopicPartition(_topicName, 0): new FetchData(offset, 35656)});
      var response = await _session.send(request, _host.host, _host.port);

      expect(response.results, hasLength(1));
      expect(
          response.results.first.messages, hasLength(greaterThanOrEqualTo(1)));
      var keyData = response.results.first.messages[offset].key;
      var valueData = response.results.first.messages[offset].value;
      var deser = new StringDeserializer();
      var value = deser.deserialize(valueData);
      expect(value, equals(_message));
      var key = deser.deserialize(keyData);
      expect(key, equals('key'));
    });

    // test('it fetches GZip encoded messages from Kafka topic', () async {
    //   var now = new DateTime.now();
    //   _message = 'test:' + now.toIso8601String();
    //   ProduceRequest produce = new ProduceRequest(1, 1000, [
    //     new ProduceEnvelope(
    //         _topicName,
    //         0,
    //         [
    //           new Message('hello world'.codeUnits),
    //           new Message('peace and love'.codeUnits)
    //         ],
    //         compression: KafkaCompression.gzip)
    //   ]);
    //
    //   ProduceResponse produceResponse = await _session.send(_host, produce);
    //   _offset = produceResponse.results.first.offset;
    //   _request = new FetchRequest(100, 1);
    //   _request.add(_topicName, 0, _offset);
    //   FetchResponse response = await _session.send(_host, _request);
    //
    //   expect(response.results, hasLength(1));
    //   expect(response.results.first.messageSet, hasLength(equals(2)));
    //   var value = response.results.first.messageSet.messages[_offset].value;
    //   var text = new String.fromCharCodes(value);
    //   expect(text, equals('hello world'));
    //
    //   value = response.results.first.messageSet.messages[_offset + 1].value;
    //   text = new String.fromCharCodes(value);
    //   expect(text, equals('peace and love'));
    // });
  });
}

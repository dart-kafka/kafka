import 'package:kafka/kafka.dart';
import 'package:test/test.dart';

void main() {
  group('FetchApi:', () {
    String topic = 'dartKafkaTest';
    Broker host;
    Session session = new Session(['127.0.0.1:9092']);
    String message;

    setUp(() async {
      var meta = await session.metadata.fetchTopics([topic]);
      var leaderId = meta[topic].partitions[0].leader;
      host = meta.brokers[leaderId];
    });

    tearDownAll(() async {
      await session.close();
    });

    test('it fetches messages from Kafka topic', () async {
      var now = new DateTime.now();
      message = 'test:' + now.toIso8601String();
      var producer = new Producer(
          new StringSerializer(),
          new StringSerializer(),
          new ProducerConfig(bootstrapServers: ['127.0.0.1:9092']));
      var rec = new ProducerRecord(topic, 0, 'key', message);
      producer.add(rec);
      var result = await rec.result;
      var offset = result.offset;
      await producer.close();

      FetchRequest request = new FetchRequest(100, 1);
      request.add(new TopicPartition(topic, 0), new FetchData(offset, 35656));
      var response = await session.send(request, host.host, host.port);

      expect(response.results, hasLength(1));
      expect(
          response.results.first.messages, hasLength(greaterThanOrEqualTo(1)));
      var keyData = response.results.first.messages[offset].key;
      var valueData = response.results.first.messages[offset].value;
      var deser = new StringDeserializer();
      var value = deser.deserialize(valueData);
      expect(value, equals(message));
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

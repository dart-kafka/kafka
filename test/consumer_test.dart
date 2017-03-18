import 'package:kafka/kafka.dart';
import 'package:test/test.dart';

void main() {
  group('Consumer:', () {
    Session session = new Session(['127.0.0.1:9092']);
    var date = new DateTime.now().millisecondsSinceEpoch;
    String topic = 'testTopic-${date}';
    Map<int, int> expectedOffsets = new Map();
    String group = 'cg:${date}';

    setUp(() async {
      var producer = new Producer(
          new StringSerializer(),
          new StringSerializer(),
          new ProducerConfig(bootstrapServers: ['127.0.0.1:9092']));
      var rec1 = new ProducerRecord(topic, 0, 'akey', 'avalue');
      var rec2 = new ProducerRecord(topic, 1, 'bkey', 'bvalue');
      var rec3 = new ProducerRecord(topic, 2, 'ckey', 'cvalue');
      producer..add(rec1)..add(rec2)..add(rec3);
      var res1 = await rec1.result;
      expectedOffsets[res1.topicPartition.partition] = res1.offset;
      var res2 = await rec2.result;
      expectedOffsets[res2.topicPartition.partition] = res2.offset;
      var res3 = await rec3.result;
      expectedOffsets[res3.topicPartition.partition] = res3.offset;
      await producer.close();
    });

    tearDownAll(() async {
      await session.close();
    });

    test('it can consume messages from multiple brokers', () async {
      var consumer = new Consumer<String, String>(
          group, new StringDeserializer(), new StringDeserializer(), session);
      await consumer.subscribe([topic]);
      var iterator = consumer.poll();
      int i = 0;
      var consumedOffsets = new Map();
      while (await iterator.moveNext()) {
        var records = iterator.current;
        records.records.forEach((record) {
          consumedOffsets[record.partition] = record.offset;
          print("Record: [${record.key}, ${record.value}]");
        });
        i += records.records.length;
        if (i >= 3) break;
      }
      expect(consumedOffsets, expectedOffsets);

//      var group = new ConsumerGroup(_session, 'cg');
//      var offsets = await group.fetchOffsets(topics);
//      expect(offsets, hasLength(3));
//      for (var o in offsets) {
//        expect(-1, o.offset);
//      }
    });

    // test('it can consume messages from multiple brokers and commit offsets',
    //     () async {
    //   var topics = {
    //     _topicName: [0, 1, 2].toSet()
    //   };
    //   var consumer = new KConsumer(
    //       _session, new ConsumerGroup(_session, 'cg'), topics, 100, 1);
    //   var consumedOffsets = new Map();
    //   await for (MessageEnvelope envelope in consumer.consume(limit: 3)) {
    //     consumedOffsets[envelope.partitionId] = envelope.offset;
    //     expect(envelope.offset, _expectedOffsets[envelope.partitionId]);
    //     envelope.commit('');
    //   }
    //   expect(consumedOffsets.length, _expectedOffsets.length);
    // });

    // test('it can handle cancelation request', () async {
    //   var topics = {
    //     _topicName: [0, 1, 2].toSet()
    //   };
    //   var consumer = new Consumer(
    //       _session, new ConsumerGroup(_session, 'cg'), topics, 100, 1);
    //   var consumedOffsets = new Map();
    //   await for (MessageEnvelope envelope in consumer.consume(limit: 3)) {
    //     consumedOffsets[envelope.partitionId] = envelope.offset;
    //     expect(envelope.offset, _expectedOffsets[envelope.partitionId]);
    //     envelope.cancel();
    //   }
    //   expect(consumedOffsets.length, equals(1));
    // });

    // test('it propagates worker errors via stream controller', () async {
    //   var topics = {
    //     'someTopic':
    //         [0, 1, 2, 3].toSet() // request partition which does not exist.
    //   };
    //
    //   var consume = () async {
    //     try {
    //       var consumer = new Consumer(
    //           _session, new ConsumerGroup(_session, 'cg'), topics, 100, 1);
    //       var consumedOffsets = new Map();
    //       await for (MessageEnvelope envelope in consumer.consume(limit: 3)) {
    //         envelope.ack();
    //       }
    //       return false;
    //     } catch (e) {
    //       return true;
    //     }
    //   };
    //
    //   var result = await consume();
    //
    //   expect(result, isTrue);
    // });

    // test('it can consume batches of messages from multiple brokers', () async {
    //   var topics = {
    //     _topicName: [0, 1, 2].toSet()
    //   };
    //   var consumer = new Consumer(
    //       _session, new ConsumerGroup(_session, 'cg'), topics, 100, 1);
    //   var consumedOffsets = new Map();
    //
    //   var first, last;
    //   await for (var batch in consumer.batchConsume(3)) {
    //     if (first == null) {
    //       first = batch;
    //       first.ack();
    //     } else if (last == null) {
    //       last = batch;
    //       last.cancel();
    //     }
    //   }
    //
    //   expect(first.items.length + last.items.length, 3);
    //
    //   for (var i in first.items) {
    //     consumedOffsets[i.partitionId] = i.offset;
    //     expect(i.offset, _expectedOffsets[i.partitionId]);
    //   }
    //   for (var i in last.items) {
    //     consumedOffsets[i.partitionId] = i.offset;
    //     expect(i.offset, _expectedOffsets[i.partitionId]);
    //   }
    //
    //   expect(consumedOffsets.length, _expectedOffsets.length);
    // });
  });
}

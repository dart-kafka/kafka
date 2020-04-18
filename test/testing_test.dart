void main() {
  // group('MockKafkaSession: ', () {
  //   test('it returns cluster and topic metadata', () async {
  //     var session = new MockKafkaSession();
  //     var meta = await session.getMetadata(['test'].toSet());
  //     expect(meta.brokers, hasLength(1));
  //     expect(meta.getBroker(1), new isInstanceOf<Broker>());
  //     expect(meta.getBroker(1).host, '127.0.0.1');
  //     expect(meta.getBroker(1).port, 9092);
  //     expect(meta.topics, hasLength(1));
  //     expect(meta.getTopicMetadata('test'), new isInstanceOf<TopicMetadata>());
  //     expect(meta.getTopicMetadata('test').partitions, hasLength(1));
  //   });

  //   test('it returns consumer metadata', () async {
  //     var session = new MockKafkaSession();
  //     var meta = await session.getConsumerMetadata('myGroup');
  //     expect(meta, new isInstanceOf<GroupCoordinatorResponse>());
  //     expect(meta.errorCode, 0);
  //     expect(meta.coordinatorId, 1);
  //     expect(meta.coordinatorHost, '127.0.0.1');
  //     expect(meta.coordinatorPort, 9092);
  //   });
  // });

  // group('Producer Mock: ', () {
  //   test('it produces messages to MockSession', () async {
  //     var session = new MockKafkaSession();
  //     var producer = new Producer(session, -1, 1000);
  //     var message = 'helloworld';
  //     var messages = [
  //       new ProduceEnvelope('test', 0, [new Message(message.codeUnits)])
  //     ];
  //     var result = await producer.produce(messages);
  //     expect(result, new isInstanceOf<ProduceResult>());
  //     expect(result.offsets, {
  //       "test": {0: 0}
  //     });

  //     result = await producer.produce(messages);
  //     expect(result, new isInstanceOf<ProduceResult>());
  //     expect(result.offsets, {
  //       "test": {0: 1}
  //     });
  //   });
  // });

  // group('ConsumerGroup Mock: ', () {
  //   test('it joins a group', () async {
  //     var session = new MockKafkaSession();
  //     var group = new ConsumerGroup(session, 'myGroup');
  //     var result = await group.join(10000, '', 'consumer', [
  //       new GroupProtocol.roundrobin(0, ['test'].toSet())
  //     ]);
  //     expect(result, new isInstanceOf<GroupMembership>());
  //   });
  // });
  //
  // group('HighLevelConsumer Mock: ', () {
  //   KafkaSession session;
  //   ConsumerGroup group;
  //   HighLevelConsumer consumer;
  //
  //   setUp(() async {
  //     var now = new DateTime.now().millisecondsSinceEpoch;
  //     var groupName = 'group-' + now.toString();
  //     var topic = 'test-topic-' + now.toString();
  //     session = new MockKafkaSession();
  //     group = new ConsumerGroup(session, groupName);
  //     consumer = new HighLevelConsumer(session, [topic].toSet(), group);
  //
  //     var producer = new Producer(session, -1, 1000);
  //     var message = 'helloworld';
  //     var messages = [
  //       new ProduceEnvelope(topic, 0, [new Message(message.codeUnits)])
  //     ];
  //     await producer.produce(messages);
  //   });
  //
  //   test('it consumes messages', () async {
  //     var message;
  //     await for (var env in consumer.stream) {
  //       message = new String.fromCharCodes(env.message.value);
  //       env.commit('');
  //       break;
  //     }
  //     expect(message, 'helloworld');
  //   });
  // });
}

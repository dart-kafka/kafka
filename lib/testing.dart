/// Provides testing utilities for Kafka-driven apps.
///
/// Includes implementations of `MockSession`, `MockProducer`,
/// `MockConsumer` which conform to corresponding interfaces.
library kafka.testing;

// import 'package:kafka/ng.dart';
// import 'dart:async';
// import 'dart:collection';

// class MockKafkaSession implements Session {
//   @override
//   Future close() {
//     return null;
//     // no-op
//   }

//   @override
//   Future<GroupCoordinatorResponse> getConsumerMetadata(String consumerGroup) {
//     return new Future.value(
//         new GroupCoordinatorResponse(0, 1, '127.0.0.1', 9092));
//   }

//   @override
//   Future<ClusterMetadata> getMetadata(Set<String> topicNames,
//       {bool invalidateCache: false}) {
//     var brokers = [new Broker(1, '127.0.0.1', 9092)];
//     var topics = topicNames.map((name) {
//       return new TopicMetadata(0, name, [
//         new PartitionMetadata(0, 0, 1, [1], [1])
//       ]);
//     }).toList();
//     return new Future.value(new ClusterMetadata(brokers, topics));
//   }

//   @override
//   Future<Set<String>> listTopics() {
//     throw new UnsupportedError('Unsupported by MockKafkaSession.');
//   }

//   Map<String, Map<int, List<Message>>> _data =
//       new Map<String, Map<int, List<Message>>>();

//   @override
//   Future send(Broker broker, KafkaRequest request) {
//     switch (request.runtimeType) {
//       case ProduceRequest:
//         return new Future.value(_produce(request));
//       case JoinGroupRequest:
//         return new Future.value(_joinGroup(request));
//       case SyncGroupRequest:
//         return new Future.value(_syncGroup(request));
//       case OffsetFetchRequest:
//         return new Future.value(_offsetFetch(request));
//       case OffsetCommitRequest:
//         return new Future.value(_offsetCommit(request));
//       case FetchRequest:
//         return new Future.value(_fetch(request));
//       case HeartbeatRequest:
//         return new Future.value(
//             new HeartbeatResponse(KafkaServerError.NoError_));
//       default:
//         return null;
//     }
//   }

//   ProduceResponse _produce(ProduceRequest request) {
//     var results = new List<TopicProduceResult>();
//     for (var envelope in request.messages) {
//       _data.putIfAbsent(envelope.topicName, () => new Map());
//       _data[envelope.topicName]
//           .putIfAbsent(envelope.partitionId, () => new List());
//       var offset = _data[envelope.topicName][envelope.partitionId].length;
//       results.add(new TopicProduceResult(
//           envelope.topicName, envelope.partitionId, 0, offset));
//       _data[envelope.topicName][envelope.partitionId].addAll(envelope.messages);
//     }

//     return new ProduceResponse(results);
//   }

//   JoinGroupResponse _joinGroup(JoinGroupRequest request) {
//     var id =
//         'dart_kafka-' + new DateTime.now().millisecondsSinceEpoch.toString();
//     var meta = request.groupProtocols.first.protocolMetadata;
//     return new JoinGroupResponse(
//         0,
//         1,
//         request.groupProtocols.first.protocolName,
//         id,
//         id,
//         [new GroupMember(id, meta)]);
//   }

//   SyncGroupResponse _syncGroup(SyncGroupRequest request) {
//     return new SyncGroupResponse(
//         0, request.groupAssignments.first.memberAssignment);
//   }

//   Map<String, Map<String, Map<int, int>>> consumerOffsets = new Map();

//   OffsetFetchResponse _offsetFetch(OffsetFetchRequest request) {
//     consumerOffsets.putIfAbsent(
//         request.consumerGroup, () => new Map<String, Map<int, int>>());
//     Map<String, Map<int, int>> groupOffsets =
//         consumerOffsets[request.consumerGroup];
//     var offsets = new List<ConsumerOffset>();
//     for (var topic in request.topics.keys) {
//       groupOffsets.putIfAbsent(topic, () => new Map<int, int>());
//       for (var partition in request.topics[topic]) {
//         groupOffsets[topic].putIfAbsent(partition, () => -1);
//         offsets.add(new ConsumerOffset(
//             topic, partition, groupOffsets[topic][partition], ''));
//       }
//     }

//     return new OffsetFetchResponse.fromOffsets(offsets);
//   }

//   OffsetCommitResponse _offsetCommit(OffsetCommitRequest request) {
//     var groupOffsets = consumerOffsets[request.consumerGroup];
//     List<OffsetCommitResult> results = [];
//     for (var offset in request.offsets) {
//       groupOffsets[offset.topicName][offset.partitionId] = offset.offset;
//       results.add(new OffsetCommitResult(
//           offset.topicName, offset.partitionId, KafkaServerError.NoError_));
//     }

//     return new OffsetCommitResponse(results);
//   }

//   FetchResponse _fetch(FetchRequest request) {
//     List<FetchResult> results = new List();
//     for (var topic in request.topics.keys) {
//       for (var p in request.topics[topic]) {
//         var messages = _data[topic][p.partitionId].asMap();
//         var requestedMessages = new Map<int, Message>();
//         for (var o in messages.keys) {
//           if (o >= p.fetchOffset) {
//             requestedMessages[o] = messages[o];
//           }
//         }
//         var messageSet = new MessageSet(requestedMessages);
//         results.add(new FetchResult(topic, p.partitionId,
//             KafkaServerError.NoError_, messages.length, messageSet));
//       }
//     }
//     return new FetchResponse(results);
//   }
// }

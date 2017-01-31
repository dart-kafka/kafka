import 'dart:async';

import 'package:logging/logging.dart';

import '../util/retry.dart';
import 'common.dart';
import 'consumer_offset_api.dart';
import 'errors.dart';
import 'fetch_api.dart';
import 'group_membership_api.dart';
import 'metadata.dart';
import 'partition_assignor.dart';
import 'serialization.dart';
import 'session.dart';

abstract class KConsumer<K, V> {
  StreamIterator<KConsumerRecords> poll();
  Future subscribe(List<String> topics);
  Future unsubscribe();

  factory KConsumer(String groupName, Deserializer<K> keyDeserializer,
      Deserializer<V> valueDeserializer,
      {KSession session}) {
    return new _KConsumerImpl(
        groupName, keyDeserializer, valueDeserializer, session);
  }
}

class _KConsumerImpl<K, V> implements KConsumer<K, V> {
  static final Logger _logger = new Logger('KConsumer');
  final String groupName;
  final KSession session;
  final Deserializer<K> keyDeserializer;
  final Deserializer<V> valueDeserializer;

  _KConsumerImpl(this.groupName, this.keyDeserializer, this.valueDeserializer,
      KSession session)
      : session = (session is KSession) ? session : KAFKA_DEFAULT_SESSION;

  StreamController<KConsumerRecords> _streamController;
  StreamIterator<KConsumerRecords> _streamIterator;

  @override
  StreamIterator<KConsumerRecords> poll() {
    if (membership == null) throw new StateError('No active subscription.');
    if (_streamController != null)
      throw new StateError('Polling already started.');

    _streamController = new StreamController<KConsumerRecords>(
        onPause: _onPause, onResume: _onResume, onCancel: _onCancel);
    _streamIterator =
        new StreamIterator<KConsumerRecords>(_streamController.stream);
    _poll().whenComplete(() {
      _streamController = null;
      _streamIterator = null;
    });
    return _streamIterator;
  }

  void _onPause() {}
  void _onResume() {}
  void _onCancel() {}

  Map<TopicPartition, int> _currentOffsets;

  Future _poll() async {
    List<KConsumerRecord<K, V>> fetchResultsToRecords(
        List<FetchResult> results) {
      return results.expand((result) {
        return result.messages.keys.map((offset) {
          var key = keyDeserializer.deserialize(result.messages[offset].key);
          var value =
              valueDeserializer.deserialize(result.messages[offset].value);
          return new KConsumerRecord<K, V>(
              result.topicName, result.partitionId, offset, key, value);
        });
      });
    }

    _currentOffsets = await _fetchOffsets();
    while (true) {
      Map<Broker, FetchRequestV0> requests = await _buildRequests();
      var futures = requests.keys.map((broker) {
        return session
            .send(requests[broker], broker.host, broker.port)
            .then((response) {
          var records = fetchResultsToRecords(response.results);
          _streamController.add(new KConsumerRecords(records));
        });
      });

      Future.wait(futures);
    }
  }

  Future<Map<TopicPartition, int>> _fetchOffsets() async {
    Future<OffsetFetchResponseV1> fetchFunc() async {
      var request = new OffsetFetchRequestV1(
          groupName, membership.assignment.partitionAssignment);
      var coord = await _getCoordinator();
      return await session.send(request, coord.host, coord.port);
    }

    var response =
        await retryAsync(fetchFunc, 5, new Duration(milliseconds: 500));

    return new Map.fromIterable(response.offsets,
        key: (_) => new TopicPartition(_.topicName, _.partitionId),
        value: (_) => _.offset);
  }

  Future<Map<Broker, FetchRequestV0>> _buildRequests() async {
    // membership.assignment.
  }

  GroupMembership _membership;
  GroupMembership get membership => _membership;
  bool _isSubscribing = false;

  @override
  Future subscribe(List<String> topics) {
    if (_isSubscribing)
      throw new StateError('Subscription already in progress.');
    _isSubscribing = true;
    var protocols = [new GroupProtocol.roundrobin(0, topics.toSet())];
    _logger.info('Joining to consumer group ${groupName}.');
    return _join(30000, '', 'consumer', protocols).then((result) {
      _membership = result;
    }).whenComplete(() {
      _isSubscribing = false;
    });
  }

  @override
  Future unsubscribe() {
    // TODO: implement unsubscribe
    return null;
  }

  Future<Broker> _coordinator;
  Future<Broker> _getCoordinator({bool refresh: false}) {
    if (refresh) {
      _coordinator = null;
    }

    if (_coordinator == null) {
      var metadata = new KMetadata(session: session);
      _coordinator =
          metadata.fetchGroupCoordinator(groupName).catchError((error) {
        _coordinator = null;
        throw error;
      });
    }

    return _coordinator;
  }

  Future<GroupMembership> _join(int sessionTimeout, String memberId,
      String protocolType, Iterable<GroupProtocol> groupProtocols) async {
    var broker = await _getCoordinator();
    var joinRequest = new JoinGroupRequestV0(
        groupName, sessionTimeout, memberId, protocolType, groupProtocols);
    JoinGroupResponseV0 joinResponse =
        await session.send(joinRequest, broker.host, broker.port);
    var protocol = joinResponse.groupProtocol;
    var isLeader = joinResponse.leaderId == joinResponse.memberId;

    var groupAssignments = new List<GroupAssignment>();
    if (isLeader) {
      groupAssignments = await _assignPartitions(protocol, joinResponse);
    }

    var syncRequest = new SyncGroupRequestV0(groupName,
        joinResponse.generationId, joinResponse.memberId, groupAssignments);
    SyncGroupResponseV0 syncResponse;
    try {
      // Wait before sending SyncRequest to give the server some time to respond
      // to all the rest JoinRequests.
      syncResponse = await new Future.delayed(new Duration(seconds: 1), () {
        return session.send(syncRequest, broker.host, broker.port);
      });

      return new GroupMembership(
          joinResponse.memberId,
          joinResponse.leaderId,
          syncResponse.assignment,
          joinResponse.generationId,
          joinResponse.groupProtocol);
    } on RebalanceInProgressError {
      _logger.warning(
          'Received "RebalanceInProgress" error code for SyncRequest, will attempt to rejoin again now.');
      return _join(sessionTimeout, memberId, protocolType, groupProtocols);
    }
  }

  Future<List<GroupAssignment>> _assignPartitions(
      String protocol, JoinGroupResponseV0 joinResponse) async {
    var groupAssignments = new List<GroupAssignment>();
    var assignor = new PartitionAssignor.forStrategy(protocol);
    var topics = new Set<String>();
    Map<String, Set<String>> subscriptions = new Map();
    joinResponse.members.forEach((m) {
      var memberProtocol = new GroupProtocol.fromBytes(protocol, m.metadata);
      subscriptions[m.id] = memberProtocol.topics;
    });
    subscriptions.values.forEach(topics.addAll);

    var metadata = new KMetadata(session: session);
    var meta = await metadata.fetchTopics(topics.toList());
    var partitionsPerTopic = new Map<String, int>.fromIterable(meta,
        key: (_) => _.topicName, value: (_) => _.partitions.length);

    Map<String, List<TopicPartition>> assignments =
        assignor.assign(partitionsPerTopic, subscriptions);

    for (var memberId in assignments.keys) {
      var topics = assignments[memberId].map((_) => _.topicName).toSet();
      var partitionAssignment = new Map<String, List<int>>.fromIterable(topics,
          value: (_) => new List<int>());
      for (var topicPartition in assignments[memberId]) {
        var topic = topicPartition.topicName;
        partitionAssignment[topic].add(topicPartition.partitionId);
      }
      groupAssignments.add(new GroupAssignment(
          memberId, new MemberAssignment(0, partitionAssignment, null)));
    }

    return groupAssignments;
  }
}

class GroupMembership {
  final String memberId;
  final String leaderId;
  final MemberAssignment assignment;
  final int generationId;
  final String groupProtocol;

  GroupMembership(this.memberId, this.leaderId, this.assignment,
      this.generationId, this.groupProtocol);

  bool get isLeader => leaderId == memberId;
}

class KConsumerRecord<K, V> {
  final String topic;
  final int partition;
  final int offset;
  final K key;
  final V value;

  KConsumerRecord(
      this.topic, this.partition, this.offset, this.key, this.value);
}

class KConsumerRecords<K, V> {
  final List<KConsumerRecord> records;

  KConsumerRecords(this.records);
}

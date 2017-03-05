import 'dart:async';

import 'package:logging/logging.dart';

import '../util/retry.dart';
import '../util/tuple.dart';
import 'common.dart';
import 'consumer_offset_api.dart';
import 'errors.dart';
import 'fetch_api.dart';
import 'group_membership_api.dart';
import 'metadata.dart';
import 'partition_assignor.dart';
import 'serialization.dart';
import 'session.dart';

final Logger _logger = new Logger('KConsumer');

/// Consumes messages from Kafka cluster.
///
/// Consumer interacts with the server to allow multiple members of the same group
/// load balance consumption by distributing topics and partitions evenly across
/// all members.
///
/// ## Usage example
///
///     void main() async {
///       var session = new KSession();
///       var consumer = new KConsumer<String, String>(
///         'test_group', new StringDeserializer(), new StringDeserializer(), session);
///       await consumer.subscribe(['foo', 'bar']);
///       var iterator = consumer.poll();
///       while (await iterator.moveNext()) {
///         KConsumerRecords records = iterator.current;
///         records.records.forEach((_) {
///           print('offset: ${_.offset}, key: ${_.key}, value: ${_.value}');
///         });
///       }
///     }
abstract class Consumer<K, V> {
  /// The consumer group name.
  String get group;

  /// Starts polling Kafka servers for new messages.
  StreamIterator<KConsumerRecords<K, V>> poll();

  /// Subscribes to [topics] as a member of consumer [group].
  Future subscribe(List<String> topics);

  Future unsubscribe();

  /// Commits current offsets to the server.
  Future commit();

  factory Consumer(String groupName, Deserializer<K> keyDeserializer,
      Deserializer<V> valueDeserializer, Session session) {
    return new _ConsumerImpl(
        groupName, keyDeserializer, valueDeserializer, session);
  }
}

/// Default implementation of Kafka consumer.
class _ConsumerImpl<K, V> implements Consumer<K, V> {
  static const int DEFAULT_MAX_BYTES = 36864;
  static const int DEFAULT_MAX_WAIT_TIME = 1000;
  static const int DEFAULT_MIN_BYTES = 1;

  final String group;
  final Session session;
  final Deserializer<K> keyDeserializer;
  final Deserializer<V> valueDeserializer;
  final int requestMaxBytes;

  _ConsumerImpl(
      this.group, this.keyDeserializer, this.valueDeserializer, this.session,
      {int requestMaxBytes})
      : requestMaxBytes = requestMaxBytes ?? DEFAULT_MAX_BYTES;

  StreamController<KConsumerRecords> _streamController;
  StreamIterator<KConsumerRecords> _streamIterator;

  @override
  StreamIterator<KConsumerRecords<K, V>> poll() {
    assert(membership != null,
        'No active subscription. Must first call KConsumer.subscribe().');
    assert(_streamController == null, 'Polling already started.');

    _streamController = new StreamController<KConsumerRecords>(
        onPause: onPause, onResume: onResume, onCancel: onCancel);
    _streamIterator =
        new StreamIterator<KConsumerRecords>(_streamController.stream);
    _poll().whenComplete(() {
      _streamController = null;
      _streamIterator = null;
    });
    return _streamIterator;
  }

  Completer _resumeCompleter;
  Future get resumeFuture => _resumeCompleter.future;
  void onPause() {
    assert(_resumeCompleter == null);
    _resumeCompleter = new Completer();
  }

  void onResume() {
    assert(_resumeCompleter is Completer && !_resumeCompleter.isCompleted);
    _resumeCompleter.complete();
    _resumeCompleter = null;
  }

  bool _isCanceled = false;
  void onCancel() {
    _isCanceled = true;
  }

  Map<TopicPartition, int> _currentOffsets;

  /// Internal polling method.
  Future _poll() async {
    _logger.info('Fetching initial offsets');
    _currentOffsets = await _fetchOffsets();
    _logger.info('Current offsets are: ${_currentOffsets}');

    List<KConsumerRecord<K, V>> fetchResultsToRecords(
        List<FetchResult> results) {
      return results.expand((result) {
        return result.messages.keys.map((offset) {
          var key = keyDeserializer.deserialize(result.messages[offset].key);
          var value =
              valueDeserializer.deserialize(result.messages[offset].value);
          return new KConsumerRecord<K, V>(
              result.topic, result.partition, offset, key, value);
        });
      }).toList(growable: false);
    }

    void updateOffsets(List<KConsumerRecord> records) {
      for (var record in records) {
        var topicPartition = new TopicPartition(record.topic, record.partition);
        var current = _currentOffsets[topicPartition];
        if (record.offset > current) {
          _currentOffsets[topicPartition] = record.offset + 1;
        }
      }
    }

    // TODO: Implement a more efficient polling algorithm.
    while (true) {
      if (_isCanceled) {
        _streamController.close();
        break;
      }
      if (_streamController.isPaused) {
        await resumeFuture;
      }

      Map<Broker, FetchRequest> requests =
          await _buildRequests(_currentOffsets);
      var futures = requests.keys.map((broker) {
        return session
            .send(requests[broker], broker.host, broker.port)
            .then((response) {
          var records = fetchResultsToRecords(response.results);
          updateOffsets(records);
          _streamController.add(new KConsumerRecords(records));
        });
      });
      // Depending on configuration this can be very inefficient.
      // It always waits for all responses before returning to the user.
      await Future.wait(futures);
    }
  }

  Future<Map<TopicPartition, int>> _fetchOffsets() async {
    Future<OffsetFetchResponse> fetchFunc() async {
      var request = new OffsetFetchRequest(
          group, membership.assignment.partitionAssignment);
      var coord = await _getCoordinator();
      return await session.send(request, coord.host, coord.port);
    }

    var response =
        await retryAsync(fetchFunc, 5, new Duration(milliseconds: 500));

    return new Map.fromIterable(response.offsets,
        key: (_) => new TopicPartition(_.topic, _.partition),
        value: (_) => _.offset);
  }

  Future<Map<Broker, FetchRequest>> _buildRequests(
      Map<TopicPartition, int> offsets) async {
    // TODO: There should be a better way to do all these traversals...
    var assignment = membership.assignment.partitionAssignment;
    var topics =
        membership.assignment.partitionAssignment.keys.toList(growable: false);
    var topicBrokers = await _fetchTopicMetadata(topics);

    List<Tuple3<Broker, TopicPartition, int>> data = topics.expand((topic) {
      return assignment[topic].map((p) {
        var topicPartition = new TopicPartition(topic, p);
        var broker = topicBrokers[topicPartition];
        var offset = offsets[topicPartition];
        offset = (offset == -1) ? 0 : offset;
        return tuple3(broker, topicPartition, offset);
      });
    }).toList(growable: false);

    Map<Broker, FetchRequest> requests = new Map();
    for (var item in data) {
      requests.putIfAbsent(item.$1,
          () => new FetchRequest(DEFAULT_MAX_WAIT_TIME, DEFAULT_MIN_BYTES));
      requests[item.$1].add(item.$2, new FetchData(item.$3, requestMaxBytes));
    }
    return requests;
  }

  Future<Map<TopicPartition, Broker>> _topicBrokers;
  Future<Map<TopicPartition, Broker>> _fetchTopicMetadata(List<String> topics) {
    if (_topicBrokers == null) {
      _topicBrokers = new Future(() async {
        var meta = new Metadata(session);
        var topicsMeta = await meta.fetchTopics(topics);
        var brokers = await meta.listBrokers();
        List<Tuple3<String, int, int>> data = topicsMeta.expand((_) {
          return _.partitions.map((p) => tuple3(_.topic, p.id, p.leader));
        }).toList(growable: false);
        return new Map<TopicPartition, Broker>.fromIterable(data, key: (_) {
          return new TopicPartition(_.$1, _.$2);
        }, value: (_) {
          return brokers.firstWhere((b) => b.id == _.$3);
        });
      });
    }
    return _topicBrokers;
  }

  GroupMembership _membership;

  GroupMembership get membership => _membership;
  bool _isSubscribing = false;

  @override
  Future subscribe(List<String> topics) {
    assert(!_isSubscribing, 'Subscription already in progress.');
    _logger.info('Subscribing to topics $topics as a member of group $group');
    _isSubscribing = true;
    var protocols = [new GroupProtocol.roundrobin(0, topics.toSet())];
    _logger.info('Joining to consumer group ${group}.');
    return _join(30000, '', 'consumer', protocols).then((result) {
      _membership = result;
      _logger.info('Subscription result: ${membership}.');
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
      var metadata = new Metadata(session);
      _coordinator = metadata.fetchGroupCoordinator(group).catchError((error) {
        _coordinator = null;
        throw error;
      });
    }

    return _coordinator;
  }

  Future<GroupMembership> _join(int sessionTimeout, String memberId,
      String protocolType, Iterable<GroupProtocol> groupProtocols) async {
    var broker = await _getCoordinator();
    var joinRequest = new JoinGroupRequest(
        group, sessionTimeout, memberId, protocolType, groupProtocols);
    JoinGroupResponse joinResponse =
        await session.send(joinRequest, broker.host, broker.port);
    var protocol = joinResponse.groupProtocol;
    var isLeader = joinResponse.leaderId == joinResponse.memberId;

    var groupAssignments = new List<GroupAssignment>();
    if (isLeader) {
      groupAssignments = await _assignPartitions(protocol, joinResponse);
    }

    var syncRequest = new SyncGroupRequest(group, joinResponse.generationId,
        joinResponse.memberId, groupAssignments);
    SyncGroupResponse syncResponse;
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
      String protocol, JoinGroupResponse joinResponse) async {
    var groupAssignments = new List<GroupAssignment>();
    var assignor = new PartitionAssignor.forStrategy(protocol);
    var topics = new Set<String>();
    Map<String, Set<String>> subscriptions = new Map();
    joinResponse.members.forEach((m) {
      var memberProtocol = new GroupProtocol.fromBytes(protocol, m.metadata);
      subscriptions[m.id] = memberProtocol.topics;
    });
    subscriptions.values.forEach(topics.addAll);

    var metadata = new Metadata(session);
    var meta = await metadata.fetchTopics(topics.toList());
    var partitionsPerTopic = new Map<String, int>.fromIterable(meta,
        key: (_) => _.topic, value: (_) => _.partitions.length);

    Map<String, List<TopicPartition>> assignments =
        assignor.assign(partitionsPerTopic, subscriptions);

    for (var memberId in assignments.keys) {
      var topics = assignments[memberId].map((_) => _.topic).toSet();
      var partitionAssignment = new Map<String, List<int>>.fromIterable(topics,
          value: (_) => new List<int>());
      for (var topicPartition in assignments[memberId]) {
        var topic = topicPartition.topic;
        partitionAssignment[topic].add(topicPartition.partition);
      }
      groupAssignments.add(new GroupAssignment(
          memberId, new MemberAssignment(0, partitionAssignment, null)));
    }

    return groupAssignments;
  }

  @override
  Future commit() async {
    assert(_streamIterator.current != null);
    // Get current offsets.
    // Send OffsetCommitRequest to the coordinator node.
  }

  Future heartbeat(GroupMembership membership) async {
    var host = await _getCoordinator();
    var request = new HeartbeatRequest(
        group, membership.generationId, membership.memberId);
    _logger.fine(
        'Sending heartbeat for member ${membership.memberId} (generationId: ${membership.generationId})');
    // TODO: retry, handle coordinator errors...
    return session.send(request, host.host, host.port);
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
  final List<KConsumerRecord<K, V>> records;

  KConsumerRecords(this.records);
}

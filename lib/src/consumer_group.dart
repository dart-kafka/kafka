import 'dart:async';

import 'package:logging/logging.dart';

import 'common.dart';
import 'consumer_offset_api.dart';
import 'errors.dart';
import 'group_membership_api.dart';
import 'offset_commit_api.dart';
import 'offset_master.dart';
import 'partition_assignor.dart';
import 'session.dart';

final Logger _logger = new Logger('ConsumerGroup');

class ConsumerGroup {
  /// The session to communicate with Kafka cluster.
  final Session session;

  /// The unique name of this group.
  final String name;

  /// Optional retention time for committed offsets. If `null` then broker's
  /// offset retention time will be used as default.
  final Duration retentionTime;

  Future<Broker> _coordinatorHost;

  ConsumerGroup(this.session, this.name, {this.retentionTime});

  /// Sends heartbeat for the member specified by [subscription].
  Future heartbeat(GroupSubscription subscription) async {
    var host = await _getCoordinator();
    var request = new HeartbeatRequest(
        name, subscription.generationId, subscription.memberId);
    _logger.fine(
        'Sending heartbeat for member ${subscription.memberId} (generationId: ${subscription.generationId})');
    return session.send(request, host.host, host.port);
  }

  /// Retrieves offsets of this consumer group for specified [partitions].
  Future<List<ConsumerOffset>> fetchOffsets(
      List<TopicPartition> partitions) async {
    return _fetchOffsets(partitions, retries: 3);
  }

  /// Internal method for fetching offsets with retries.
  Future<List<ConsumerOffset>> _fetchOffsets(List<TopicPartition> partitions,
      {int retries: 0, bool refresh: false}) async {
    var broker = await _getCoordinator(refresh: refresh);
    var request = new OffsetFetchRequest(name, partitions);
    try {
      var response = await session.send(request, broker.host, broker.port);
      return new List<ConsumerOffset>.from(response.offsets);
    } on NotCoordinatorForConsumerError {
      if (retries > 1) {
        _logger.info(
            'GroupMember(${name}): encountered NotCoordinatorForConsumerError(16) when fetching offsets. '
            'Scheduling retry with metadata refresh.');
        return _fetchOffsets(partitions, retries: retries - 1, refresh: true);
      } else {
        rethrow;
      }
    } on OffsetsLoadInProgressError {
      if (retries > 1) {
        _logger.info(
            'GroupMember(${name}): encountered OffsetsLoadInProgressError(14) when fetching offsets. '
            'Scheduling retry after delay.');
        return new Future<List<ConsumerOffset>>.delayed(
            const Duration(seconds: 1), () async {
          return _fetchOffsets(partitions, retries: retries - 1);
        });
      } else {
        rethrow;
      }
    }
  }

  /// Commits provided [partitions] to the server for this consumer group.
  Future commitOffsets(List<ConsumerOffset> offsets,
      {GroupSubscription subscription}) {
    return _commitOffsets(offsets, subscription: subscription, retries: 3);
  }

  /// Internal method for commiting offsets with retries.
  Future _commitOffsets(List<ConsumerOffset> offsets,
      {GroupSubscription subscription,
      int retries: 0,
      bool refresh: false}) async {
    try {
      var host = await _getCoordinator(refresh: refresh);
      var generationId = subscription?.generationId ?? -1;
      var memberId = subscription?.memberId ?? '';
      var retentionInMsecs = retentionTime?.inMilliseconds ?? -1;
      var request = new OffsetCommitRequest(
          name, offsets, generationId, memberId, retentionInMsecs);

      _logger.fine(
          "commiting offsets: group_id: $name on $host member_id: $memberId");
      await session.send(request, host.host, host.port);
    } on NotCoordinatorForConsumerError {
      if (retries > 1) {
        _logger.info(
            'ConsumerGroup(${name}): encountered NotCoordinatorForConsumerError(16) when commiting offsets. '
            'Scheduling retry with metadata refresh.');
        return _commitOffsets(offsets,
            subscription: subscription, retries: retries - 1, refresh: true);
      } else {
        rethrow;
      }
    }
  }

  Future resetOffsetsToEarliest(List<TopicPartition> topicPartitions,
      {GroupSubscription subscription}) async {
    var offsetMaster = new OffsetMaster(session);
    var earliestOffsets = await offsetMaster.fetchEarliest(topicPartitions);
    var offsets = new List<ConsumerOffset>();
    for (var earliest in earliestOffsets) {
      // When consuming we always pass `currentOffset + 1` to fetch next
      // message so here we need to substract 1 from earliest offset, otherwise
      // we'll end up in an infinite loop of "InvalidOffset" errors.
      var actualOffset = earliest.offset - 1;
      offsets.add(new ConsumerOffset(
          earliest.topic, earliest.partition, actualOffset, 'resetToEarliest'));
    }

    return commitOffsets(offsets, subscription: subscription);
  }

  // Future resetOffsetsToLatest(Map<String, Set<int>> topicPartitions,
  //     {GroupMembershipInfo membership}) async {
  //   var offsetMaster = new OffsetMaster(session);
  //   var latestOffsets = await offsetMaster.fetchLatest(topicPartitions);
  //   var offsets = new List<ConsumerOffset>();
  //   for (var latest in latestOffsets) {
  //     var actualOffset = latest.offset - 1;
  //     offsets.add(new ConsumerOffset(latest.topicName, latest.partitionId,
  //         actualOffset, 'resetToEarliest'));
  //   }
  //
  //   return commitOffsets(offsets, membership: membership);
  // }

  /// Returns instance of coordinator host for this consumer group.
  Future<Broker> _getCoordinator({bool refresh: false}) {
    if (refresh) {
      _coordinatorHost = null;
    }

    if (_coordinatorHost == null) {
      _coordinatorHost =
          session.metadata.fetchGroupCoordinator(name).catchError((error) {
        _coordinatorHost = null;
        _logger.severe('Error fetching consumer coordinator.', error);
        throw error;
      });
    }

    return _coordinatorHost;
  }

  Future<GroupSubscription> join(
      int sessionTimeout,
      int rebalanceTimeout,
      String memberId,
      String protocolType,
      Iterable<GroupProtocol> groupProtocols) async {
    var broker = await _getCoordinator();
    var joinRequest = new JoinGroupRequest(name, sessionTimeout,
        rebalanceTimeout, memberId, protocolType, groupProtocols);
    JoinGroupResponse joinResponse =
        await session.send(joinRequest, broker.host, broker.port);
    var protocol = joinResponse.groupProtocol;
    var isLeader = joinResponse.leaderId == joinResponse.memberId;

    var groupAssignments = new List<GroupAssignment>();
    if (isLeader) {
      groupAssignments = await _assignPartitions(protocol, joinResponse);
    }

    var syncRequest = new SyncGroupRequest(name, joinResponse.generationId,
        joinResponse.memberId, groupAssignments);
    SyncGroupResponse syncResponse;
    try {
      // Wait before sending SyncRequest to give the server some time to respond
      // to all the rest JoinRequests.
      syncResponse = await new Future.delayed(new Duration(seconds: 1), () {
        return session.send(syncRequest, broker.host, broker.port);
      });

      return new GroupSubscription(
          joinResponse.memberId,
          joinResponse.leaderId,
          syncResponse.assignment,
          joinResponse.generationId,
          joinResponse.groupProtocol);
    } on RebalanceInProgressError {
      _logger.warning(
          'Received "RebalanceInProgress" error code for SyncRequest, will attempt to rejoin again now.');
      return join(sessionTimeout, rebalanceTimeout, memberId, protocolType,
          groupProtocols);
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

    var meta = await session.metadata.fetchTopics(topics.toList());
    var partitionsPerTopic = new Map<String, int>.fromIterable(meta.asList,
        key: (_) => _.name, value: (_) => _.partitions.length);

    Map<String, List<TopicPartition>> assignments =
        assignor.assign(partitionsPerTopic, subscriptions);
    for (var memberId in assignments.keys) {
      var partitionAssignment = new Map<String, List<int>>();
      assignments[memberId].forEach((topicPartition) {
        partitionAssignment.putIfAbsent(
            topicPartition.topic, () => new List<int>());
        partitionAssignment[topicPartition.topic].add(topicPartition.partition);
      });
      groupAssignments.add(new GroupAssignment(
          memberId, new MemberAssignment(0, partitionAssignment, null)));
    }

    return groupAssignments;
  }

  //
  // Future leave(GroupMembershipInfo membership) async {
  //   _logger.info('Attempting to leave group "${name}".');
  //   var host = await _getCoordinator();
  //   var request = new LeaveGroupRequest(name, membership.memberId);
  //   return session.send(host, request).catchError((error) {
  //     _logger.warning('Received ${error} on attempt to leave group gracefully. '
  //         'Ignoring the error to let current session timeout.');
  //   });
  // }
}

class GroupSubscription {
  final String memberId;
  final String leaderId;
  final MemberAssignment assignment;
  final int generationId;
  final String groupProtocol;

  GroupSubscription(this.memberId, this.leaderId, this.assignment,
      this.generationId, this.groupProtocol);

  bool get isLeader => leaderId == memberId;

  @override
  String toString() =>
      'GroupSubscription{memberId: $memberId, leaderId: $leaderId, assignment: $assignment, generationId: $generationId, protocol: $groupProtocol}';
}

import 'common.dart';
import 'dart:async';
import 'package:logging/logging.dart';
import 'session.dart';
import 'metadata.dart';
import 'errors.dart';
import 'group_membership_api.dart';
import 'partition_assignor.dart';

/// Data structure representing consumer offset.
class ConsumerOffset {
  final String topicName;
  final int partitionId;
  final int offset;
  final String metadata;
  final int errorCode;

  ConsumerOffset(this.topicName, this.partitionId, this.offset, this.metadata,
      [this.errorCode]);
}

class GroupMembership {
  static final Logger _logger = new Logger('ConsumerGroup');

  /// The session to communicate with Kafka cluster.
  final KSession session;

  /// The unique name of this group.
  final String name;

  /// Optional retention time for committed offsets. If `null` then broker's
  /// offset retention time will be used as default.
  final Duration retentionTime;

  Future<Broker> _coordinatorHost;

  GroupMembership(this.session, this.name, {this.retentionTime});

  /// Retrieves offsets of this consumer group from the server.
  ///
  /// Keys in [topicPartitions] map are topic names and values are corresponding
  /// partition IDs.
  // Future<List<ConsumerOffset>> fetchOffsets(
  //     Map<String, Set<int>> topicPartitions) async {
  //   return _fetchOffsets(topicPartitions, retries: 3);
  // }

  /// Internal method for fetching offsets with retries.
  // Future<List<ConsumerOffset>> _fetchOffsets(
  //     Map<String, Set<int>> topicPartitions,
  //     {int retries: 0,
  //     bool refresh: false}) async {
  //   var host = await _getCoordinator(refresh: refresh);
  //   var request = new OffsetFetchRequest(name, topicPartitions);
  //   try {
  //     var response = await session.send(host, request);
  //     return new List<ConsumerOffset>.from(response.offsets);
  //   } on NotCoordinatorForConsumerError {
  //     if (retries > 1) {
  //       _logger.info(
  //           'ConsumerGroup(${name}): encountered NotCoordinatorForConsumerError(16) when fetching offsets. '
  //           'Scheduling retry with metadata refresh.');
  //       return _fetchOffsets(topicPartitions,
  //           retries: retries - 1, refresh: true);
  //     } else {
  //       rethrow;
  //     }
  //   } on OffsetsLoadInProgressError {
  //     if (retries > 1) {
  //       _logger.info(
  //           'ConsumerGroup(${name}): encountered OffsetsLoadInProgressError(14) when fetching offsets. '
  //           'Scheduling retry after delay.');
  //       return new Future<List<ConsumerOffset>>.delayed(
  //           const Duration(seconds: 1), () async {
  //         return _fetchOffsets(topicPartitions, retries: retries - 1);
  //       });
  //     } else {
  //       rethrow;
  //     }
  //   }
  // }

  /// Commits provided [offsets] to the server for this consumer group.
  // Future commitOffsets(List<ConsumerOffset> offsets,
  //     {GroupMembershipInfo membership}) {
  //   return _commitOffsets(offsets, membership: membership, retries: 3);
  // }

  /// Internal method for commiting offsets with retries.
  // Future _commitOffsets(List<ConsumerOffset> offsets,
  //     {GroupMembershipInfo membership,
  //     int retries: 0,
  //     bool refresh: false}) async {
  //   try {
  //     var host = await _getCoordinator(refresh: refresh);
  //     var generationId =
  //         (membership is GroupMembershipInfo) ? membership.generationId : -1;
  //     var memberId =
  //         (membership is GroupMembershipInfo) ? membership.memberId : '';
  //     var retentionInMsecs =
  //         (retentionTime is Duration) ? retentionTime.inMilliseconds : -1;
  //     var request = new OffsetCommitRequest(
  //         name, offsets, generationId, memberId, retentionInMsecs);
  //     await session.send(host, request);
  //   } on NotCoordinatorForConsumerError {
  //     if (retries > 1) {
  //       _logger.info(
  //           'ConsumerGroup(${name}): encountered NotCoordinatorForConsumerError(16) when commiting offsets. '
  //           'Scheduling retry with metadata refresh.');
  //       return _commitOffsets(offsets,
  //           membership: membership, retries: retries - 1, refresh: true);
  //     } else {
  //       rethrow;
  //     }
  //   }
  // }

  // Future resetOffsetsToEarliest(Map<String, Set<int>> topicPartitions,
  //     {GroupMembershipInfo membership}) async {
  //   var offsetMaster = new OffsetMaster(session);
  //   var earliestOffsets = await offsetMaster.fetchEarliest(topicPartitions);
  //   var offsets = new List<ConsumerOffset>();
  //   for (var earliest in earliestOffsets) {
  //     // When consuming we always pass `currentOffset + 1` to fetch next
  //     // message so here we need to substract 1 from earliest offset, otherwise
  //     // we'll end up in an infinite loop of "InvalidOffset" errors.
  //     var actualOffset = earliest.offset - 1;
  //     offsets.add(new ConsumerOffset(earliest.topicName, earliest.partitionId,
  //         actualOffset, 'resetToEarliest'));
  //   }
  //
  //   return commitOffsets(offsets, membership: membership);
  // }

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
      var metadata = new KMetadata(session);
      _coordinatorHost =
          metadata.fetchGroupCoordinator(name).catchError((error) {
        _coordinatorHost = null;
        throw error;
      });
    }

    return _coordinatorHost;
  }

  Future<GroupMembershipInfo> join(int sessionTimeout, String memberId,
      String protocolType, Iterable<GroupProtocol> groupProtocols) async {
    var broker = await _getCoordinator();
    var joinRequest = new JoinGroupRequestV0(
        name, sessionTimeout, memberId, protocolType, groupProtocols);
    JoinGroupResponseV0 joinResponse =
        await session.send(joinRequest, broker.host, broker.port);
    var protocol = joinResponse.groupProtocol;
    var isLeader = joinResponse.leaderId == joinResponse.memberId;

    var groupAssignments = new List<GroupAssignment>();
    if (isLeader) {
      groupAssignments = await _assignPartitions(protocol, joinResponse);
    }

    var syncRequest = new SyncGroupRequestV0(name, joinResponse.generationId,
        joinResponse.memberId, groupAssignments);
    SyncGroupResponseV0 syncResponse;
    try {
      // Wait before sending SyncRequest to give the server some time to respond
      // to all the rest JoinRequests.
      syncResponse = await new Future.delayed(new Duration(seconds: 1), () {
        return session.send(syncRequest, broker.host, broker.port);
      });

      return new GroupMembershipInfo(
          joinResponse.memberId,
          joinResponse.leaderId,
          syncResponse.assignment,
          joinResponse.generationId,
          joinResponse.groupProtocol);
    } on RebalanceInProgressError {
      _logger.warning(
          'Received "RebalanceInProgress" error code for SyncRequest, will attempt to rejoin again now.');
      return join(sessionTimeout, memberId, protocolType, groupProtocols);
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

    var metadata = new KMetadata(session);
    var meta = await metadata.fetchTopics(topics.toList());
    var partitionsPerTopic = new Map<String, int>.fromIterable(meta,
        key: (_) => _.topic, value: (_) => _.partitions.length);

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

class GroupMembershipInfo {
  final String memberId;
  final String leaderId;
  final MemberAssignment assignment;
  final int generationId;
  final String groupProtocol;

  GroupMembershipInfo(this.memberId, this.leaderId, this.assignment,
      this.generationId, this.groupProtocol);

  bool get isLeader => leaderId == memberId;
}

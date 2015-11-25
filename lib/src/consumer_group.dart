part of kafka;

class ConsumerGroup {
  final KafkaSession session;
  final String name;

  Broker _coordinatorHost;

  ConsumerGroup(this.session, this.name);

  /// Retrieves offsets of this consumer group from the server.
  ///
  /// Keys in [topicPartitions] map are topic names and values are corresponding
  /// partition IDs.
  Future<List<ConsumerOffset>> fetchOffsets(
      Map<String, Set<int>> topicPartitions) async {
    return _fetchOffsets(topicPartitions, retries: 3);
  }

  /// Internal method for fetching offsets with retries.
  Future<List<ConsumerOffset>> _fetchOffsets(
      Map<String, Set<int>> topicPartitions,
      {int retries: 0,
      bool refresh: false}) async {
    var host = await _getCoordinator(refresh: refresh);
    var request = new OffsetFetchRequest(name, topicPartitions);
    var response = await session.send(host, request);
    var offsets = new List<ConsumerOffset>.from(response.offsets);

    for (var offset in offsets) {
      var error = new KafkaServerError(offset.errorCode);
      if (error.isNotCoordinatorForConsumer && retries > 1) {
        // Re-fetch coordinator metadata and try again
        kafkaLogger?.info(
            'ConsumerGroup(${name}): encountered API error 16 (NotCoordinatorForConsumerCode) when fetching offsets. Scheduling retry with metadata refresh.');
        return _fetchOffsets(topicPartitions,
            retries: retries - 1, refresh: true);
      } else if (error.isOffsetsLoadInProgress && retries > 1) {
        // Wait a little and try again.
        kafkaLogger?.info(
            'ConsumerGroup(${name}): encountered API error 14 (OffsetsLoadInProgressCode) when fetching offsets. Scheduling retry after delay.');
        return new Future.delayed(const Duration(seconds: 1), () async {
          return _fetchOffsets(topicPartitions, retries: retries - 1);
        });
      } else if (error.isError) {
        kafkaLogger?.info(
            'ConsumerGroup(${name}): fetchOffsets failed. Error code: ${offset.errorCode} for partition ${offset.partitionId} of ${offset.topicName}.');
        throw error;
      }
    }

    return offsets;
  }

  /// Commits provided [offsets] to the server for this consumer group.
  Future commitOffsets(List<ConsumerOffset> offsets, int consumerGenerationId,
      String consumerId) async {
    return _commitOffsets(offsets, consumerGenerationId, consumerId,
        retries: 3);
  }

  /// Internal method for commiting offsets with retries.
  Future _commitOffsets(
      List<ConsumerOffset> offsets, int consumerGenerationId, String consumerId,
      {int retries: 0, bool refresh: false}) async {
    var host = await _getCoordinator(refresh: refresh);
    var request = new OffsetCommitRequest(
        name, offsets, consumerGenerationId, consumerId);
    OffsetCommitResponse response = await session.send(host, request);
    for (var offset in response.offsets) {
      var error = new KafkaServerError(offset.errorCode);
      if (error.isNotCoordinatorForConsumer && retries > 1) {
        // Re-fetch coordinator metadata and try again
        kafkaLogger?.info(
            'ConsumerGroup(${name}): encountered API error 16 (NotCoordinatorForConsumerCode) when commiting offsets. Scheduling retry with metadata refresh.');
        return _commitOffsets(offsets, consumerGenerationId, consumerId,
            retries: retries - 1, refresh: true);
      } else if (error.isError) {
        kafkaLogger?.info(
            'ConsumerGroup(${name}): commitOffsets failed. Error code: ${offset.errorCode} for partition ${offset.partitionId} of ${offset.topicName}.');
        throw error;
      }
    }

    return null;
  }

  Future resetOffsetsToEarliest(Map<String, Set<int>> topicPartitions) async {
    var offsetMaster = new OffsetMaster(session);
    var earliestOffsets = await offsetMaster.fetchEarliest(topicPartitions);
    var offsets = new List<ConsumerOffset>();
    for (var earliest in earliestOffsets) {
      offsets.add(new ConsumerOffset(earliest.topicName, earliest.partitionId,
          earliest.offset, 'resetToEarliest'));
    }

    return commitOffsets(offsets, 0, '');
  }

  /// Returns instance of coordinator host for this consumer group.
  Future<Broker> _getCoordinator({bool refresh: false}) async {
    if (refresh) {
      _coordinatorHost = null;
    }

    if (_coordinatorHost == null) {
      var metadata = await session.getConsumerMetadata(name);
      _coordinatorHost = new Broker(metadata.coordinatorId,
          metadata.coordinatorHost, metadata.coordinatorPort);
    }

    return _coordinatorHost;
  }
}

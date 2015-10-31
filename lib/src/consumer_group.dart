part of kafka;

class ConsumerGroup {
  final KafkaClient client;
  final String name;
  final Logger logger;

  KafkaHost _coordinatorHost;

  ConsumerGroup(this.client, this.name, [this.logger]);

  /// Retrieves offsets of this consumer group from the server.
  ///
  /// Keys in [topicPartitions] map are topic names and values are corresponding
  /// partition IDs.
  Future<Map<String, List<ConsumerOffset>>> fetchOffsets(
      Map<String, Set<int>> topicPartitions) async {
    return _fetchOffsets(topicPartitions, retries: 3);
  }

  /// Internal method for fetching offsets with retries.
  Future<Map<String, List<ConsumerOffset>>> _fetchOffsets(
      Map<String, Set<int>> topicPartitions,
      {int retries: 0,
      bool refresh: false}) async {
    var host = await _getCoordinatorHost(refresh: refresh);
    var request = new OffsetFetchRequest(client, host, name, topicPartitions);
    var response = await request.send();
    var offsets = new Map<String, List<ConsumerOffset>>.from(response.offsets);

    for (var topic in offsets.keys) {
      for (var partition in offsets[topic]) {
        if (partition.errorCode == 16 && retries > 1) {
          // Re-fetch coordinator metadata and try again
          logger?.info(
              'ConsumerGroup(${name}): encountered API error 16 (NotCoordinatorForConsumerCode) when fetching offsets. Scheduling retry with metadata refresh.');
          return _fetchOffsets(topicPartitions,
              retries: retries - 1, refresh: true);
        } else if (partition.errorCode == 14 && retries > 1) {
          // Wait a little and try again.
          logger?.info(
              'ConsumerGroup(${name}): encountered API error 14 (OffsetsLoadInProgressCode) when fetching offsets. Scheduling retry after delay.');
          return new Future.delayed(const Duration(seconds: 1), () async {
            return _fetchOffsets(topicPartitions, retries: retries - 1);
          });
        } else if (partition.errorCode != 0) {
          logger?.info(
              'ConsumerGroup(${name}): fetchOffsets failed. Error code: ${partition.errorCode} for partition ${partition.partitionId} of ${topic}.');
          throw new KafkaApiError.fromErrorCode(partition.errorCode);
        }
      }
    }

    return offsets;
  }

  /// Commits provided [offsets] to the server for this consumer group.
  Future commitOffsets(Map<String, List<ConsumerOffset>> offsets,
      int consumerGenerationId, String consumerId) async {
    return _commitOffsets(offsets, consumerGenerationId, consumerId,
        retries: 3);
  }

  /// Internal method for commiting offsets with retries.
  Future _commitOffsets(Map<String, List<ConsumerOffset>> offsets,
      int consumerGenerationId, String consumerId,
      {int retries: 0, bool refresh: false}) async {
    var host = await _getCoordinatorHost(refresh: refresh);
    var request = new OffsetCommitRequest(
        client, host, name, offsets, consumerGenerationId, consumerId);
    var response = await request.send();
    for (var topic in response.topics.keys) {
      for (var partition in response.topics[topic]) {
        if (partition.errorCode == 16 && retries > 1) {
          // Re-fetch coordinator metadata and try again
          logger?.info(
              'ConsumerGroup(${name}): encountered API error 16 (NotCoordinatorForConsumerCode) when commiting offsets. Scheduling retry with metadata refresh.');
          return _commitOffsets(offsets, consumerGenerationId, consumerId,
              retries: retries - 1, refresh: true);
        } else if (partition.errorCode != 0) {
          logger?.info(
              'ConsumerGroup(${name}): commitOffsets failed. Error code: ${partition.errorCode} for partition ${partition.partitionId} of ${topic}.');
          throw new KafkaApiError.fromErrorCode(partition.errorCode);
        }
      }
    }

    return null;
  }

  Future resetOffsetsToEarliest(Map<String, Set<int>> topicPartitions) async {
    var meta = await client.getMetadata();
    var requests = new Map<KafkaHost, OffsetRequest>();
    for (var topic in topicPartitions.keys) {
      var partitions = topicPartitions[topic];
      for (var p in partitions) {
        var leader = meta.getTopicMetadata(topic).getPartition(p).leader;
        var host = new KafkaHost(
            meta.getBroker(leader).host, meta.getBroker(leader).port);
        if (!requests.containsKey(host)) {
          requests[host] = new OffsetRequest(client, host, leader);
        }
        requests[host].addTopicPartition(topic, p, -2, 1);
      }
    }

    var offsets = new Map<String, List<ConsumerOffset>>();
    for (var request in requests.values) {
      var response = await request.send();
      for (var topic in response.topics.keys) {
        var partitions = response.topics[topic];
        for (var p in partitions) {
          if (!offsets.containsKey(topic)) {
            offsets[topic] = [];
          }
          print('Reset offset: p=${p.partitionId}, o=${p.offsets.first}');
          offsets[topic].add(new ConsumerOffset(
              p.partitionId, p.offsets.first, 'resetToEarliest'));
        }
      }
    }

    return commitOffsets(offsets, 0, '');
  }

  /// Returns instance of coordinator host for this consumer group.
  Future<KafkaHost> _getCoordinatorHost({bool refresh: false}) async {
    if (refresh) {
      _coordinatorHost = null;
    }

    if (_coordinatorHost == null) {
      var metadata = await client.getConsumerMetadata(name);
      _coordinatorHost =
          new KafkaHost(metadata.coordinatorHost, metadata.coordinatorPort);
    }

    return _coordinatorHost;
  }
}

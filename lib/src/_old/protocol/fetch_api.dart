part of kafka.protocol;

/// Kafka FetchRequest.
class FetchRequest extends KafkaRequest {
  /// API key of [FetchRequest]
  final int apiKey = 1;

  /// API version of [FetchRequest]
  final int apiVersion = 0;

  /// The replica id indicates the node id of the replica initiating this request.
  /// Normal consumers should always specify this as -1 as they have no node id.
  final int _replicaId = -1;

  /// Maximum amount of time in milliseconds to block waiting if insufficient
  /// data is available at the time the request is issued.
  final int maxWaitTime;

  /// Minimum number of bytes of messages that must be available
  /// to give a response.
  final int minBytes;

  Map<String, List<FetchPartitionInfo>> topics = new Map();

  /// Creates new instance of FetchRequest.
  FetchRequest(this.maxWaitTime, this.minBytes) : super();

  @override
  toString() => 'FetchRequest(${maxWaitTime}, ${minBytes}, ${topics})';

  /// Adds [topicName] with [paritionId] to this FetchRequest. [fetchOffset]
  /// defines the offset to begin this fetch from.
  void add(String topicName, int partitionId, int fetchOffset,
      [int maxBytes = 65536]) {
    topics.putIfAbsent(topicName, () => new List());
    topics[topicName]
        .add(new FetchPartitionInfo(partitionId, fetchOffset, maxBytes));
  }

  @override
  List<int> toBytes() {
    var builder = new KafkaBytesBuilder.withRequestHeader(
        apiKey, apiVersion, correlationId);

    builder.addInt32(_replicaId);
    builder.addInt32(maxWaitTime);
    builder.addInt32(minBytes);

    builder.addInt32(topics.length);
    topics.forEach((topicName, partitions) {
      builder.addString(topicName);
      builder.addInt32(partitions.length);
      partitions.forEach((p) {
        builder.addInt32(p.partitionId);
        builder.addInt64(p.fetchOffset);
        builder.addInt32(p.maxBytes);
      });
    });

    var body = builder.takeBytes();
    builder.addBytes(body);

    return builder.takeBytes();
  }

  @override
  createResponse(List<int> data) {
    return new FetchResponse.fromBytes(data);
  }
}

class FetchPartitionInfo {
  final int partitionId;
  final int fetchOffset;
  final int maxBytes;
  FetchPartitionInfo(this.partitionId, this.fetchOffset, this.maxBytes);
}

/// Kafka FetchResponse.
class FetchResponse {
  /// List of [FetchResult]s for each topic-partition.
  final List<FetchResult> results;

  FetchResponse(this.results);

  /// Creates new instance of FetchResponse from binary data.
  factory FetchResponse.fromBytes(List<int> data) {
    var reader = new KafkaBytesReader.fromBytes(data);
    var size = reader.readInt32();
    assert(size == data.length - 4);

    reader.readInt32(); // correlationId
    var count = reader.readInt32();
    var results = new List<FetchResult>();
    var firstErrorCode;
    var erroredTopicPartitions = new List<TopicPartition>();
    while (count > 0) {
      var topicName = reader.readString();
      var partitionCount = reader.readInt32();
      while (partitionCount > 0) {
        var partitionId = reader.readInt32();
        var errorCode = reader.readInt16();
        var highwaterMarkOffset = reader.readInt64();
        var messageSetSize = reader.readInt32();
        var data = reader.readRaw(messageSetSize);
        var messageReader = new KafkaBytesReader.fromBytes(data);
        var messageSet = new MessageSet.fromBytes(messageReader);
        if (errorCode != KafkaServerError.NoError_ && firstErrorCode == null) {
          firstErrorCode = errorCode;
        }
        if (errorCode == KafkaServerError.OffsetOutOfRange) {
          erroredTopicPartitions
              .add(new TopicPartition(topicName, partitionId));
        }

        results.add(new FetchResult(topicName, partitionId, errorCode,
            highwaterMarkOffset, messageSet));
        partitionCount--;
      }
      count--;
    }

    var response = new FetchResponse(results);
    if (firstErrorCode == null) {
      return response;
    } else if (firstErrorCode == KafkaServerError.OffsetOutOfRange) {
      throw new OffsetOutOfRangeError(response, erroredTopicPartitions);
    } else {
      throw new KafkaServerError.fromCode(firstErrorCode, response);
    }
  }
}

/// Data structure representing result of fetching messages for particular
/// topic-partition.
class FetchResult {
  final String topicName;
  final int partitionId;
  final int errorCode;
  final int highwaterMarkOffset;
  final MessageSet messageSet;

  FetchResult(this.topicName, this.partitionId, this.errorCode,
      this.highwaterMarkOffset, this.messageSet);
}

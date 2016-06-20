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

  Map<String, List<_FetchPartitionInfo>> _topics = new Map();

  /// Creates new instance of FetchRequest.
  FetchRequest(this.maxWaitTime, this.minBytes) : super();

  @override
  toString() => 'FetchRequest(${maxWaitTime}, ${minBytes}, ${_topics})';

  /// Adds [topicName] with [paritionId] to this FetchRequest. [fetchOffset]
  /// defines the offset to begin this fetch from.
  void add(String topicName, int partitionId, int fetchOffset,
      [int maxBytes = 65536]) {
    //
    if (!_topics.containsKey(topicName)) {
      _topics[topicName] = new List();
    }
    _topics[topicName]
        .add(new _FetchPartitionInfo(partitionId, fetchOffset, maxBytes));
  }

  @override
  List<int> toBytes() {
    var builder = new KafkaBytesBuilder.withRequestHeader(
        apiKey, apiVersion, correlationId);

    builder.addInt32(_replicaId);
    builder.addInt32(maxWaitTime);
    builder.addInt32(minBytes);

    builder.addInt32(_topics.length);
    _topics.forEach((topicName, partitions) {
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

class _FetchPartitionInfo {
  int partitionId;
  int fetchOffset;
  int maxBytes;
  _FetchPartitionInfo(this.partitionId, this.fetchOffset, this.maxBytes);
}

/// Kafka FetchResponse.
class FetchResponse {
  /// List of [FetchResult]s for each topic-partition.
  final List<FetchResult> results;

  /// Indicates if server returned any errors in this response.
  ///
  /// Actual errors can be found in the result object for particular
  /// topic-partition.
  final bool hasErrors;

  FetchResponse._(this.results, this.hasErrors);

  /// Creates new instance of FetchResponse from binary data.
  factory FetchResponse.fromBytes(List<int> data) {
    var reader = new KafkaBytesReader.fromBytes(data);
    var size = reader.readInt32();
    assert(size == data.length - 4);

    reader.readInt32(); // correlationId
    var count = reader.readInt32();
    var results = new List<FetchResult>();
    var hasErrors = false;
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
        if (errorCode != KafkaServerError.NoError) hasErrors = true;

        results.add(new FetchResult(topicName, partitionId, errorCode,
            highwaterMarkOffset, messageSet));
        partitionCount--;
      }
      count--;
    }

    return new FetchResponse._(results, hasErrors);
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

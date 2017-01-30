import 'dart:io';

import 'package:logging/logging.dart';

import '../util/crc32.dart';
import 'common.dart';
import 'errors.dart';
import 'io.dart';
import 'messages.dart';

/// Kafka FetchRequestV0.
class FetchRequestV0 implements KRequest<FetchResponseV0> {
  final int apiKey = 1;

  final int apiVersion = 0;

  /// The replica id indicates the node id of the replica initiating this request.
  /// Normal consumers should always specify this as -1 as they have no node id.
  static const int replicaId = -1;

  /// Maximum amount of time in milliseconds to block waiting if insufficient
  /// data is available at the time the request is issued.
  final int maxWaitTime;

  /// Minimum number of bytes of messages that must be available
  /// to give a response.
  final int minBytes;

  /// Topics and partitions to fetch messages from.
  final Map<TopicPartition, FetchData> fetchData;

  /// Creates new instance of FetchRequest.
  FetchRequestV0(this.maxWaitTime, this.minBytes, this.fetchData);

  @override
  toString() => 'FetchRequest(${maxWaitTime}, ${minBytes}, ${fetchData})';

  @override
  ResponseDecoder<FetchResponseV0> get decoder => new _FetchResponseV0Decoder();

  @override
  RequestEncoder<KRequest> get encoder => new _FetchRequestV0Encoder();

  Map<String, Map<int, FetchData>> _fetchDatabyTopic;
  Map<String, Map<int, FetchData>> get fetchDataByTopic {
    if (_fetchDatabyTopic == null) {
      var result = new Map<String, Map<int, FetchData>>();
      fetchData.keys.forEach((_) {
        result.putIfAbsent(_.topicName, () => new Map());
        result[_.topicName][_.partitionId] = fetchData[_];
      });
      _fetchDatabyTopic = result;
    }
    return _fetchDatabyTopic;
  }
}

class FetchData {
  final int fetchOffset;
  final int maxBytes;
  FetchData(this.fetchOffset, this.maxBytes);
}

class _FetchRequestV0Encoder implements RequestEncoder<FetchRequestV0> {
  @override
  List<int> encode(FetchRequestV0 request) {
    var builder = new KafkaBytesBuilder();

    builder.addInt32(FetchRequestV0.replicaId);
    builder.addInt32(request.maxWaitTime);
    builder.addInt32(request.minBytes);

    builder.addInt32(request.fetchDataByTopic.length);
    request.fetchDataByTopic.forEach((topic, partitions) {
      builder.addString(topic);
      builder.addInt32(partitions.length);
      partitions.forEach((partition, data) {
        builder.addInt32(partition);
        builder.addInt64(data.fetchOffset);
        builder.addInt32(data.maxBytes);
      });
    });
    return builder.takeBytes();
  }
}

/// Kafka FetchResponseV0.
class FetchResponseV0 {
  /// List of [FetchResult]s for each topic-partition.
  final List<FetchResult> results;

  FetchResponseV0(this.results) {
    var errors = results
        .map((_) => _.errorCode)
        .where((_) => _ != KafkaServerError.NoError_);
    if (errors.isNotEmpty) {
      throw new KafkaServerError.fromCode(errors.first, this);
    }
  }
}

/// Represents result of fetching messages for a particular
/// topic-partition.
class FetchResult {
  final String topicName;
  final int partitionId;
  final int errorCode;
  final int highwaterMarkOffset;
  final Map<int, Message> messages;

  FetchResult(this.topicName, this.partitionId, this.errorCode,
      this.highwaterMarkOffset, this.messages);
}

class _FetchResponseV0Decoder implements ResponseDecoder<FetchResponseV0> {
  final Logger _logger = new Logger('_FetchResponseV0Decoder');

  @override
  FetchResponseV0 decode(List<int> data) {
    var reader = new KafkaBytesReader.fromBytes(data);
    var count = reader.readInt32();
    var results = new List<FetchResult>();
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
        var messageSet = _readMessageSet(messageReader);

        results.add(new FetchResult(topicName, partitionId, errorCode,
            highwaterMarkOffset, messageSet));
        partitionCount--;
      }
      count--;
    }

    return new FetchResponseV0(results);
  }

  /// Reads a set of messages from FetchResponse.
  Map<int, Message> _readMessageSet(KafkaBytesReader reader) {
    int messageSize = -1;
    var messages = new Map<int, Message>();
    while (reader.isNotEOF) {
      try {
        int offset = reader.readInt64();
        messageSize = reader.readInt32();
        var crc = reader.readInt32();

        var data = reader.readRaw(messageSize - 4);
        var actualCrc = Crc32.signed(data);
        if (actualCrc != crc) {
          _logger.warning(
              'Message CRC sum mismatch. Expected crc: ${crc}, actual: ${actualCrc}');
          throw new MessageCrcMismatchError(
              'Expected crc: ${crc}, actual: ${actualCrc}');
        }
        var messageReader = new KafkaBytesReader.fromBytes(data);
        var message = _readMessage(messageReader);
        if (message.attributes.compression == KafkaCompression.none) {
          messages[offset] = message;
        } else {
          if (message.attributes.compression == KafkaCompression.snappy)
            throw new UnimplementedError(
                'Snappy compression is not supported yet by the client.');

          var codec = new GZipCodec();
          var innerReader =
              new KafkaBytesReader.fromBytes(codec.decode(message.value));
          var innerMessageSet = _readMessageSet(innerReader);
          messages.addAll(innerMessageSet);
        }
      } on RangeError {
        // According to the spec server is allowed to return partial
        // messages, so we just ignore it here and exit the loop.
        var remaining = reader.length - reader.offset;
        _logger.info('Encountered partial message. '
            'Expected message size: ${messageSize}, bytes left in '
            'buffer: ${remaining}, total buffer size ${reader.length}');
        break;
      }
    }
    return messages;
  }

  Message _readMessage(KafkaBytesReader reader) {
    reader.readInt8(); // magicByte
    var attributes = new MessageAttributes.fromByte(reader.readInt8());
    var key = reader.readBytes();
    var value = reader.readBytes();

    return new Message(value, attributes: attributes, key: key);
  }
}

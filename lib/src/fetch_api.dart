import 'dart:io';

import 'package:logging/logging.dart';

import 'common.dart';
import 'errors.dart';
import 'io.dart';
import 'messages.dart';
import 'util/crc32.dart';

final _logger = new Logger('FetchApi');

/// Kafka FetchRequest.
class FetchRequest implements KRequest<FetchResponse> {
  @override
  final int apiKey = ApiKey.fetch;

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
  final Map<TopicPartition, FetchData> fetchData = new Map();

  /// Creates new instance of FetchRequest.
  FetchRequest(this.maxWaitTime, this.minBytes);

  void add(TopicPartition partition, FetchData data) {
    fetchData[partition] = data;
  }

  @override
  toString() => 'FetchRequest{${maxWaitTime}, ${minBytes}, ${fetchData}}';

  @override
  ResponseDecoder<FetchResponse> get decoder => const _FetchResponseDecoder();

  @override
  RequestEncoder<KRequest> get encoder => const _FetchRequestEncoder();

  Map<String, Map<int, FetchData>> _fetchDataByTopic;
  Map<String, Map<int, FetchData>> get fetchDataByTopic {
    if (_fetchDataByTopic == null) {
      var result = new Map<String, Map<int, FetchData>>();
      fetchData.keys.forEach((_) {
        result.putIfAbsent(_.topic, () => new Map());
        result[_.topic][_.partition] = fetchData[_];
      });
      _fetchDataByTopic = result;
    }
    return _fetchDataByTopic;
  }
}

class FetchData {
  final int fetchOffset;
  final int maxBytes;
  FetchData(this.fetchOffset, this.maxBytes);
}

class _FetchRequestEncoder implements RequestEncoder<FetchRequest> {
  const _FetchRequestEncoder();

  @override
  List<int> encode(FetchRequest request, int version) {
    assert(
        version == 2, 'Only v2 of Fetch request is supported by the client.');
    var builder = new KafkaBytesBuilder();

    builder.addInt32(FetchRequest.replicaId);
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

/// Kafka FetchResponse.
class FetchResponse {
  /// Duration in milliseconds for which the request was throttled due to quota
  /// violation. (Zero if the request did not violate any quota.)
  final int throttleTime;

  /// List of [FetchResult]s for each topic-partition.
  final List<FetchResult> results;

  FetchResponse(this.throttleTime, this.results) {
    var errors = results.map((_) => _.error).where((_) => _ != Errors.NoError);
    if (errors.isNotEmpty) {
      throw new KafkaError.fromCode(errors.first, this);
    }
  }
}

/// Represents result of fetching messages for a particular
/// topic-partition.
class FetchResult {
  final String topic;
  final int partition;
  final int error;
  final int highwaterMarkOffset;
  final Map<int, Message> messages;

  FetchResult(this.topic, this.partition, this.error, this.highwaterMarkOffset,
      this.messages);
}

class _FetchResponseDecoder implements ResponseDecoder<FetchResponse> {
  const _FetchResponseDecoder();

  @override
  FetchResponse decode(List<int> data) {
    var reader = new KafkaBytesReader.fromBytes(data);
    var throttleTime = reader.readInt32();
    var count = reader.readInt32();
    var results = new List<FetchResult>();
    while (count > 0) {
      var topic = reader.readString();
      var partitionCount = reader.readInt32();
      while (partitionCount > 0) {
        var partition = reader.readInt32();
        var error = reader.readInt16();
        var highwaterMarkOffset = reader.readInt64();
        var messageSetSize = reader.readInt32();
        var data = reader.readRaw(messageSetSize);
        var messageReader = new KafkaBytesReader.fromBytes(data);
        var messageSet = _readMessageSet(messageReader);

        results.add(new FetchResult(
            topic, partition, error, highwaterMarkOffset, messageSet));
        partitionCount--;
      }
      count--;
    }

    return new FetchResponse(throttleTime, results);
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
        if (message.attributes.compression == Compression.none) {
          messages[offset] = message;
        } else {
          if (message.attributes.compression == Compression.snappy)
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
    final magicByte = reader.readInt8();
    assert(magicByte == 1,
        'Unsupported message format $magicByte. Only version 1 is supported by the client.');
    final attrByte = reader.readInt8();
    final attributes = new MessageAttributes.fromByte(attrByte);
    final timestamp = reader.readInt64();

    final key = reader.readBytes();
    final value = reader.readBytes();
    return new Message(value,
        attributes: attributes, key: key, timestamp: timestamp);
  }
}

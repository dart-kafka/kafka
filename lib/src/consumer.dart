part of kafka;

/// High-level Kafka consumer class.
///
/// Provides convenience layer on top of Kafka's low-level APIs.
///
/// TODO: add `consumeBatch(int maxBatchSize)`
class Consumer {
  /// Instance of [KafkaSession] used to send requests.
  final KafkaSession session;

  /// Consumer group this consumer belongs to.
  final ConsumerGroup consumerGroup;

  /// Topics and partitions to consume.
  final Map<String, Set<int>> topicPartitions;

  /// Maximum amount of time in milliseconds to block waiting if insufficient
  /// data is available at the time the request is issued.
  final int maxWaitTime;

  /// Minimum number of bytes of messages that must be available
  /// to give a response.
  final int minBytes;

  /// Creates new consumer identified by [consumerGroup].
  Consumer(this.session, this.consumerGroup, this.topicPartitions,
      this.maxWaitTime, this.minBytes);

  /// Consumes messages from Kafka. If [limit] is specified consuming
  /// will stop after exactly [limit] messages have been retrieved. If no
  /// specific limit is set it'll default to `-1` and will consume all incoming
  /// messages continuously.
  Stream<MessageEnvelope> consume({int limit: -1}) {
    var controller = new _MessageStreamController(limit);

    Future<List<_HostWorker>> list = _buildWorkers(controller);
    list.then((workers) {
      if (workers.isEmpty) {
        controller.close();
        return;
      }
      var remaining = workers.length;
      List<Future> futures = workers.map((w) => w.run()).toList();
      futures.forEach((f) {
        f.then((_) {
          remaining--;
          if (remaining == 0) {
            _logger?.info('Consumer: All workers are done. Closing stream.');
            controller.close();
          }
        });
      });
    });

    return controller.stream;
  }

  Future<List<_HostWorker>> _buildWorkers(
      _MessageStreamController controller) async {
    var meta = await session.getMetadata();
    var topicsByHost = new Map<KafkaHost, Map<String, Set<int>>>();

    topicPartitions.forEach((topic, partitions) {
      partitions.forEach((p) {
        var leader = meta.getTopicMetadata(topic).getPartition(p).leader;
        var host = new KafkaHost(
            meta.getBroker(leader).host, meta.getBroker(leader).port);
        if (topicsByHost.containsKey(host) == false) {
          topicsByHost[host] = new Map<String, Set<int>>();
        }
        if (topicsByHost[host].containsKey(topic) == false) {
          topicsByHost[host][topic] = new Set<int>();
        }
        topicsByHost[host][topic].add(p);
      });
    });

    var workers = new List<_HostWorker>();
    topicsByHost.forEach((host, topics) {
      workers.add(new _HostWorker(session, host, consumerGroup, controller,
          topics, maxWaitTime, minBytes));
    });

    return workers;
  }
}

class _MessageStreamController {
  final int limit;
  final StreamController<MessageEnvelope> _controller =
      new StreamController<MessageEnvelope>();
  int _added = 0;

  _MessageStreamController(this.limit);

  bool get canAdd => (limit == -1) || (_added < limit);
  Stream<MessageEnvelope> get stream => _controller.stream;

  void add(MessageEnvelope event) {
    _controller.add(event);
    _added++;
  }

  void close() {
    _controller.close();
  }
}

/// Worker responsible for fetching messages from one particular Kafka broker.
class _HostWorker {
  final KafkaSession session;
  final KafkaHost host;
  final ConsumerGroup group;
  final _MessageStreamController controller;
  final Map<String, Set<int>> topicPartitions;
  final int maxWaitTime;
  final int minBytes;

  _HostWorker(this.session, this.host, this.group, this.controller,
      this.topicPartitions, this.maxWaitTime, this.minBytes);

  Future run() async {
    _logger?.info('Consumer: Running worker on host ${host.host}:${host.port}');
    Completer completer = new Completer();

    var shouldContinue = true;
    while (shouldContinue) {
      var request = await _createRequest();
      var response = await request.send();
      var didReset = await _resetOffsetsIfNeeded(response);
      if (didReset) {
        _logger?.warning('Offsets were reset to earliest. Forcing re-fetch.');
        continue;
      }
      var messageCount = 0;
      for (var topic in response.topics.keys) {
        var partitions = response.topics[topic];
        for (var p in partitions) {
          for (var offset in p.messages.messages.keys) {
            var message = p.messages.messages[offset];
            var envelope =
                new MessageEnvelope(topic, p.partitionId, offset, message);
            if (controller.canAdd) {
              messageCount++;
              controller.add(envelope);
              var metadata = await envelope.future;
              var commitOffset = {
                topic: [new ConsumerOffset(p.partitionId, offset, metadata)]
              };
              await group.commitOffsets(commitOffset, 0, '');
            } else {
              completer.complete();
              shouldContinue = false;
              break;
            }
          }
          if (!shouldContinue) break;
        }
        if (!shouldContinue) break;
      }
      if (messageCount == 0 && controller.canAdd == false) {
        completer.complete();
        shouldContinue = false;
      }
    }

    return completer.future;
  }

  Future<bool> _resetOffsetsIfNeeded(FetchResponse response) async {
    var topicsToReset = new Map<String, Set<int>>();
    for (var topic in response.topics.keys) {
      var partitions = response.topics[topic];
      for (var p in partitions) {
        if (p.errorCode == 1) {
          _logger?.warning(
              'Consumer: received API error 1 for topic ${topic}:${p.partitionId}');
          if (!topicsToReset.containsKey(topic)) {
            topicsToReset[topic] = [];
          }
          topicsToReset[topic].add(p.partitionId);
        }
      }
    }

    if (topicsToReset.isNotEmpty) {
      await group.resetOffsetsToEarliest(topicsToReset);
      return true;
    } else {
      return false;
    }
  }

  Future<FetchRequest> _createRequest() async {
    var offsets = await group.fetchOffsets(topicPartitions);
    var request = new FetchRequest(session, host, maxWaitTime, minBytes);
    topicPartitions.forEach((topic, partitions) {
      for (var p in partitions) {
        var offset = offsets[topic].firstWhere((o) => o.partitionId == p);
        request.add(topic, p, offset.offset + 1);
      }
    });

    return request;
  }
}

/// Envelope for a [Message] used by high-level consumer.
class MessageEnvelope {
  final String topicName;
  final int partitionId;
  final int offset;
  final Message message;

  Completer<String> _completer = new Completer<String>();

  MessageEnvelope(this.topicName, this.partitionId, this.offset, this.message);

  Future<String> get future => _completer.future;

  /// Acknowledges that messages has been processed successfully. You must call
  /// [ack] or [nack] when you're done with this message.
  void ack(String metadata) {
    _completer.complete(metadata);
  }

  void nack(String error) {
    _completer.completeError(error);
  }
}

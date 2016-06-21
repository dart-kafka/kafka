part of kafka;

/// Determines behavior of [Consumer] when it receives `OffsetOutOfRange` API
/// error.
enum OffsetOutOfRangeBehavior {
  /// Consumer will throw [KafkaServerError] with error code `1`.
  throwError,

  /// Consumer will reset it's offsets to the earliest available for particular
  /// topic-partition.
  resetToEarliest,

  /// Consumer will reset it's offsets to the latest available for particular
  /// topic-partition.
  resetToLatest
}

/// High-level Kafka consumer class.
///
/// Provides convenience layer on top of Kafka's low-level APIs.
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

  /// Determines this consumer's strategy of handling `OffsetOutOfRange` API
  /// errors.
  ///
  /// Default value is `resetToEarliest` which will automatically reset offset
  /// of ConsumerGroup for particular topic-partition to the earliest offset
  /// available.
  ///
  /// See [OffsetOutOfRangeBehavior] for details on each value.
  OffsetOutOfRangeBehavior onOffsetOutOfRange =
      OffsetOutOfRangeBehavior.resetToEarliest;

  /// Creates new consumer identified by [consumerGroup].
  Consumer(this.session, this.consumerGroup, this.topicPartitions,
      this.maxWaitTime, this.minBytes);

  /// Consumes messages from Kafka. If [limit] is specified consuming
  /// will stop after exactly [limit] messages have been retrieved. If no
  /// specific limit is set it'll default to `-1` and will consume all incoming
  /// messages continuously.
  Stream<MessageEnvelope> consume({int limit: -1}) {
    var controller = new _MessageStreamController(limit);

    Future<List<_ConsumerWorker>> list = _buildWorkers();
    list.then((workers) {
      if (workers.isEmpty) {
        controller.close();
        return;
      }
      var remaining = workers.length;
      var futures = workers.map((w) => w.run(controller)).toList();
      futures.forEach((Future f) {
        f.then((_) {
          remaining--;
          if (remaining == 0) {
            kafkaLogger
                ?.info('Consumer: All workers are done. Closing stream.');
            controller.close();
          }
        }, onError: (error, stackTrace) {
          controller.addError(error, stackTrace);
        });
      });
    }, onError: (error, stackTrace) {
      controller.addError(error, stackTrace);
    });

    return controller.stream;
  }

  /// Consume messages in batches.
  ///
  /// This will create a stream of [BatchEnvelope] objects. Each batch
  /// will contain up to [maxBatchSize] of `MessageEnvelope`s.
  ///
  /// Note that calling `commit`, `ack`, or `cancel` on individual message
  /// envelope will take no effect. Instead one should use corresponding methods
  /// on the BatchEnvelope itself.
  ///
  /// Currently batches are formed on per broker basis, meaning each batch will
  /// always contain messages from one particular broker.
  Stream<BatchEnvelope> batchConsume(int maxBatchSize) {
    var controller = new _BatchStreamController();

    Future<List<_ConsumerWorker>> list = _buildWorkers();
    list.then((workers) {
      if (workers.isEmpty) {
        controller.close();
        return;
      }
      var remaining = workers.length;
      var futures =
          workers.map((w) => w.runBatched(controller, maxBatchSize)).toList();
      futures.forEach((Future f) {
        f.then((_) {
          kafkaLogger.info('Consumer: worker finished.');
          remaining--;
          if (remaining == 0) {
            kafkaLogger
                ?.info('Consumer: All workers are done. Closing stream.');
            controller.close();
          }
        }, onError: (error, stackTrace) {
          controller.addError(error, stackTrace);
        });
      });
    }, onError: (error, stackTrace) {
      controller.addError(error, stackTrace);
    });

    return controller.stream;
  }

  Future<List<_ConsumerWorker>> _buildWorkers() async {
    var meta = await session.getMetadata(topicPartitions.keys.toSet());
    var topicsByBroker = new Map<Broker, Map<String, Set<int>>>();

    topicPartitions.forEach((topic, partitions) {
      partitions.forEach((p) {
        var leader = meta.getTopicMetadata(topic).getPartition(p).leader;
        var broker = meta.getBroker(leader);
        if (topicsByBroker.containsKey(broker) == false) {
          topicsByBroker[broker] = new Map<String, Set<int>>();
        }
        if (topicsByBroker[broker].containsKey(topic) == false) {
          topicsByBroker[broker][topic] = new Set<int>();
        }
        topicsByBroker[broker][topic].add(p);
      });
    });

    var workers = new List<_ConsumerWorker>();
    topicsByBroker.forEach((host, topics) {
      var worker = new _ConsumerWorker(
          session, host, topics, maxWaitTime, minBytes,
          group: consumerGroup);
      worker.onOffsetOutOfRange = onOffsetOutOfRange;
      workers.add(worker);
    });

    return workers;
  }
}

class _MessageStreamController {
  final int limit;
  final StreamController<MessageEnvelope> _controller =
      new StreamController<MessageEnvelope>();
  int _added = 0;
  bool _cancelled = false;

  _MessageStreamController(this.limit);

  bool get canAdd =>
      (_cancelled == false && ((limit == -1) || (_added < limit)));
  Stream<MessageEnvelope> get stream => _controller.stream;

  /// Attempts to add [event] to the stream.
  /// Returns true if adding event succeeded, false otherwise.
  bool add(MessageEnvelope event) {
    if (canAdd) {
      _controller.add(event);
      _added++;
      return true;
    }
    return false;
  }

  void addError(Object error, [StackTrace stackTrace]) {
    _controller.addError(error, stackTrace);
  }

  void cancel() {
    _cancelled = true;
  }

  void close() {
    _controller.close();
  }
}

/// Worker responsible for fetching messages from one particular Kafka broker.
class _ConsumerWorker {
  final KafkaSession session;
  final Broker host;
  final ConsumerGroup group;
  final Map<String, Set<int>> topicPartitions;
  final int maxWaitTime;
  final int minBytes;

  OffsetOutOfRangeBehavior onOffsetOutOfRange =
      OffsetOutOfRangeBehavior.resetToEarliest;

  _ConsumerWorker(this.session, this.host, this.topicPartitions,
      this.maxWaitTime, this.minBytes,
      {this.group});

  Future run(_MessageStreamController controller) async {
    kafkaLogger
        ?.info('Consumer: Running worker on host ${host.host}:${host.port}');

    while (controller.canAdd) {
      var request = await _createRequest();
      kafkaLogger?.fine('Consumer: Sending fetch request to ${host}.');
      FetchResponse response = await session.send(host, request);
      var didReset = await _checkOffsets(response);
      if (didReset) {
        kafkaLogger?.warning(
            'Offsets were reset to ${onOffsetOutOfRange}. Forcing re-fetch.');
        continue;
      }
      for (var item in response.results) {
        for (var offset in item.messageSet.messages.keys) {
          var message = item.messageSet.messages[offset];
          var envelope = new MessageEnvelope(
              item.topicName, item.partitionId, offset, message);
          if (!controller.add(envelope)) {
            return;
          } else {
            var result = await envelope.result;
            if (result.status == _ProcessingStatus.commit) {
              var offsets = [
                new ConsumerOffset(item.topicName, item.partitionId, offset,
                    result.commitMetadata)
              ];
              await group.commitOffsets(offsets, -1, '');
            } else if (result.status == _ProcessingStatus.cancel) {
              controller.cancel();
              return;
            }
          }
        }
      }
    }
  }

  Future runBatched(_BatchStreamController controller, int maxBatchSize) async {
    kafkaLogger?.info(
        'Consumer: Running batch worker on host ${host.host}:${host.port}');

    while (controller.canAdd) {
      var request = await _createRequest();
      FetchResponse response = await session.send(host, request);
      var didReset = await _checkOffsets(response);
      if (didReset) {
        kafkaLogger?.warning(
            'Offsets were reset to ${onOffsetOutOfRange}. Forcing re-fetch.');
        continue;
      }

      for (var batch in responseToBatches(response, maxBatchSize)) {
        if (!controller.add(batch)) return;
        var result = await batch.result;
        if (result.status == _ProcessingStatus.commit) {
          await group.commitOffsets(batch.offsetsToCommit, -1, '');
        } else if (result.status == _ProcessingStatus.cancel) {
          controller.cancel();
          return;
        }
      }
    }
  }

  Iterable<BatchEnvelope> responseToBatches(
      FetchResponse response, int maxBatchSize) sync* {
    BatchEnvelope batch;
    for (var item in response.results) {
      for (var offset in item.messageSet.messages.keys) {
        var message = item.messageSet.messages[offset];
        var envelope = new MessageEnvelope(
            item.topicName, item.partitionId, offset, message);

        if (batch == null) batch = new BatchEnvelope();
        if (batch.items.length < maxBatchSize) {
          batch.items.add(envelope);
        }
        if (batch.items.length == maxBatchSize) {
          yield batch;
          batch = null;
        }
      }
    }
    if (batch is BatchEnvelope && batch.items.isNotEmpty) {
      yield batch;
      batch = null;
    }
  }

  Future<bool> _checkOffsets(FetchResponse response) async {
    var topicsToReset = new Map<String, Set<int>>();
    for (var result in response.results) {
      if (result.errorCode == KafkaServerError.OffsetOutOfRange) {
        kafkaLogger?.warning(
            'Consumer: received API error 1 for topic ${result.topicName}:${result.partitionId}');
        if (!topicsToReset.containsKey(result.topicName)) {
          topicsToReset[result.topicName] = new Set();
        }
        topicsToReset[result.topicName].add(result.partitionId);
        kafkaLogger?.info('Topics to reset: ${topicsToReset}');
      }
    }

    if (topicsToReset.isNotEmpty) {
      switch (onOffsetOutOfRange) {
        case OffsetOutOfRangeBehavior.throwError:
          throw new KafkaServerError(1);
        case OffsetOutOfRangeBehavior.resetToEarliest:
          await group.resetOffsetsToEarliest(topicsToReset);
          break;
        case OffsetOutOfRangeBehavior.resetToLatest:
          await group.resetOffsetsToLatest(topicsToReset);
          break;
      }
      return true;
    } else {
      return false;
    }
  }

  Future<FetchRequest> _createRequest() async {
    var offsets = await group.fetchOffsets(topicPartitions);
    var request = new FetchRequest(maxWaitTime, minBytes);
    for (var o in offsets) {
      request.add(o.topicName, o.partitionId, o.offset + 1);
    }

    return request;
  }
}

enum _ProcessingStatus { commit, ack, cancel }

class _ProcessingResult {
  final _ProcessingStatus status;
  final String commitMetadata;

  _ProcessingResult.commit(String metadata)
      : status = _ProcessingStatus.commit,
        commitMetadata = metadata;
  _ProcessingResult.ack()
      : status = _ProcessingStatus.ack,
        commitMetadata = '';
  _ProcessingResult.cancel()
      : status = _ProcessingStatus.cancel,
        commitMetadata = '';
}

/// Envelope for a [Message] used by high-level consumer.
class MessageEnvelope {
  /// Topic name of this message.
  final String topicName;

  /// Partition ID of this message.
  final int partitionId;

  /// This message's offset
  final int offset;

  /// Actual message received from Kafka broker.
  final Message message;

  Completer<_ProcessingResult> _completer = new Completer<_ProcessingResult>();

  /// Creates new envelope.
  MessageEnvelope(this.topicName, this.partitionId, this.offset, this.message);

  Future<_ProcessingResult> get result => _completer.future;

  /// Signals that message has been processed and it's offset can
  /// be committed (in case of high-level [Consumer] implementation). In case if
  /// consumerGroup functionality is not used (like in the [Fetcher]) then
  /// this method's behaviour will be the same as in [ack] method.
  void commit(String metadata) {
    _completer.complete(new _ProcessingResult.commit(metadata));
  }

  /// Signals that message has been processed and we are ready for
  /// the next one. This method will **not** trigger offset commit if this
  /// envelope has been created by a high-level [Consumer].
  void ack() {
    _completer.complete(new _ProcessingResult.ack());
  }

  /// Signals to consumer to cancel any further deliveries and close the stream.
  void cancel() {
    _completer.complete(new _ProcessingResult.cancel());
  }
}

/// StreamController for batch consuming of messages.
class _BatchStreamController {
  final StreamController<BatchEnvelope> _controller =
      new StreamController<BatchEnvelope>();
  bool _cancelled = false;

  bool get canAdd => (_cancelled == false);
  Stream<BatchEnvelope> get stream => _controller.stream;

  /// Attempts to add [batch] to the stream.
  /// Returns true if adding event succeeded, false otherwise.
  bool add(BatchEnvelope batch) {
    if (canAdd) {
      _controller.add(batch);
      return true;
    }
    return false;
  }

  void addError(Object error, [StackTrace stackTrace]) {
    _controller.addError(error, stackTrace);
  }

  void cancel() {
    _cancelled = true;
  }

  void close() {
    _controller.close();
  }
}

/// Envelope for message batches used by `Consumer.batchConsume`.
class BatchEnvelope {
  final List<MessageEnvelope> items = new List();

  Completer<_ProcessingResult> _completer = new Completer<_ProcessingResult>();
  Future<_ProcessingResult> get result => _completer.future;

  String commitMetadata;

  /// Signals that batch has been processed and it's offsets can
  /// be committed. In case if
  /// consumerGroup functionality is not used (like in the [Fetcher]) then
  /// this method's behaviour will be the same as in [ack] method.
  void commit(String metadata) {
    commitMetadata = metadata;
    _completer.complete(new _ProcessingResult.commit(metadata));
  }

  /// Signals that batch has been processed and we are ready for
  /// the next one. This method will **not** trigger offset commit if this
  /// envelope has been created by a high-level [Consumer].
  void ack() {
    _completer.complete(new _ProcessingResult.ack());
  }

  /// Signals to consumer to cancel any further deliveries and close the stream.
  void cancel() {
    _completer.complete(new _ProcessingResult.cancel());
  }

  Iterable<ConsumerOffset> get offsetsToCommit {
    var grouped = new Map<TopicPartition, int>();
    for (var envelope in items) {
      var key = new TopicPartition(envelope.topicName, envelope.partitionId);
      if (!grouped.containsKey(key)) {
        grouped[key] = envelope.offset;
      } else if (grouped[key] < envelope.offset) {
        grouped[key] = envelope.offset;
      }
    }

    List<ConsumerOffset> offsets = [];
    for (var key in grouped.keys) {
      offsets.add(new ConsumerOffset(
          key.topicName, key.partitionId, grouped[key], commitMetadata));
    }

    return offsets;
  }
}

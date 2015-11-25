part of kafka;

/// Message Fetcher.
///
/// Main difference to [Consumer] is that this class does not store it's state
/// in consumer metadata.
///
/// It will fetch all messages starting from specified [topicOffsets]. If no
/// limit is set it will run forever consuming all incoming messages.
class Fetcher {
  /// Instance of Kafka session.
  final KafkaSession session;

  /// Offsets to start from.
  final List<TopicOffset> topicOffsets;

  Fetcher(this.session, this.topicOffsets);

  /// Consumes messages from Kafka topics.
  ///
  /// It will start from specified [topicOffsets]. If no [limit] is set it will
  /// run continuously consuming all incoming messages.
  Stream<MessageEnvelope> fetch({int limit: -1}) {
    var controller = new _MessageStreamController(limit);

    Future<List<_FetcherWorker>> list = _buildWorkers(controller);
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
            kafkaLogger
                ?.info('Fetcher: All workers are done. Closing the stream.');
            controller.close();
          }
        });
      });
    });

    return controller.stream;
  }

  Future<List<_FetcherWorker>> _buildWorkers(
      _MessageStreamController controller) async {
    var meta = await session.getMetadata();
    var offsetsByBroker = new Map<Broker, List<TopicOffset>>();

    topicOffsets.forEach((offset) {
      var leader = meta
          .getTopicMetadata(offset.topicName)
          .getPartition(offset.partitionId)
          .leader;
      var broker = meta.getBroker(leader);
      if (offsetsByBroker.containsKey(broker) == false) {
        offsetsByBroker[broker] = new List();
      }
      offsetsByBroker[broker].add(offset);
    });

    var workers = new List<_FetcherWorker>();
    offsetsByBroker.forEach((host, offsets) {
      workers
          .add(new _FetcherWorker(session, host, controller, offsets, 100, 1));
    });

    return workers;
  }
}

class _FetcherWorker {
  final KafkaSession session;
  final Broker host;
  final _MessageStreamController controller;
  final List<TopicOffset> startFromOffsets;
  final int maxWaitTime;
  final int minBytes;

  _FetcherWorker(this.session, this.host, this.controller,
      this.startFromOffsets, this.maxWaitTime, this.minBytes);

  Future run() async {
    kafkaLogger
        ?.info('Fetcher: Running worker on host ${host.host}:${host.port}');
    var offsets = startFromOffsets.toList();

    while (controller.canAdd) {
      var request = await _createRequest(offsets);
      var response = await session.send(host, request);
      _checkResponseForErrors(response);

      for (var item in response.messageSets) {
        for (var offset in item.item3.messages.keys) {
          var message = item.item3.messages[offset];
          var envelope =
              new MessageEnvelope(item.item1, item.item2, offset, message);
          if (!controller.add(envelope)) {
            return;
          } else {
            var result = await envelope.result;
            if (result.status == _ProcessingStatus.cancel) {
              controller.cancel();
              return;
            }
          }
        }
        if (item.item3.messages.isNotEmpty) {
          var nextOffset = new TopicOffset(
              item.item1, item.item2, item.item3.messages.keys.last + 1);
          var previousOffset = offsets.firstWhere(
              (o) => o.topicName == item.item1 && o.partitionId == item.item2);
          offsets.remove(previousOffset);
          offsets.add(nextOffset);
        }
      }
    }
  }

  Future<FetchRequest> _createRequest(List<TopicOffset> offsets) async {
    var offsetMaster = new OffsetMaster(session);
    var request = new FetchRequest(maxWaitTime, minBytes);
    for (var o in offsets) {
      if (o.isEarliest) {
        var result = await offsetMaster.fetchEarliest({
          o.topicName: [o.partitionId]
        });
        request.add(result.first.topicName, result.first.partitionId,
            result.first.offset);
      } else {
        request.add(o.topicName, o.partitionId, o.offset);
      }
    }

    return request;
  }

  _checkResponseForErrors(FetchResponse response) {
    if (!response.hasErrors) return;

    for (var topic in response.topics.keys) {
      var partitions = response.topics[topic];
      for (var p in partitions) {
        if (p.errorCode != KafkaServerErrorCode.NoError) {
          throw new KafkaServerError(p.errorCode);
        }
      }
    }
  }
}

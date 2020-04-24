// Copyright (c) 2013, the Dart project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

import "dart:async";
import "package:test/test.dart";
import "package:kafka/src/consumer_streamiterator.dart";
import "package:kafka/src/consumer.dart";

main() {
  test("stream iterator basic", () async {
    var stream = createStream();
    ConsumerStreamIterator<String, String> iterator =
        new ConsumerStreamIterator<String, String>(stream);
    expect(iterator.current, isNull);
    expect(await iterator.moveNext(), isTrue);
    expect(iterator.current.records.first.key, 'k1');
    expect(await iterator.moveNext(), isTrue);
    expect(iterator.current.records.first.key, 'k2');
    expect(await iterator.moveNext(), isFalse);
    expect(iterator.current, isNull);
    expect(await iterator.moveNext(), isFalse);
  });

  test("stream iterator prefilled", () async {
    var stream = createStream();
    ConsumerStreamIterator<String, String> iterator =
        new ConsumerStreamIterator<String, String>(stream);
    await new Future.delayed(Duration.zero);
    expect(iterator.current, isNull);
    expect(await iterator.moveNext(), isTrue);
    expect(iterator.current.records.first.key, 'k1');
    expect(await iterator.moveNext(), isTrue);
    expect(iterator.current.records.first.key, 'k2');
    expect(await iterator.moveNext(), isFalse);
    expect(iterator.current, isNull);
    expect(await iterator.moveNext(), isFalse);
  });

  test("stream iterator error", () async {
    var stream = createErrorStream();
    ConsumerStreamIterator<String, String> iterator =
        new ConsumerStreamIterator<String, String>(stream);
    expect(await iterator.moveNext(), isTrue);
    expect(iterator.current.records.first.key, 'k1');
    var hasNext = iterator.moveNext();
    expect(hasNext, throwsA("BAD")); // This is an async expectation,
    await hasNext.catchError((_) {}); // so we have to wait for the future too.
    expect(iterator.current, isNull);
    expect(await iterator.moveNext(), isFalse);
    expect(iterator.current, isNull);
  });

  test("stream iterator current/moveNext during move", () async {
    var stream = createStream();
    ConsumerStreamIterator<String, String> iterator =
        new ConsumerStreamIterator<String, String>(stream);
    var hasNext = iterator.moveNext();
    expect(iterator.moveNext, throwsA(isStateError));
    expect(await hasNext, isTrue);
    expect(iterator.current.records.first.key, 'k1');
    iterator.cancel();
  });

  test("stream iterator error during cancel", () async {
    var stream = createCancelErrorStream();
    ConsumerStreamIterator<String, String> iterator =
        new ConsumerStreamIterator(stream);
    for (int i = 0; i < 10; i++) {
      expect(await iterator.moveNext(), isTrue);
      expect(iterator.current.records.first.offset, i);
    }
    var hasNext = iterator.moveNext(); // active moveNext will be completed.
    var cancel = iterator.cancel();
    expect(cancel, throwsA("BAD"));
    expect(await hasNext, isFalse);
    expect(await iterator.moveNext(), isFalse);
  });

  test("stream iterator collects offsets of consumed records", () async {
    var stream = createStream();
    ConsumerStreamIterator<String, String> iterator =
        new ConsumerStreamIterator<String, String>(stream);
    await iterator.moveNext();
    expect(iterator.offsets, hasLength(1));
    expect(iterator.offsets.first.offset, 100);
    await iterator.moveNext();
    expect(iterator.offsets, hasLength(2));
    expect(iterator.offsets.last.offset, 200);
    iterator.clearOffsets();
    expect(iterator.offsets, isEmpty);
  });
}

Stream<ConsumerRecords<String, String>> createStream() async* {
  yield new ConsumerRecords(
      [new ConsumerRecord('test', 0, 100, 'k1', 'v1', 0)]);
  yield new ConsumerRecords(
      [new ConsumerRecord('test', 1, 200, 'k2', 'v2', 0)]);
}

Stream<ConsumerRecords<String, String>> createErrorStream() async* {
  yield new ConsumerRecords([new ConsumerRecord('test', 0, 1, 'k1', 'v1', 0)]);
  // Emit an error without stopping the generator.
  yield* (new Future<ConsumerRecords<String, String>>.error("BAD").asStream());
  yield new ConsumerRecords([new ConsumerRecord('test', 1, 1, 'k2', 'v2', 0)]);
}

/// Create a stream that throws when cancelled.
Stream<ConsumerRecords<String, String>> createCancelErrorStream() async* {
  int i = 0;
  try {
    while (true)
      yield new ConsumerRecords(
          [new ConsumerRecord('test', 0, i++, 'k1', 'v1', 0)]);
  } finally {
    throw "BAD";
  }
}

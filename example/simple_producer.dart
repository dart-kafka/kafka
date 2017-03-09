import 'dart:async';
// Import BenchmarkBase class.
import 'package:benchmark_harness/benchmark_harness.dart';
import 'package:kafka/ng.dart';

Future main() async {
  await run();
  await session.close();
}

var session = new Session([new ContactPoint('127.0.0.1:9092')]);
var producer = new Producer<String, String>(
    new StringSerializer(), new StringSerializer(), session);

Future run() {
  List<Future> results = [];
  for (var i = 0; i < 100; i++) {
    // Loop through a list of partitions.
    for (var p in [0, 1, 2]) {
      var future = producer.send(new ProducerRecord(
          'simple_topic', p, 'key:${p},$i', 'value:${p},$i'));
      results.add(future);
    }
  }
  return Future.wait(results);
}

// Create a new benchmark by extending BenchmarkBase.
class TemplateBenchmark extends BenchmarkBase {
  const TemplateBenchmark() : super("Template");

  static Session session;
  static Producer producer;

  static void main() {
    new TemplateBenchmark().report();
  }

  // The benchmark code.
  Future run() {
    List<Future> results = [];
    for (var i = 0; i < 100; i++) {
      // Loop through a list of partitions.
      for (var p in [0, 1, 2]) {
        var future = producer.send(new ProducerRecord(
            'simple_topic', p, 'key:${p},$i', 'value:${p},$i'));
        results.add(future);
      }
    }
    return Future.wait(results);
  }

  @override
  Future exercise() async {
    for (int i = 0; i < 10; i++) {
      await run();
    }
  }

  // Not measured: setup code executed before the benchmark runs.
  void setup() {}

  // Not measured: teardown code executed after the benchmark runs.
  void teardown() {}
}

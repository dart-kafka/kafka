import 'package:kafka/kafka.dart';
import 'package:test/test.dart';

void main() {
  group('RoundRobinPartitionAssignor:', () {
    test('it assignes partitions on one topic', () async {
      var assignor = new PartitionAssignor.forStrategy('roundrobin');
      var partitionsPerTopic = {"foo": 5};
      var memberSubscriptions = {
        "m1": ["foo"].toSet(),
        "m2": ["foo"].toSet()
      };

      var assignments =
          assignor.assign(partitionsPerTopic, memberSubscriptions);

      expect(assignments['m1'], hasLength(3));
      expect(assignments['m1'], contains(new TopicPartition('foo', 0)));
      expect(assignments['m1'], contains(new TopicPartition('foo', 2)));
      expect(assignments['m1'], contains(new TopicPartition('foo', 4)));
      expect(assignments['m2'], hasLength(2));
      expect(assignments['m2'], contains(new TopicPartition('foo', 1)));
      expect(assignments['m2'], contains(new TopicPartition('foo', 3)));
    });

    test('it validates member subscriptions are identical', () {
      var assignor = new PartitionAssignor.forStrategy('roundrobin');
      var partitionsPerTopic = {"foo": 5};
      var memberSubscriptions = {
        "m1": ["foo"].toSet(),
        "m2": ["foo", "bar"].toSet()
      };

      expect(() {
        assignor.assign(partitionsPerTopic, memberSubscriptions);
      }, throwsStateError);
    });
  });
}

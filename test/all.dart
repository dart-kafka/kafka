library kafka.all_tests;

import 'common/errors_test.dart' as errors_test;
import 'common/messages_test.dart' as messages_test;
import 'util/crc32_test.dart' as crc32_test;
import 'protocol/bytes_builder_test.dart' as bytes_builder_test;
import 'protocol/bytes_reader_test.dart' as bytes_reader_test;
import 'protocol/fetch_test.dart' as fetch_test;
import 'protocol/offset_commit_test.dart' as offset_commit_test;
import 'protocol/offset_fetch_test.dart' as offset_fetch_test;
import 'protocol/offset_test.dart' as offset_test;
import 'protocol/produce_test.dart' as produce_test;
import 'session_test.dart' as session_test;
import 'consumer_group_test.dart' as consumer_group_test;
import 'producer_test.dart' as producer_test;
import 'consumer_test.dart' as consumer_test;
import 'fetcher_test.dart' as fetcher_test;

void main() {
  errors_test.main();
  messages_test.main();
  bytes_builder_test.main();
  bytes_reader_test.main();
  crc32_test.main();
  session_test.main();
  fetch_test.main();
  offset_commit_test.main();
  offset_fetch_test.main();
  offset_test.main();
  produce_test.main();
  consumer_group_test.main();
  producer_test.main();
  consumer_test.main();
  fetcher_test.main();
}

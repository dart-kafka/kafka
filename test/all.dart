library kafka.all_tests;

import 'common/errors_test.dart' as errors_test;
import 'common/messages_test.dart' as messages_test;
import 'fetcher_test.dart' as fetcher_test;
import 'producer_test.dart' as producer_test;
import 'io/bytes_builder_test.dart' as bytes_builder_test;
import 'io/bytes_reader_test.dart' as bytes_reader_test;
import 'protocol/fetch_test.dart' as fetch_test;
import 'protocol/offset_commit_test.dart' as offset_commit_test;
import 'protocol/offset_fetch_test.dart' as offset_fetch_test;
import 'protocol/offset_test.dart' as offset_test;
import 'ng/metadata_test.dart' as metadata_test;
import 'util/crc32_test.dart' as crc32_test;
import 'testing_test.dart' as testing_test;

void main() {
  errors_test.main();
  messages_test.main();
  bytes_builder_test.main();
  bytes_reader_test.main();
  crc32_test.main();
  metadata_test.main();
  fetch_test.main();
  offset_commit_test.main();
  offset_fetch_test.main();
  offset_test.main();
  producer_test.main();
  fetcher_test.main();
  testing_test.main();
}

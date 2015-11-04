library kafka.all_tests;

import 'library_test.dart' as library_test;
import 'bytes_builder_test.dart' as bytes_builder_test;
import 'bytes_reader_test.dart' as bytes_reader_test;
import 'util/crc32_test.dart' as crc32_test;
import 'client_test.dart' as client_test;
import 'api/consumer_metadata_test.dart' as consumer_metadata_test;
import 'api/fetch_test.dart' as fetch_test;
import 'api/offset_commit_test.dart' as offset_commit_test;
import 'api/offset_fetch_test.dart' as offset_fetch_test;
import 'api/offset_test.dart' as offset_test;
import 'api/produce_test.dart' as produce_test;
import 'consumer_group_test.dart' as consumer_group_test;
import 'producer_test.dart' as producer_test;
import 'consumer_test.dart' as consumer_test;

void main() {
  library_test.main();
  bytes_builder_test.main();
  bytes_reader_test.main();
  crc32_test.main();
  client_test.main();
  consumer_metadata_test.main();
  fetch_test.main();
  offset_commit_test.main();
  offset_fetch_test.main();
  offset_test.main();
  produce_test.main();
  consumer_group_test.main();
  producer_test.main();
  consumer_test.main();
}

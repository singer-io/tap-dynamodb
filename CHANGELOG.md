# Changelog

## 1.4.0
  * Adds proxy AWS Account support [#59](https://github.com/singer-io/tap-dynamodb/pull/59)

## 1.3.1
  * Removes deprecated terminaltables dependency [#60](https://github.com/singer-io/tap-dynamodb/pull/60)

## 1.3.0
  * Updates to run on python 3.11.7 [#57](https://github.com/singer-io/tap-dynamodb/pull/57)

## 1.2.3
  * Fix error handling for log-based setup [#50](https://github.com/singer-io/tap-dynamodb/pull/50)

## 1.2.2
  * Fix empty string projection filter [#49](https://github.com/singer-io/tap-dynamodb/pull/49)

## 1.2.1
  * Reduce logging by only logging once at the start of a full-table scan [#48](https://github.com/singer-io/tap-dynamodb/pull/48)

## 1.2.0
  * Expression attributes implementation [#37](https://github.com/singer-io/tap-dynamodb/pull/37)
  * Implement request timeout [#38](https://github.com/singer-io/tap-dynamodb/pull/38)
  * Missing parent objects from projection expression [#38](https://github.com/singer-io/tap-dynamodb/pull/37)

## 1.1.2
  * Fix issue where decimals can throw a Rounded signal if they are too wide [#33](https://github.com/singer-io/tap-dynamodb/pull/33)

## 1.1.1
  * Add more error checking to ensure a projection provided is also not empty [#26](https://github.com/singer-io/tap-dynamodb/pull/26)

## 1.1.0
  * Rework of log based sync to only sync records from closed shards
  * Log based bookmarks now use a timestamp to determine if they are still valid

## 1.0.0
  * Releasing v1.0.0 for GA

## 0.4.3
 * Properly paginate over table names [#18](https://github.com/singer-io/tap-dynamodb/pull/18)

## 0.4.2
 * Fixed `ExclusiveStartShardId` not being set correctly [#16](https://github.com/singer-io/tap-dynamodb/pull/16)

## 0.4.1
 * Fixed bug where missing `finished_shard` bookmarks missing caused `has_stream_aged_out()` to throw an exception

## 0.4.0
 * Added bookmark `finished_shards` to track closed shard which we have synced entirely

## 0.3.1
 * Do not throw an error if a log-based record is missing a key in the projection

## 0.3.0
 * Include to version in the record messages

## 0.2.0
 * Add `region_name` to config props and specify when creating client [#5](https://github.com/singer-io/tap-dynamodb/pull/5)

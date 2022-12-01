# Changelog

## 1.2.0
  * Expression attributes implementation [#37](https://github.com/singer-io/tap-dz-dynamodb/pull/37)
  * Implement request timeout [#38](https://github.com/singer-io/tap-dz-dynamodb/pull/38)
  * Missing parent objects from projection expression [#38](https://github.com/singer-io/tap-dz-dynamodb/pull/37)

## 1.1.2
  * Fix issue where decimals can throw a Rounded signal if they are too wide [#33](https://github.com/singer-io/tap-dz-dynamodb/pull/33)

## 1.1.1
  * Add more error checking to ensure a projection provided is also not empty [#26](https://github.com/singer-io/tap-dz-dynamodb/pull/26)

## 1.1.0
  * Rework of log based sync to only sync records from closed shards
  * Log based bookmarks now use a timestamp to determine if they are still valid

## 1.0.0
  * Releasing v1.0.0 for GA

## 0.4.3
 * Properly paginate over table names [#18](https://github.com/singer-io/tap-dz-dynamodb/pull/18)

## 0.4.2
 * Fixed `ExclusiveStartShardId` not being set correctly [#16](https://github.com/singer-io/tap-dz-dynamodb/pull/16)

## 0.4.1
 * Fixed bug where missing `finished_shard` bookmarks missing caused `has_stream_aged_out()` to throw an exception

## 0.4.0
 * Added bookmark `finished_shards` to track closed shard which we have synced entirely

## 0.3.1
 * Do not throw an error if a log-based record is missing a key in the projection

## 0.3.0
 * Include to version in the record messages

## 0.2.0
 * Add `region_name` to config props and specify when creating client [#5](https://github.com/singer-io/tap-dz-dynamodb/pull/5)

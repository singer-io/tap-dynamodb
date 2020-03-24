# Changelog

## 0.4.2
 * Fixed but where `ExclusiveStartShardId` was not being set correctly

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

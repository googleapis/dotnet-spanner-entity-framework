# Changelog

## [3.5.0](https://github.com/googleapis/dotnet-spanner-entity-framework/compare/Google.Cloud.EntityFrameworkCore.Spanner-3.4.1...Google.Cloud.EntityFrameworkCore.Spanner-3.5.0) (2025-07-31)


### Features

* support structural json mapping ([#584](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/584)) ([70065e4](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/70065e426aa0ab45a6e8b929e3915a5bb2e96019)), closes [#581](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/581)


### Bug Fixes

* add ON DELETE CASCADE for foreign keys ([#585](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/585)) ([e112d82](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/e112d82d29602c9badf025f04ae03d5effc94828)), closes [#583](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/583)

## [3.4.1](https://github.com/googleapis/dotnet-spanner-entity-framework/compare/Google.Cloud.EntityFrameworkCore.Spanner-3.4.0...Google.Cloud.EntityFrameworkCore.Spanner-3.4.1) (2025-06-17)


### Bug Fixes

* identity annotations on properties were not generated ([#574](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/574)) ([f7bb1ee](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/f7bb1eea5c65b74cbfbbcb162d03ca6941351f84))

## [3.4.0](https://github.com/googleapis/dotnet-spanner-entity-framework/compare/Google.Cloud.EntityFrameworkCore.Spanner-3.3.0...Google.Cloud.EntityFrameworkCore.Spanner-3.4.0) (2025-06-10)


### Features

* add DateOnly type mapping ([#553](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/553)) ([b0729b6](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/b0729b6f2e54fc92fb9afb80ad95305bb0fd4946))


### Bug Fixes

* identity options in annotations should be serialized ([#551](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/551)) ([a1cd65d](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/a1cd65d2ffa016336b705f7db5687cdc4fa49ad5))

## [3.3.0](https://github.com/googleapis/dotnet-spanner-entity-framework/compare/Google.Cloud.EntityFrameworkCore.Spanner-3.2.0...Google.Cloud.EntityFrameworkCore.Spanner-3.3.0) (2025-04-25)


### Features

* add support for float32 ([#519](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/519)) ([f47d8b8](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/f47d8b8d35073f378fe9f6b380a0d6802effcddf))


### Bug Fixes

* migration default value should be wrapped in parentheses ([#512](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/512)) ([8253cef](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/8253cef7d6f74e310c94e7f415c74e22d5b67151))


### Performance Improvements

* use inline-begin-transaction ([#518](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/518)) ([ff05c9b](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/ff05c9bdb0fab5f45d6fe4d46554228e0908c3d4))

## [3.2.0](https://github.com/googleapis/dotnet-spanner-entity-framework/compare/Google.Cloud.EntityFrameworkCore.Spanner-3.1.1...Google.Cloud.EntityFrameworkCore.Spanner-3.2.0) (2025-03-13)


### Features

* support auto-generated primary keys with IDENTITY columns ([#503](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/503)) ([b3a9570](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/b3a9570810ac6a83ece9f073c0b6c44bd5595c5b))

## [3.1.1](https://github.com/googleapis/dotnet-spanner-entity-framework/compare/Google.Cloud.EntityFrameworkCore.Spanner-3.1.0...Google.Cloud.EntityFrameworkCore.Spanner-3.1.1) (2025-02-26)


### Bug Fixes

* README contained outdated links ([#498](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/498)) ([779b29f](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/779b29f3b364ce75ce8ba1585cdcbd8431eaba01))

## [3.1.0](https://github.com/googleapis/dotnet-spanner-entity-framework/compare/Google.Cloud.EntityFrameworkCore.Spanner-3.0.1...Google.Cloud.EntityFrameworkCore.Spanner-3.1.0) (2025-02-19)


### Features

* add type mapping for int arrays and int lists ([#490](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/490)) ([1ba0ce7](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/1ba0ce790526e43d373f9a5fc86ce80d3d411821))

## [3.0.1](https://github.com/googleapis/dotnet-spanner-entity-framework/compare/Google.Cloud.EntityFrameworkCore.Spanner-3.0.0...Google.Cloud.EntityFrameworkCore.Spanner-3.0.1) (2024-12-01)


### Bug Fixes

* ValueConverters were not respected for complex types ([#475](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/475)) ([0cd4d74](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/0cd4d74ab29d1906b757eda53bb2ca921824c176)), closes [#462](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/462)


### Performance Improvements

* translate Contains to a parameterized IN query ([#472](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/472)) ([85cad1c](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/85cad1cc0f1ca332a10c278c20e4870ea8669872))

## [3.0.0](https://github.com/googleapis/dotnet-spanner-entity-framework/compare/Google.Cloud.EntityFrameworkCore.Spanner-2.2.0...Google.Cloud.EntityFrameworkCore.Spanner-3.0.0) (2024-10-10)


### ⚠ BREAKING CHANGES

* target Entity Framework Core 8 ([#434](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/434))

### Features

* retry DML with THEN RETURN in transactions ([#446](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/446)) ([7679d6f](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/7679d6fd77c017854ce88354b9edd11bbece301e))
* support bit-reversed sequences ([#442](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/442)) ([252e1f8](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/252e1f82ff593f6a666f1be42a336483429010ca))
* support THEN RETURN clauses ([#443](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/443)) ([58cd4e2](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/58cd4e2e246f75a483dad607483b18f737f7b250))
* target Entity Framework Core 8 ([#434](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/434)) ([73bd082](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/73bd0827f508de565d6fb3e1c5f069f71766507a))

## [2.2.0](https://github.com/googleapis/dotnet-spanner-entity-framework/compare/Google.Cloud.EntityFrameworkCore.Spanner-2.1.6...Google.Cloud.EntityFrameworkCore.Spanner-2.2.0) (2024-10-02)


### Features

* support DEFAULT column values ([#439](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/439)) ([4e400df](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/4e400dfd661f10f125ec89f732041df362e987a1))

## [2.1.6](https://github.com/googleapis/dotnet-spanner-entity-framework/compare/Google.Cloud.EntityFrameworkCore.Spanner-2.1.5...Google.Cloud.EntityFrameworkCore.Spanner-2.1.6) (2024-09-12)


### Bug Fixes

* trigger a release ([#436](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/436)) ([4d42348](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/4d42348be469fa88310b88f0da4802fafafce7f1))

## [2.1.5](https://github.com/googleapis/dotnet-spanner-entity-framework/compare/Google.Cloud.EntityFrameworkCore.Spanner-2.1.4...Google.Cloud.EntityFrameworkCore.Spanner-2.1.5) (2024-07-10)


### Bug Fixes

* add missing BeginDbTransactionAsync and CommitAsync async methods ([#409](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/409)) ([493a786](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/493a7864be81bb7356b291ff309be2451544f63c))

## [2.1.4](https://github.com/googleapis/dotnet-spanner-entity-framework/compare/Google.Cloud.EntityFrameworkCore.Spanner-2.1.3...Google.Cloud.EntityFrameworkCore.Spanner-2.1.4) (2024-06-25)


### Bug Fixes

* add missing async methods ([#400](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/400)) ([3f988b9](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/3f988b95aa724647bf2a011f3a7afc01d3f59c40))

## [2.1.3](https://github.com/googleapis/dotnet-spanner-entity-framework/compare/Google.Cloud.EntityFrameworkCore.Spanner-2.1.2...Google.Cloud.EntityFrameworkCore.Spanner-2.1.3) (2023-06-09)


### Bug Fixes

* dispose the underlying SpannerDataReader ([#293](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/293)) ([95526b5](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/95526b5c0cd0de3f23e75dcb9af54c783005e635))

## [2.1.2](https://github.com/googleapis/dotnet-spanner-entity-framework/compare/Google.Cloud.EntityFrameworkCore.Spanner-2.1.1...Google.Cloud.EntityFrameworkCore.Spanner-2.1.2) (2023-04-18)


### Bug Fixes

* EnsureCreated should skip validation before migration ([#287](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/287)) ([9c7c356](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/9c7c35622f7578165585dd8bb238957dd41e8419)), closes [#286](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/286)

## [2.1.1](https://github.com/googleapis/dotnet-spanner-entity-framework/compare/Google.Cloud.EntityFrameworkCore.Spanner-2.1.0...Google.Cloud.EntityFrameworkCore.Spanner-2.1.1) (2023-02-28)


### Bug Fixes

* prevent errors with newlines by using triple-quoted literals ([#283](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/283)) ([a6c5c8e](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/a6c5c8e515ecbb2c1bd76141fe6e983d0b712c76)), closes [#273](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/273)

## [2.1.0](https://github.com/googleapis/dotnet-spanner-entity-framework/compare/Google.Cloud.EntityFrameworkCore.Spanner-2.0.6...Google.Cloud.EntityFrameworkCore.Spanner-2.1.0) (2023-01-18)


### Features

* Support for IEnumerable&lt;T&gt;.Contains (item IN values) with column values ([#270](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/270)) ([16ffa4d](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/16ffa4dd6541b22dda2b84ec910cc517eb668f79))

## [2.0.6](https://github.com/googleapis/dotnet-spanner-entity-framework/compare/Google.Cloud.EntityFrameworkCore.Spanner-2.0.5...Google.Cloud.EntityFrameworkCore.Spanner-2.0.6) (2022-11-29)


### Bug Fixes

* Conflicting dependency for charset-normalizer ([#257](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/257)) ([e4b4e58](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/e4b4e58b01eb59ca3f41c7b6834c0a8f892e6766))

## [2.0.5](https://github.com/googleapis/dotnet-spanner-entity-framework/compare/Google.Cloud.EntityFrameworkCore.Spanner-2.0.4...Google.Cloud.EntityFrameworkCore.Spanner-2.0.5) (2022-11-29)


### Bug Fixes

* Conflicting dependency for Protobuf version ([#255](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/255)) ([03f39ef](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/03f39efc14822bd95c99e5fa4ec7fe96afa2a17f))

## [2.0.4](https://github.com/googleapis/dotnet-spanner-entity-framework/compare/Google.Cloud.EntityFrameworkCore.Spanner-2.0.3...Google.Cloud.EntityFrameworkCore.Spanner-2.0.4) (2022-11-26)


### Bug Fixes

* missing select column TABLE_NAME ([#251](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/251)) ([165b087](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/165b0875d42dfd27100150bef03d915ad9df5cad))

## [2.0.3](https://github.com/googleapis/dotnet-spanner-entity-framework/compare/Google.Cloud.EntityFrameworkCore.Spanner-2.0.2...Google.Cloud.EntityFrameworkCore.Spanner-2.0.3) (2022-10-11)


### Bug Fixes

* migrations failure in version 2.0.0 ([#215](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/215)) ([b820e46](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/b820e462d18829543a06306c41e53a17cc48565d)), closes [#212](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/212)

## [2.0.2](https://github.com/googleapis/dotnet-spanner-entity-framework/compare/Google.Cloud.EntityFrameworkCore.Spanner-2.0.1...Google.Cloud.EntityFrameworkCore.Spanner-2.0.2) (2022-09-29)


### Bug Fixes

* regenerate requirements.txt ([#199](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/199)) ([0344f01](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/0344f01fcbe842c36d0ff128625f106cc452c270))

## [2.0.1](https://github.com/googleapis/dotnet-spanner-entity-framework/compare/Google.Cloud.EntityFrameworkCore.Spanner-2.0.0...Google.Cloud.EntityFrameworkCore.Spanner-2.0.1) (2022-09-29)


### Bug Fixes

* update requirements hash ([#197](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/197)) ([8fad68b](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/8fad68bd745a687d2ab7d3ea8b9d0cbc55d7715a))

## [2.0.0](https://github.com/googleapis/dotnet-spanner-entity-framework/compare/Google.Cloud.EntityFrameworkCore.Spanner-1.1.0...Google.Cloud.EntityFrameworkCore.Spanner-2.0.0) (2022-09-28)


### ⚠ BREAKING CHANGES

* migrate to Entity Framework Core 6.0 (#152)

### Features

* migrate to Entity Framework Core 6.0 ([#152](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/152)) ([a6e27aa](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/a6e27aa25c9deeab7022284bb1a21e79976a54de))

## Version 1.1.0, released 2022-01-04

- [Commit fdc22c1](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/fdc22c1) fix: DataReader.GetFieldValue<T> could fail for array types ([#144](https://github.com/googleapis/dotnet-spanner-entity-framework/pull/144))
- [Commit 031d842](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/031d842) chore: cast COLUMN\_DEFAULT value to STRING ([#145](https://github.com/googleapis/dotnet-spanner-entity-framework/pull/145))
- [Commit 0f90c7f](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/0f90c7f) feat: add helper method for getting SpannerConnection + PDML sample ([#133](https://github.com/googleapis/dotnet-spanner-entity-framework/pull/133))
- [Commit ce757fb](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/ce757fb) samples: add a quick start sample ([#128](https://github.com/googleapis/dotnet-spanner-entity-framework/pull/128))

## Version 1.0.0, released 2021-09-09

- [Commit baee99b](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/baee99b) feat: support single stale reads ([#120](https://github.com/googleapis/dotnet-spanner-entity-framework/pull/120))
- [Commit 429486c](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/429486c) fix: use build instead of revision in version ([#118](https://github.com/googleapis/dotnet-spanner-entity-framework/pull/118))
- [Commit 7bfb5e4](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/7bfb5e4) test: use assembly version instead of hard coded string ([#114](https://github.com/googleapis/dotnet-spanner-entity-framework/pull/114))
- [Commit 0c4b76a](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/0c4b76a) test: use unicode for random strings ([#115](https://github.com/googleapis/dotnet-spanner-entity-framework/pull/115))
- [Commit 1869689](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/1869689) test: rollback should not clear the \_abortNextStatement flag ([#110](https://github.com/googleapis/dotnet-spanner-entity-framework/pull/110))

## Version 0.2.0, released 2021-09-03

- [Commit 6664d8d](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/6664d8d) test: add test to verify the use of DDL batches ([#92](https://github.com/googleapis/dotnet-spanner-entity-framework/pull/92))
- [Commit 68256c9](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/68256c9) chore(deps): update dependency xunit to v2.4.1 ([#97](https://github.com/googleapis/dotnet-spanner-entity-framework/pull/97))
- [Commit e5d6018](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/e5d6018) chore: add renovate.json ([#85](https://github.com/googleapis/dotnet-spanner-entity-framework/pull/85))
- [Commit 85dd134](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/85dd134) build: split build actions and skip integration tests on prod from forks ([#93](https://github.com/googleapis/dotnet-spanner-entity-framework/pull/93))
- [Commit 5b89350](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/5b89350) chore: add history.md ([#94](https://github.com/googleapis/dotnet-spanner-entity-framework/pull/94))
- [Commit 7a35e22](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/7a35e22) chore: add autorelease scripts ([#90](https://github.com/googleapis/dotnet-spanner-entity-framework/pull/90))
- [Commit 1d01f5a](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/1d01f5a) feat: support JSON data type ([#91](https://github.com/googleapis/dotnet-spanner-entity-framework/pull/91))
- [Commit 4e510a7](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/4e510a7) monitoring: add custom client header for efcore ([#89](https://github.com/googleapis/dotnet-spanner-entity-framework/pull/89))
- [Commit 346115b](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/346115b) chore: add CONTRIBUTING.md ([#88](https://github.com/googleapis/dotnet-spanner-entity-framework/pull/88))
- [Commit fa4aafe](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/fa4aafe) chore: add SECURITY.md ([#87](https://github.com/googleapis/dotnet-spanner-entity-framework/pull/87))
- [Commit 7644fe5](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/7644fe5) chore: add a Code of Conduct ([#86](https://github.com/googleapis/dotnet-spanner-entity-framework/pull/86))
- [Commit 4c43965](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/4c43965) docs: add sample for using query hints ([#84](https://github.com/googleapis/dotnet-spanner-entity-framework/pull/84))
- [Commit c520460](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/c520460) chore: add missing license headers to files ([#82](https://github.com/googleapis/dotnet-spanner-entity-framework/pull/82))
- [Commit b846eb3](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/b846eb3) feat: propagate commit timestamp to current DbContext ([#79](https://github.com/googleapis/dotnet-spanner-entity-framework/pull/79))
- [Commit 730e4bf](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/730e4bf) test: enable test assertion for emulator ([#78](https://github.com/googleapis/dotnet-spanner-entity-framework/pull/78))
- [Commit 122db74](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/122db74) docs: instructions on how to build and publish NuGet package locally. ([#77](https://github.com/googleapis/dotnet-spanner-entity-framework/pull/77))
- [Commit c0d13dc](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/c0d13dc) feat: validate entity model against database ([#76](https://github.com/googleapis/dotnet-spanner-entity-framework/pull/76))

## Version 0.1.0, released 2021-04-08

Initial release.

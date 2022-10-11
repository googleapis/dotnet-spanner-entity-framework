# Changelog

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

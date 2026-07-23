# Changelog

## [1.2.0](https://github.com/googleapis/dotnet-spanner-entity-framework/compare/Google.Cloud.Spanner.DataProvider-1.1.0...Google.Cloud.Spanner.DataProvider-1.2.0) (2026-07-23)


### Features

* **ado.net:** support more schema tables ([#778](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/778)) ([56f5b2f](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/56f5b2f8aab28028c5a4ff4e1fef2e7390dd7384))


### Bug Fixes

* **ado.net:** NaN and Inf values were not returned correctly ([#773](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/773)) ([f1f5d90](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/f1f5d90dd90f23ea67884c4b06b3abf22bc549d5))
* **ado.net:** prevent swallowing of non-Spanner exceptions in async paths ([#769](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/769)) ([61552b3](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/61552b3909af061d1b9b016821742632eeaeb3d7))
* **ado.net:** translate SpannerException to SpannerDbException ([#772](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/772)) ([8a4f690](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/8a4f690a7e5240389339631306128535a2a1da8d))
* **spanner-ado-net:** prevent sync calls on async execution paths for DML stats ([#776](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/776)) ([263c789](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/263c7894c3def66de301d7a4354bc6bf0391d7a1))


### Performance Improvements

* **ado.net:** fetch multiple rows at a time ([#779](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/779)) ([cbdc157](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/cbdc1573d74f0dadf861fb429c1440e2ab2b1bcb))

## [1.1.0](https://github.com/googleapis/dotnet-spanner-entity-framework/compare/Google.Cloud.Spanner.DataProvider-1.0.0...Google.Cloud.Spanner.DataProvider-1.1.0) (2026-03-23)


### Features

* allow string to be mapped to JSON columns ([#598](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/598)) ([a0c1414](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/a0c14146e9fe2cef1bd44d7fd1ad20861efd5383))


### Bug Fixes

* **ado.net:** trigger release of ADO.NET ([#720](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/720)) ([bec40ec](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/bec40ec08dfff9c49b5d237cff79158f89b967f4))
* document failed release job and retrigger release ([#701](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/701)) ([ee7f72b](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/ee7f72be66c554184f98c085f3093d8faa67d084))
* re-trigger ADO.NET release ([#705](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/705)) ([91e223f](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/91e223f7e50f08ce3e7c41a5243d477c9e8bb5f5))
* remove 'Alpha' prefix from package name ([#707](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/707)) ([0f02447](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/0f02447c539e08ff63303ff0f4ae412a940a078a))
* trigger a release ([#436](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/436)) ([4d42348](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/4d42348be469fa88310b88f0da4802fafafce7f1))

## [0.2.3](https://github.com/googleapis/dotnet-spanner-entity-framework/compare/Google.Cloud.Spanner.DataProvider-0.2.2...Google.Cloud.Spanner.DataProvider-0.2.3) (2026-02-25)


### Bug Fixes

* remove 'Alpha' prefix from package name ([#707](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/707)) ([0f02447](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/0f02447c539e08ff63303ff0f4ae412a940a078a))

## [0.2.2](https://github.com/googleapis/dotnet-spanner-entity-framework/compare/Google.Cloud.Spanner.DataProvider-0.2.1...Google.Cloud.Spanner.DataProvider-0.2.2) (2026-02-24)


### Bug Fixes

* re-trigger ADO.NET release ([#705](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/705)) ([91e223f](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/91e223f7e50f08ce3e7c41a5243d477c9e8bb5f5))

## [0.2.1](https://github.com/googleapis/dotnet-spanner-entity-framework/compare/Google.Cloud.Spanner.DataProvider-0.2.0...Google.Cloud.Spanner.DataProvider-0.2.1) (2026-02-20)

Re-trigger release.

### Bug Fixes

* document failed release job and retrigger release ([#701](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/701)) ([ee7f72b](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/ee7f72be66c554184f98c085f3093d8faa67d084))

## [0.2.0](https://github.com/googleapis/dotnet-spanner-entity-framework/compare/Google.Cloud.Spanner.DataProvider-0.1.0...Google.Cloud.Spanner.DataProvider-0.2.0) (2026-02-18)

Release job failed.

### Features

* allow string to be mapped to JSON columns ([#598](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/598)) ([a0c1414](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/a0c14146e9fe2cef1bd44d7fd1ad20861efd5383))


### Bug Fixes

* trigger a release ([#436](https://github.com/googleapis/dotnet-spanner-entity-framework/issues/436)) ([4d42348](https://github.com/googleapis/dotnet-spanner-entity-framework/commit/4d42348be469fa88310b88f0da4802fafafce7f1))

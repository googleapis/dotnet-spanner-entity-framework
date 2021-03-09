``` ini

BenchmarkDotNet=v0.12.1, OS=Windows 10.0.18363.1379 (1909/November2018Update/19H2)
Intel Core i7-8705G CPU 3.10GHz (Kaby Lake G), 1 CPU, 8 logical and 4 physical cores
.NET Core SDK=3.1.403
  [Host]     : .NET Core 3.1.9 (CoreCLR 4.700.20.47201, CoreFX 4.700.20.47203), X64 RyuJIT DEBUG
  DefaultJob : .NET Core 3.1.9 (CoreCLR 4.700.20.47201, CoreFX 4.700.20.47203), X64 RyuJIT


```
|                                             Method |      Mean |     Error |    StdDev |
|--------------------------------------------------- |----------:|----------:|----------:|
|                                  ReadOneRowSpanner |  4.208 ms | 0.2471 ms | 0.7170 ms |
|                                       ReadOneRowEF |  5.918 ms | 0.3596 ms | 1.0261 ms |
|                    SaveOneRowWithFetchAfterSpanner |  9.221 ms | 0.4109 ms | 1.1921 ms |
|                         SaveOneRowWithFetchAfterEF | 10.259 ms | 0.3708 ms | 1.0759 ms |
|                            SaveMultipleRowsSpanner |  3.582 ms | 0.2123 ms | 0.6091 ms |
|                                 SaveMultipleRowsEF |  4.710 ms | 0.2471 ms | 0.7011 ms |
|                       SelectMultipleSingersSpanner | 15.756 ms | 0.4517 ms | 1.3177 ms |
|                            SelectMultipleSingersEF | 14.818 ms | 0.4098 ms | 1.1559 ms |
|  SelectMultipleSingersInReadOnlyTransactionSpanner | 15.778 ms | 0.3648 ms | 1.0525 ms |
|       SelectMultipleSingersInReadOnlyTransactionEF | 16.181 ms | 0.4693 ms | 1.3314 ms |
| SelectMultipleSingersInReadWriteTransactionSpanner | 15.374 ms | 0.3861 ms | 1.0891 ms |
|      SelectMultipleSingersInReadWriteTransactionEF | 18.413 ms | 0.5058 ms | 1.4349 ms |

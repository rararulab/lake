# Changelog

## [1.3.1](https://github.com/rararulab/lake/compare/v1.3.0...v1.3.1) (2026-07-19)


### Bug Fixes

* **release:** align Iceberg lockfile version ([#218](https://github.com/rararulab/lake/issues/218)) ([1b822ce](https://github.com/rararulab/lake/commit/1b822ced15988e6c7a8343f040101e0bf9f7be9e))
* **release:** align Iceberg lockfile version ([#218](https://github.com/rararulab/lake/issues/218)) ([73933ba](https://github.com/rararulab/lake/commit/73933ba157a8128dda9da170e9ccacdc8e02f742))

## [1.3.0](https://github.com/rararulab/lake/compare/v1.2.0...v1.3.0) (2026-07-19)


### Features

* **iceberg:** support durable asynchronous SQL ([#212](https://github.com/rararulab/lake/issues/212)) ([#216](https://github.com/rararulab/lake/issues/216)) ([e506635](https://github.com/rararulab/lake/commit/e506635a6896b7a7e2163941f6c622d96219dd00))


### Bug Fixes

* **release:** align Iceberg lockfile version ([#213](https://github.com/rararulab/lake/issues/213)) ([#214](https://github.com/rararulab/lake/issues/214)) ([5e01f21](https://github.com/rararulab/lake/commit/5e01f219a5637b10aafeac89ba3408c941f1b2e9))

## [1.2.0](https://github.com/rararulab/lake/compare/v1.1.0...v1.2.0) (2026-07-19)


### Features

* **iceberg:** authenticate external REST catalog sessions ([#199](https://github.com/rararulab/lake/issues/199)) ([#200](https://github.com/rararulab/lake/issues/200)) ([64443f7](https://github.com/rararulab/lake/commit/64443f72b740ec8b5f1120b1b5f2b211459621f0))
* **iceberg:** bound REST catalog request latency ([#204](https://github.com/rararulab/lake/issues/204)) ([#205](https://github.com/rararulab/lake/issues/205)) ([22ffaba](https://github.com/rararulab/lake/commit/22ffabaf2f790709969f533637be89be1071f043))
* **iceberg:** renew expired REST OAuth sessions ([#202](https://github.com/rararulab/lake/issues/202)) ([#203](https://github.com/rararulab/lake/issues/203)) ([082ad4c](https://github.com/rararulab/lake/commit/082ad4c13707aefd0dae0cc7bdcc54e7d1f1c3eb))


### Bug Fixes

* **iceberg:** require TLS for external REST credentials ([#206](https://github.com/rararulab/lake/issues/206)) ([#207](https://github.com/rararulab/lake/issues/207)) ([8077407](https://github.com/rararulab/lake/commit/80774076bb5c2db320a1decf3c9fcb7744c325a4))
* **iceberg:** single-flight concurrent snapshot refreshes ([#208](https://github.com/rararulab/lake/issues/208)) ([#209](https://github.com/rararulab/lake/issues/209)) ([2307678](https://github.com/rararulab/lake/commit/2307678ea2b793a9414f17a240c11c628ac62b2d))

## [1.1.0](https://github.com/rararulab/lake/compare/v1.0.0...v1.1.0) (2026-07-18)


### Features

* **iceberg:** federate REST catalog tables ([#188](https://github.com/rararulab/lake/issues/188)) ([#198](https://github.com/rararulab/lake/issues/198)) ([0519774](https://github.com/rararulab/lake/commit/05197742c20e30d6d22aeed90102edf64a6d0588))


### Bug Fixes

* **ci:** restore clippy green on main ([#196](https://github.com/rararulab/lake/issues/196)) ([#197](https://github.com/rararulab/lake/issues/197)) ([8f6bf24](https://github.com/rararulab/lake/commit/8f6bf24b5b3ef47ad0f721930216fb644ecfbd13))
* **ci:** serialize direct mise bootstrap jobs ([#194](https://github.com/rararulab/lake/issues/194)) ([#195](https://github.com/rararulab/lake/issues/195)) ([9865ec5](https://github.com/rararulab/lake/commit/9865ec50cc61ed5d3e07c2d8688305577ed540a9))
* **ci:** serialize mise tool bootstrap ([#191](https://github.com/rararulab/lake/issues/191)) ([#193](https://github.com/rararulab/lake/issues/193)) ([1d40c60](https://github.com/rararulab/lake/commit/1d40c60af55d7f1a23876e7cab6fb5a0cc5d7436))
* **release:** publish multi-arch image for each release tag ([#184](https://github.com/rararulab/lake/issues/184)) ([#189](https://github.com/rararulab/lake/issues/189)) ([0d655a4](https://github.com/rararulab/lake/commit/0d655a4e156f65d3b9d291c00b3a1201d5d8c7e4))

## [1.0.0](https://github.com/rararulab/lake/compare/v0.0.1...v1.0.0) (2026-07-17)


### ⚠ BREAKING CHANGES

* **sdk:** consume all local Flight result endpoints ([#132](https://github.com/rararulab/lake/issues/132)) (#133)

### Features

* **catalog:** cache real schemas for Flight SQL discovery ([#27](https://github.com/rararulab/lake/issues/27)) ([a9878a1](https://github.com/rararulab/lake/commit/a9878a1a1cb9b601529ed5d96a6466264c9334b6))
* **catalog:** persist table schema IPC ([#27](https://github.com/rararulab/lake/issues/27)) ([43fc370](https://github.com/rararulab/lake/commit/43fc3703ecf9aa866b04bc6905a4e56826446096))
* **cli:** add fail-closed object GC worker ([#23](https://github.com/rararulab/lake/issues/23)) ([2234a9c](https://github.com/rararulab/lake/commit/2234a9c8c8596a9feb4e92fca9c7ccfe52941b68))
* **cli:** initialize structured server logging ([#64](https://github.com/rararulab/lake/issues/64)) ([06fc695](https://github.com/rararulab/lake/commit/06fc69502b0c47d89abaabe12fcd0d85e5b26974)), closes [#63](https://github.com/rararulab/lake/issues/63)
* **cli:** load protected tenant principal maps ([#25](https://github.com/rararulab/lake/issues/25)) ([4cd4632](https://github.com/rararulab/lake/commit/4cd46323303b138dae689727db17cd01c09bf781))
* **common:** add object reference delta domain ([#23](https://github.com/rararulab/lake/issues/23)) ([549a398](https://github.com/rararulab/lake/commit/549a398f64a7b3e85b15b43eeea26fae02149cc2))
* **deploy:** add hardened Kubernetes reference ([fd323e5](https://github.com/rararulab/lake/commit/fd323e557f5820b20a28a66cc48dff5efadd0f38))
* **deploy:** add hardened Kubernetes reference ([#71](https://github.com/rararulab/lake/issues/71)) ([1426020](https://github.com/rararulab/lake/commit/142602049aa47ead6b07a73f30f258d2dc416058))
* **engine:** configure Lance version retention ([3a3788e](https://github.com/rararulab/lake/commit/3a3788e83e513b6ea5b91bb06701494f180e3875))
* **engine:** configure Lance version retention ([#85](https://github.com/rararulab/lake/issues/85)) ([68e6e97](https://github.com/rararulab/lake/commit/68e6e97ad96bde4f8545b7c56a30246f9b3a5c7d))
* **engine:** expose retained object reference lineage ([#23](https://github.com/rararulab/lake/issues/23)) ([dfdce81](https://github.com/rararulab/lake/commit/dfdce8157c2b33a81f26c19b27ac80e8f6dcb5ca))
* **lance:** journal append object references ([#23](https://github.com/rararulab/lake/issues/23)) ([0657cf3](https://github.com/rararulab/lake/commit/0657cf35b3940ed62b658ddb24d965c75ac86767))
* **lance:** page retained object reference lineage ([#23](https://github.com/rararulab/lake/issues/23)) ([65f871e](https://github.com/rararulab/lake/commit/65f871e261251c14069355af5e56c6a8d6b36ea1))
* **manifest:** add O(1) latest pointer ([#41](https://github.com/rararulab/lake/issues/41)) ([d785ba9](https://github.com/rararulab/lake/commit/d785ba91e339edec827e13c733b6a0983b13ba97))
* **manifest:** add O(1) latest pointer ([#41](https://github.com/rararulab/lake/issues/41)) ([83d443a](https://github.com/rararulab/lake/commit/83d443a69c1b9a05feae78e10d8eb8d78a722f48))
* **manifest:** reclaim removed history ([#42](https://github.com/rararulab/lake/issues/42)) ([0e9c450](https://github.com/rararulab/lake/commit/0e9c4506502265095909e93a819694dcd7492ff0))
* **manifest:** reclaim removed history ([#42](https://github.com/rararulab/lake/issues/42)) ([06ceb09](https://github.com/rararulab/lake/commit/06ceb09abfdd1fc5b4d75c6dc599fc982f4a4754))
* **meta:** add atomic lease-fenced mutations ([aa4fe77](https://github.com/rararulab/lake/commit/aa4fe7787dbbc9eccd659618eeb5bbe730a802fd))
* **meta:** add atomic lease-fenced mutations ([#33](https://github.com/rararulab/lake/issues/33)) ([2edef30](https://github.com/rararulab/lake/commit/2edef30218c26c07cfa8bb18eed57638db802320))
* **meta:** add prefix entry scans ([#27](https://github.com/rararulab/lake/issues/27)) ([1ee8915](https://github.com/rararulab/lake/commit/1ee891544b5d8903a9aad225d79b5c9a9d2755d8))
* **meta:** expose Dynamo prefix metrics ([#96](https://github.com/rararulab/lake/issues/96)) ([68b5e8d](https://github.com/rararulab/lake/commit/68b5e8d1bdc82c714d07ef458ef7566a6f201f11))
* **meta:** expose Dynamo prefix-layout operational metrics ([6eb718f](https://github.com/rararulab/lake/commit/6eb718f116f0b40d09c6890dac23c6550537a7a2))
* **meta:** page table registrations ([#59](https://github.com/rararulab/lake/issues/59)) ([4865211](https://github.com/rararulab/lake/commit/48652110b51d0301415e6a9f785800cddb38ad23))
* **metasrv:** add durable drop tombstones ([6f85c06](https://github.com/rararulab/lake/commit/6f85c06efa19b1e28fc3bd0b7835a6fe5108e814))
* **metasrv:** add durable drop tombstones ([#37](https://github.com/rararulab/lake/issues/37)) ([5096bd0](https://github.com/rararulab/lake/commit/5096bd0d3144852bb117532281d45fff6533f92d))
* **metasrv:** configure append admission limits ([#53](https://github.com/rararulab/lake/issues/53)) ([8a95341](https://github.com/rararulab/lake/commit/8a9534191b4983b2216cc469372c6f63bfa5cde5))
* **metasrv:** configure paged table maintenance ([#59](https://github.com/rararulab/lake/issues/59)) ([a42bd7b](https://github.com/rararulab/lake/commit/a42bd7b71159404625de7cb410f03b732b0a96ef))
* **metasrv:** enforce delegated tenant authorization ([#25](https://github.com/rararulab/lake/issues/25)) ([a3a3859](https://github.com/rararulab/lake/commit/a3a3859eeaa274cb7710331ee639d4d28b8a5a06))
* **metasrv:** fence metadata publications ([d1b936d](https://github.com/rararulab/lake/commit/d1b936daef1e59443ecbdb55da022ebc8d33b87f))
* **metasrv:** fence metadata publications ([#35](https://github.com/rararulab/lake/issues/35)) ([89c1a4e](https://github.com/rararulab/lake/commit/89c1a4e949bf780d0587b2dfd0c0d17aed743790))
* **metasrv:** reserve append concurrency and memory ([#53](https://github.com/rararulab/lake/issues/53)) ([ea4335d](https://github.com/rararulab/lake/commit/ea4335de99ee6cbec1092da608c22da18446f033))
* **objects:** add incremental reference journal and safe orphan GC ([8b2b616](https://github.com/rararulab/lake/commit/8b2b6167816f08d082dee4662348e318f9613907))
* **objects:** add managed FILE range reads ([#10](https://github.com/rararulab/lake/issues/10)) ([179a378](https://github.com/rararulab/lake/commit/179a37826e0c4c62ebe341ac42988bfcd3f07e50))
* **objects:** add S3 managed stage for SQL FILE ([#8](https://github.com/rararulab/lake/issues/8)) ([432eaf0](https://github.com/rararulab/lake/commit/432eaf04a8cb85b82a52d29dfac12367dddaefe6))
* **objects:** apply GC plans resumably ([#23](https://github.com/rararulab/lake/issues/23)) ([5516ea0](https://github.com/rararulab/lake/commit/5516ea04240dcbcee2f40c8362da8739dc1b7d30))
* **objects:** build bounded live-reference indexes ([#23](https://github.com/rararulab/lake/issues/23)) ([2da40d2](https://github.com/rararulab/lake/commit/2da40d2838bdac13b1d35be6056109853fba1a9d))
* **objects:** manage large objects through SQL ([#4](https://github.com/rararulab/lake/issues/4)) ([4b72147](https://github.com/rararulab/lake/commit/4b72147ab2d1b9d86e76254443eb809dc2c7d51b))
* **objects:** page managed object inventories ([#23](https://github.com/rararulab/lake/issues/23)) ([fccfb9d](https://github.com/rararulab/lake/commit/fccfb9d2bc9edd58aca1d057ac2d31bdf44d1fb9))
* **objects:** publish immutable GC plans ([#23](https://github.com/rararulab/lake/issues/23)) ([e90c3d5](https://github.com/rararulab/lake/commit/e90c3d5d07e773aa39641216c90e944204bfc751))
* **objects:** resume multi-gigabyte S3 uploads across restarts ([#22](https://github.com/rararulab/lake/issues/22)) ([3f77b11](https://github.com/rararulab/lake/commit/3f77b11127e1dd0e341b5a066b3ede7cf94d5e50))
* **objects:** scope managed stages by tenant ([#25](https://github.com/rararulab/lake/issues/25)) ([0e8bdc1](https://github.com/rararulab/lake/commit/0e8bdc1de6fc3c71cfc24bb23e70abc0781cb0e7))
* **objects:** stream fail-closed orphan GC plans ([#23](https://github.com/rararulab/lake/issues/23)) ([4dc36c6](https://github.com/rararulab/lake/commit/4dc36c64309a54f68737ddc490d2447a2b2e2187))
* **observability:** add bounded OTLP distributed tracing ([#74](https://github.com/rararulab/lake/issues/74)) ([5222ba1](https://github.com/rararulab/lake/commit/5222ba14bceb7e66c3a748efd646389f4e89396f)), closes [#73](https://github.com/rararulab/lake/issues/73)
* **observability:** expose bounded Prometheus metrics ([1d89475](https://github.com/rararulab/lake/commit/1d89475448f96860c1d6bb35e5af063604b7b8f4))
* **observability:** expose bounded Prometheus metrics ([#69](https://github.com/rararulab/lake/issues/69)) ([e7cc17b](https://github.com/rararulab/lake/commit/e7cc17bebd9b67cc0ddafae9fcb056803bf28998))
* **query:** add bounded admission control and deadlines ([#18](https://github.com/rararulab/lake/issues/18)) ([6d44e7e](https://github.com/rararulab/lake/commit/6d44e7e512fc5b253277894ddc5c0ab3e12dd097))
* **query:** add bounded tenant admission ([b5d1a17](https://github.com/rararulab/lake/commit/b5d1a17d8e2b0d9964fc1fde1ada82491e2bc8f7))
* **query:** add bounded tenant admission ([#116](https://github.com/rararulab/lake/issues/116)) ([d45dca9](https://github.com/rararulab/lake/commit/d45dca96bb98fdd1f557eec2be7fd8c22dd41b0f))
* **query:** add durable async query results ([2c8ee15](https://github.com/rararulab/lake/commit/2c8ee1527a9046a747536a89590ceb5ff1f99b7d))
* **query:** add durable async query results ([#110](https://github.com/rararulab/lake/issues/110)) ([6c50316](https://github.com/rararulab/lake/commit/6c50316425cc264a32cb5cbea45a7eefe237e2e7))
* **query:** add durable async tenant resource quotas ([#126](https://github.com/rararulab/lake/issues/126)) ([9901460](https://github.com/rararulab/lake/commit/990146066bbcca06640489cd995569887ec7636c))
* **query:** add durable async tenant resource quotas ([#126](https://github.com/rararulab/lake/issues/126)) ([c1ad4ca](https://github.com/rararulab/lake/commit/c1ad4ca51669b7284473cd0371744736e631f7c2))
* **query:** bound DataFusion memory and spill ([2f0e952](https://github.com/rararulab/lake/commit/2f0e95204d26609578d167816b057a0b0e714c80))
* **query:** bound DataFusion memory and spill ([#39](https://github.com/rararulab/lake/issues/39)) ([3979d0c](https://github.com/rararulab/lake/commit/3979d0c7325527334d2a5c0bc673da99f6b29cb7))
* **query:** configure discovery bounds ([#51](https://github.com/rararulab/lake/issues/51)) ([d8ed0ef](https://github.com/rararulab/lake/commit/d8ed0ef745277a84868028751456291e50e8d5ab))
* **query:** deny cross-tenant SQL before planning ([#25](https://github.com/rararulab/lake/issues/25)) ([de1eebc](https://github.com/rararulab/lake/commit/de1eebc3fe76ccc48849966c50b1d3343936ed5f))
* **query:** filter Flight SQL discovery by tenant ([#25](https://github.com/rararulab/lake/issues/25)) ([f0a54c4](https://github.com/rararulab/lake/commit/f0a54c46e6c846ef87c4238fc7bc134dab64c0a6))
* **query:** issue encrypted tenant-bound tickets ([b22a021](https://github.com/rararulab/lake/commit/b22a02176e1c0df3d92726038bd579ad2ebd064c))
* **query:** issue encrypted tenant-bound tickets ([#106](https://github.com/rararulab/lake/issues/106)) ([da6b23d](https://github.com/rararulab/lake/commit/da6b23d12070b86aab71e1c715a4bf423ea989a0))
* **query:** make async scheduling tenant-fair ([3d1ff33](https://github.com/rararulab/lake/commit/3d1ff33088cc797e1ed57ce0e0d2a6aa98e00b25))
* **query:** make async scheduling tenant-fair ([#118](https://github.com/rararulab/lake/issues/118)) ([ece83fc](https://github.com/rararulab/lake/commit/ece83fc41f49b952c6afd502ca7d2ec8e5727382))
* **query:** pin statement table snapshots ([ecb3e5c](https://github.com/rararulab/lake/commit/ecb3e5cf33ab8d53abdcc8920379619aa198f900))
* **query:** pin statement table snapshots ([#108](https://github.com/rararulab/lake/issues/108)) ([b5fbf18](https://github.com/rararulab/lake/commit/b5fbf1851271c5200f46f34aca0412ca2fabb973))
* **query:** return cached schemas in Flight discovery ([#27](https://github.com/rararulab/lake/issues/27)) ([5dde9f5](https://github.com/rararulab/lake/commit/5dde9f56cce01439f576cca12d6ca7bd8ff3fa26))
* **query:** stream direct SQL results ([#128](https://github.com/rararulab/lake/issues/128)) ([#129](https://github.com/rararulab/lake/issues/129)) ([4962fa8](https://github.com/rararulab/lake/commit/4962fa893071b01e72c8640ea9b9535fa553811b))
* **runtime:** add graceful Flight shutdown and lease handoff ([#20](https://github.com/rararulab/lake/issues/20)) ([4950938](https://github.com/rararulab/lake/commit/49509381ff5e8cac8def09e11d2506e0e7660f42))
* **sdk:** add bounded incarnation-safe schema caching ([85e4e49](https://github.com/rararulab/lake/commit/85e4e492c0e7a074f70fc80a0c2fd4052b4023a9))
* **sdk:** add bounded multi-row typed inserts ([51aebaa](https://github.com/rararulab/lake/commit/51aebaa47a07bbe4002271f5f2fe41c24513ce0a))
* **sdk:** add bounded multi-row typed inserts ([#77](https://github.com/rararulab/lake/issues/77)) ([713086a](https://github.com/rararulab/lake/commit/713086a2152c63c666f39991a3b50d66306e86b0))
* **sdk:** add bounded presigned managed reads ([c5b38c8](https://github.com/rararulab/lake/commit/c5b38c8ca6b608aa22f1ed076a0977abbb14b2c7))
* **sdk:** add bounded presigned managed reads ([#79](https://github.com/rararulab/lake/issues/79)) ([d07c564](https://github.com/rararulab/lake/commit/d07c5644d10b6bd75b0cfa1b7a207435dab32003))
* **sdk:** add bounded singleflight schema cache ([#75](https://github.com/rararulab/lake/issues/75)) ([1be196a](https://github.com/rararulab/lake/commit/1be196abc94e509124fb088abe9124b8ca0a4e1a))
* **sdk:** add credentialless managed read capabilities ([#141](https://github.com/rararulab/lake/issues/141)) ([c75b3b9](https://github.com/rararulab/lake/commit/c75b3b9217c297bf546fbfac19756587cf8f4135))
* **sdk:** add restart-safe async query handles ([e32ff22](https://github.com/rararulab/lake/commit/e32ff224723ba76b2a37a1073131923ad8da7cc5))
* **sdk:** add restart-safe async query handles ([#112](https://github.com/rararulab/lake/issues/112)) ([929a571](https://github.com/rararulab/lake/commit/929a5711442eaffe487a721e79e0a3fa8da6429c))
* **sdk:** discover managed FILE stage from query endpoint ([#13](https://github.com/rararulab/lake/issues/13)) ([f968e94](https://github.com/rararulab/lake/commit/f968e9401ea43288d4d7942589cd925002fcbdd1))
* **sdk:** persist ambiguous append recovery ([#90](https://github.com/rararulab/lake/issues/90)) ([c2d8a23](https://github.com/rararulab/lake/commit/c2d8a236c9c7d9a87ff35e13ae66115252224a7e))
* **sdk:** persist ambiguous append recovery ([#90](https://github.com/rararulab/lake/issues/90)) ([c1e97bb](https://github.com/rararulab/lake/commit/c1e97bb6a71c855b6364e0d4c4192609cb3e05db))
* **sdk:** stream credentialless managed FILE reads ([#142](https://github.com/rararulab/lake/issues/142)) ([#143](https://github.com/rararulab/lake/issues/143)) ([8343b27](https://github.com/rararulab/lake/commit/8343b270d5c7c886c2c75ac6a9dd983024b90bc7))
* **sdk:** verify managed object reads at EOF ([5b82147](https://github.com/rararulab/lake/commit/5b82147c318b2ba7b0d62442547ea84835f1512d))
* **sdk:** verify managed object reads at EOF ([#83](https://github.com/rararulab/lake/issues/83)) ([8cdb677](https://github.com/rararulab/lake/commit/8cdb67734eec6fa491fb7418041b042823801122))
* **security:** enforce tenant catalog and object boundaries ([#25](https://github.com/rararulab/lake/issues/25)) ([89ad778](https://github.com/rararulab/lake/commit/89ad778af989d1528be94550e0cfd4ff24889a41))
* **security:** install explicit development principal ([#25](https://github.com/rararulab/lake/issues/25)) ([a189d3f](https://github.com/rararulab/lake/commit/a189d3f7a80c30c51d7eadb44effd3063d9b53d4))
* **security:** model tenant-scoped principals ([#25](https://github.com/rararulab/lake/issues/25)) ([1c9a499](https://github.com/rararulab/lake/commit/1c9a4990753dd9e5d161bcc85706ecbf364f9150))
* **security:** secure Flight transport and RPC identity ([#16](https://github.com/rararulab/lake/issues/16)) ([5157361](https://github.com/rararulab/lake/commit/5157361816b924555e83baaf3249de1cb9a67134))
* **servers:** expose authenticated gRPC health readiness ([#66](https://github.com/rararulab/lake/issues/66)) ([e0c2d92](https://github.com/rararulab/lake/commit/e0c2d92058ab8e97a44b2525439ba33a62819a83))


### Bug Fixes

* **catalog:** fence stale registration cache fills ([#45](https://github.com/rararulab/lake/issues/45)) ([3c4d932](https://github.com/rararulab/lake/commit/3c4d932807133574f290e55d69d371ceadbbff23))
* **ci:** cover Cargo configuration in path filters ([#165](https://github.com/rararulab/lake/issues/165)) ([#167](https://github.com/rararulab/lake/issues/167)) ([2b7b0f1](https://github.com/rararulab/lake/commit/2b7b0f1cde087954b649ac535135acf07e2b8763))
* **ci:** install nextest for integration jobs ([#2](https://github.com/rararulab/lake/issues/2)) ([fa5e387](https://github.com/rararulab/lake/commit/fa5e387550afeaa19cf549ab777402dbb564b373))
* **ci:** install protoc for lance builds ([#2](https://github.com/rararulab/lake/issues/2)) ([edddf77](https://github.com/rararulab/lake/commit/edddf7783b3a4f077eab22961552ebe3d6aeb676))
* **ci:** isolate LocalStack test credentials ([#81](https://github.com/rararulab/lake/issues/81)) ([108fbc3](https://github.com/rararulab/lake/commit/108fbc30bd80c0c6a6585f2fd06c17ebac9517c0))
* **ci:** restore documentation and integration gates ([d7c5a53](https://github.com/rararulab/lake/commit/d7c5a536037450aa77d4fc237f6315b49d928610))
* **ci:** trigger policy checks on execution changes ([#164](https://github.com/rararulab/lake/issues/164)) ([#166](https://github.com/rararulab/lake/issues/166)) ([18a37ff](https://github.com/rararulab/lake/commit/18a37ff98ccf05dc9b9400f8cc7e967076af10b8))
* **commit:** close failover and table-recreate gaps ([#29](https://github.com/rararulab/lake/issues/29)) ([7e1e83d](https://github.com/rararulab/lake/commit/7e1e83d4c6bdd9018ad711c4f98160307b724abd))
* **commit:** make FILE append retries idempotent ([#29](https://github.com/rararulab/lake/issues/29)) ([5c70514](https://github.com/rararulab/lake/commit/5c70514b1a2ebe0128380f220f6929c92d3af9e7))
* **commit:** make FILE append retries idempotent ([#29](https://github.com/rararulab/lake/issues/29)) ([d6d6754](https://github.com/rararulab/lake/commit/d6d6754516705440648ad93b4b38983ce3cca2a8))
* **docs:** align Kubernetes Recreate availability contract ([#163](https://github.com/rararulab/lake/issues/163)) ([#171](https://github.com/rararulab/lake/issues/171)) ([7f3ca4f](https://github.com/rararulab/lake/commit/7f3ca4fb4fd59a26ca802a683b803c8fd14dcb5d))
* **docs:** repair rustdoc links ([#2](https://github.com/rararulab/lake/issues/2)) ([5a0f3f2](https://github.com/rararulab/lake/commit/5a0f3f205a912d60e7acc46a7c5cf59a66090d21))
* **engine:** bind reference staging to operation lifetime ([#91](https://github.com/rararulab/lake/issues/91)) ([2e64a5a](https://github.com/rararulab/lake/commit/2e64a5a3cbf061ae01427bf38adf09ace47b2546))
* **engine:** bind reference staging to operation lifetime ([#91](https://github.com/rararulab/lake/issues/91)) ([087dd76](https://github.com/rararulab/lake/commit/087dd761b06612e5cb11f78f17916467bc80bb24))
* **engine:** reconcile terminal stage cleanup races ([26734d7](https://github.com/rararulab/lake/commit/26734d72620063cc24ccd3db870aa7b117b99223))
* **engine:** reconcile terminal stage cleanup races ([#86](https://github.com/rararulab/lake/issues/86)) ([1df6dad](https://github.com/rararulab/lake/commit/1df6dad6ffec3c202226f034732483c9a2997c9e))
* **gate:** serialize upstream ADBC test process ([#179](https://github.com/rararulab/lake/issues/179)) ([#181](https://github.com/rararulab/lake/issues/181)) ([87c412b](https://github.com/rararulab/lake/commit/87c412ba04f99f291d9d51dc660f342987709aa2))
* **manifest:** bind history to incarnation ([#41](https://github.com/rararulab/lake/issues/41)) ([4a8c2e2](https://github.com/rararulab/lake/commit/4a8c2e27c623fa67800bef0c106cc3cb0b60b926))
* **manifest:** bind pointers to incarnation ([#41](https://github.com/rararulab/lake/issues/41)) ([276519d](https://github.com/rararulab/lake/commit/276519d1cdc95c74ea349ccf8d2011e0b4325938))
* **manifest:** fence delete and converge finalize ([#41](https://github.com/rararulab/lake/issues/41)) ([f3abfdc](https://github.com/rararulab/lake/commit/f3abfdc523d31a1aead571b6b64a246b2fc6ed53))
* **manifest:** fence resumed delete ([#42](https://github.com/rararulab/lake/issues/42)) ([1e73479](https://github.com/rararulab/lake/commit/1e73479021503f32d07895baa3d2934e33296f16))
* **manifest:** guard history creation ([#41](https://github.com/rararulab/lake/issues/41)) ([91433e9](https://github.com/rararulab/lake/commit/91433e942e7aea0b126ccfeae16dc479f000a62b))
* **meta:** harden Dynamo migration finalization ([#94](https://github.com/rararulab/lake/issues/94)) ([b73339b](https://github.com/rararulab/lake/commit/b73339b06926ea4b322f6c6fba7a3d11a68fa272))
* **meta:** make Dynamo metrics live and bounded ([#96](https://github.com/rararulab/lake/issues/96)) ([73cbd60](https://github.com/rararulab/lake/commit/73cbd603ac8fe2f3798d3f2fe1c2b66e1a45e6bc))
* **metasrv:** bound background task cleanup ([#57](https://github.com/rararulab/lake/issues/57)) ([69f46d2](https://github.com/rararulab/lake/commit/69f46d276dcfcc2d8c0326ae5490ae69634f05aa))
* **metasrv:** bound FILE append IPC decode memory ([#162](https://github.com/rararulab/lake/issues/162)) ([#169](https://github.com/rararulab/lake/issues/169)) ([b6a7994](https://github.com/rararulab/lake/commit/b6a7994e1ad145dc122cfe4d5f03b85eafcd66b7))
* **metasrv:** bound operation GC wall time ([#98](https://github.com/rararulab/lake/issues/98)) ([48b4148](https://github.com/rararulab/lake/commit/48b41482dd43f6ef97a91798d9c838aad6d96fbf))
* **metasrv:** bound the complete shutdown lifecycle ([bb5083d](https://github.com/rararulab/lake/commit/bb5083da2dda5f8907ac4d5712d3d8b02d144b77))
* **metasrv:** page control-plane catalog enumeration ([#138](https://github.com/rararulab/lake/issues/138)) ([#139](https://github.com/rararulab/lake/issues/139)) ([00535ac](https://github.com/rararulab/lake/commit/00535ac90fb608004cacc6548a0e0afec3bb99ba))
* **metasrv:** point-read durable drop recovery ([#37](https://github.com/rararulab/lake/issues/37)) ([1e7104e](https://github.com/rararulab/lake/commit/1e7104ecc83f5097647cbf66710d22e28db7c7a1))
* **metasrv:** retain partial GC cursor on shutdown ([#98](https://github.com/rararulab/lake/issues/98)) ([0b97809](https://github.com/rararulab/lake/commit/0b97809f43d44920643bcd1132051171070317d4))
* **metasrv:** serialize lease renewal publications ([#35](https://github.com/rararulab/lake/issues/35)) ([448fd81](https://github.com/rararulab/lake/commit/448fd818be93050f05ef08d8dea6dcbccaaa0558))
* **metasrv:** share one total shutdown deadline ([#57](https://github.com/rararulab/lake/issues/57)) ([224b57f](https://github.com/rararulab/lake/commit/224b57f8d00df52e3dd6735011f60b269db12f4c))
* **metasrv:** stop GC at shutdown boundaries ([#57](https://github.com/rararulab/lake/issues/57)) ([285a2f7](https://github.com/rararulab/lake/commit/285a2f7453f1faeef5110c581d86b4a6af4b30dd))
* **metasrv:** stop maintenance at shutdown boundaries ([#57](https://github.com/rararulab/lake/issues/57)) ([1385ab1](https://github.com/rararulab/lake/commit/1385ab148da0cb1698c19c4c7f7b7e41bb09503e))
* **meta:** wait for DynamoDB table readiness ([#152](https://github.com/rararulab/lake/issues/152)) ([#153](https://github.com/rararulab/lake/issues/153)) ([fc7347d](https://github.com/rararulab/lake/commit/fc7347d0ac11a7b42f826a08be2c14427bc500a9))
* **objects:** abort multipart uploads on cancellation ([#104](https://github.com/rararulab/lake/issues/104)) ([6dada73](https://github.com/rararulab/lake/commit/6dada73cd55d19ed3411e260f35e0c25391e93d3))
* **objects:** abort multipart uploads on cancellation ([#104](https://github.com/rararulab/lake/issues/104)) ([a3ef0f3](https://github.com/rararulab/lake/commit/a3ef0f3493a7216f6cb8447f612915d10214bb44))
* **objects:** bound S3 multipart FILE uploads ([#150](https://github.com/rararulab/lake/issues/150)) ([#151](https://github.com/rararulab/lake/issues/151)) ([1fc5158](https://github.com/rararulab/lake/commit/1fc515896c03c19fd42940fc9db373e695d6785c))
* **objects:** clean local staging after upload cancellation ([#161](https://github.com/rararulab/lake/issues/161)) ([#168](https://github.com/rararulab/lake/issues/168)) ([ef3b198](https://github.com/rararulab/lake/commit/ef3b19892b6593a34ff9f47b5afd5c306e005037))
* **objects:** reject truncated managed FILE range reads ([#154](https://github.com/rararulab/lake/issues/154)) ([#155](https://github.com/rararulab/lake/issues/155)) ([00362a5](https://github.com/rararulab/lake/commit/00362a586f352717d1dd188ef07fd2ce2509bec8))
* **observability:** harden metrics lifecycle coverage ([#69](https://github.com/rararulab/lake/issues/69)) ([f1ff39d](https://github.com/rararulab/lake/commit/f1ff39dfb62a22be520e39cc6334eb01049e2005))
* **query:** admit Flight discovery streams ([#51](https://github.com/rararulab/lake/issues/51)) ([d97866b](https://github.com/rararulab/lake/commit/d97866b113b5750e4981ce3348983aacf0bf56a8))
* **query:** bound async result manifest memory ([#130](https://github.com/rararulab/lake/issues/130)) ([#131](https://github.com/rararulab/lake/issues/131)) ([6fc2b5b](https://github.com/rararulab/lake/commit/6fc2b5b87c5e848ef6e35e0bc98e7a4d8cd1f323))
* **query:** bound async result memory ([e7c5a45](https://github.com/rararulab/lake/commit/e7c5a452116530cd79606eed15bb0757113c2ea4))
* **query:** bound async result memory ([#120](https://github.com/rararulab/lake/issues/120)) ([92cd0f1](https://github.com/rararulab/lake/commit/92cd0f1811c80ec1e81c71ddade1e05e979733b0))
* **query:** guard catalog refresh task lifetime ([#47](https://github.com/rararulab/lake/issues/47)) ([81edbb1](https://github.com/rararulab/lake/commit/81edbb150de5f56713c13aa5a1cd3f32dd101c18))
* **query:** preserve spill quota after rejection ([#39](https://github.com/rararulab/lake/issues/39)) ([453e650](https://github.com/rararulab/lake/commit/453e650daef659ee780142f47a36b518e7608d7f))
* **query:** release admission on stream error ([#51](https://github.com/rararulab/lake/issues/51)) ([d97e591](https://github.com/rararulab/lake/commit/d97e59164ac8e41a2124931c2bf07a4af18ce316))
* **query:** restore cold clippy manifest gate ([#159](https://github.com/rararulab/lake/issues/159)) ([#160](https://github.com/rararulab/lake/issues/160)) ([e0f7b3d](https://github.com/rararulab/lake/commit/e0f7b3d59367cf1b71c1610f9b49e50eff948148))
* **sdk:** align async poll decode limit with ticket bound ([#178](https://github.com/rararulab/lake/issues/178)) ([#183](https://github.com/rararulab/lake/issues/183)) ([c4516c0](https://github.com/rararulab/lake/commit/c4516c0992665578721e178ce7ec06e96e79e2ac))
* **sdk:** bound async result ticket metadata ([#174](https://github.com/rararulab/lake/issues/174)) ([#177](https://github.com/rararulab/lake/issues/177)) ([ccd0dc0](https://github.com/rararulab/lake/commit/ccd0dc04c10ae7934c966ad343d69bdbd175be88))
* **sdk:** consume all local Flight result endpoints ([#132](https://github.com/rararulab/lake/issues/132)) ([#133](https://github.com/rararulab/lake/issues/133)) ([f7e7672](https://github.com/rararulab/lake/commit/f7e767284b20d9356aaf36c3f51b582245aa2f16))
* **sdk:** enforce exact batch metadata bounds ([#77](https://github.com/rararulab/lake/issues/77)) ([d14f504](https://github.com/rararulab/lake/commit/d14f504501a2c789fdf855270968bd39398fe766))
* **sdk:** fence stale schema cache loaders ([#75](https://github.com/rararulab/lake/issues/75)) ([750a666](https://github.com/rararulab/lake/commit/750a666c678203e7d6a450c42a5d5c954ec17e28))
* **sdk:** preserve append identity beyond retry timeout ([#29](https://github.com/rararulab/lake/issues/29)) ([a4a76c2](https://github.com/rararulab/lake/commit/a4a76c2646eff8080a1a720b017af0a833907ed7))
* **sdk:** preserve public error source types ([#75](https://github.com/rararulab/lake/issues/75)) ([95e3e38](https://github.com/rararulab/lake/commit/95e3e389ce45cb59ec42fb9ae54d927a78a98987))
* **sdk:** redact and revalidate read capabilities ([#79](https://github.com/rararulab/lake/issues/79)) ([2c3c5f4](https://github.com/rararulab/lake/commit/2c3c5f4c893b28870a0613b903df9822ff9fef4f))
* **sdk:** restore clippy gate for capability tests ([#180](https://github.com/rararulab/lake/issues/180)) ([#182](https://github.com/rararulab/lake/issues/182)) ([420a443](https://github.com/rararulab/lake/commit/420a4435524a71c015794ec3bcfb6a5a18a3cc38))
* **sdk:** singleflight schema lookup failures ([#75](https://github.com/rararulab/lake/issues/75)) ([893f0ba](https://github.com/rararulab/lake/commit/893f0ba94ea5b4a9d21ce4e2985f2c43f4bad4d8))
* **sdk:** stream managed FILE example reads ([#135](https://github.com/rararulab/lake/issues/135)) ([#137](https://github.com/rararulab/lake/issues/137)) ([defd7f3](https://github.com/rararulab/lake/commit/defd7f39cb9ad64468f6f63c886f2ddebb4085be))
* **security:** fail closed without request identity ([#25](https://github.com/rararulab/lake/issues/25)) ([61f0ce0](https://github.com/rararulab/lake/commit/61f0ce07e694c0b2a7f4a6e4696921e1ed6998e0))
* **spec:** use Jujutsu change scope in lifecycle guard ([#175](https://github.com/rararulab/lake/issues/175)) ([#176](https://github.com/rararulab/lake/issues/176)) ([03f52ad](https://github.com/rararulab/lake/commit/03f52ad6ce88ac4e95ea5cecf3d1333393f73eae))


### Performance Improvements

* **catalog:** cache providers by table generation ([a31fc2b](https://github.com/rararulab/lake/commit/a31fc2b0202dd22e2c80f500d229a02a3209a191))
* **catalog:** cache providers by table generation ([#45](https://github.com/rararulab/lake/issues/45)) ([525b5a2](https://github.com/rararulab/lake/commit/525b5a2aa962a88fdd29c85effa5973990cb9936))
* **catalog:** gate directory refresh by generation ([#100](https://github.com/rararulab/lake/issues/100)) ([fbf4bb9](https://github.com/rararulab/lake/commit/fbf4bb9af0c8de100864a5f5c544a9bc77206c8b))
* **catalog:** gate directory refresh by generation ([#100](https://github.com/rararulab/lake/issues/100)) ([39bed0d](https://github.com/rararulab/lake/commit/39bed0db34c1a29f120804b1252bbad9b590ab17))
* **catalog:** publish immutable discovery generations ([d04fc0d](https://github.com/rararulab/lake/commit/d04fc0d64431b417e59a76ec62a7ead56a93cd06))
* **catalog:** publish immutable discovery generations ([#49](https://github.com/rararulab/lake/issues/49)) ([150f627](https://github.com/rararulab/lake/commit/150f627a89464098aa5d4696a716965fc8b29d73))
* **catalog:** serve last-good while revalidating ([eea7096](https://github.com/rararulab/lake/commit/eea70960e8af7744ced13bc239dc02381baf392e))
* **catalog:** serve last-good while revalidating ([#47](https://github.com/rararulab/lake/issues/47)) ([dd8155b](https://github.com/rararulab/lake/commit/dd8155b8b7214e15e23c4af96637653674d7f89e))
* **cli:** collapse managed-object GC registry N+1 reads ([#62](https://github.com/rararulab/lake/issues/62)) ([074e843](https://github.com/rararulab/lake/commit/074e843dbd6a69123dd4c67d6f800fc30f4aa8ed)), closes [#61](https://github.com/rararulab/lake/issues/61)
* **lance:** stream table removal results ([48b7cc9](https://github.com/rararulab/lake/commit/48b7cc9cfebdc7b5831ef27edea26c255d95967f))
* **lance:** stream table removal results ([#55](https://github.com/rararulab/lake/issues/55)) ([65394db](https://github.com/rararulab/lake/commit/65394dbf69a36ef6a57c0a797f462d789e7743fb))
* **meta:** isolate Dynamo prefix reads ([0e37e49](https://github.com/rararulab/lake/commit/0e37e490ccb305ef79d0dc3b1831277cfe9e3a24))
* **meta:** isolate Dynamo prefix reads ([#94](https://github.com/rararulab/lake/issues/94)) ([a549d05](https://github.com/rararulab/lake/commit/a549d0527ac82d9e7a2b95054130403711390770))
* **metasrv:** admit FILE append lifetimes ([#53](https://github.com/rararulab/lake/issues/53)) ([a83eb22](https://github.com/rararulab/lake/commit/a83eb2284937841b6f57ce603458f444517bd049))
* **metasrv:** bound append concurrency and buffered metadata ([86f3eec](https://github.com/rararulab/lake/commit/86f3eecb2c8a73ea7a5e15589c0c1940da4ca36d))
* **metasrv:** drain bounded operation GC pages ([#98](https://github.com/rararulab/lake/issues/98)) ([14e6c6a](https://github.com/rararulab/lake/commit/14e6c6a94d396c8c3ccaff4e7b055ed182b63103))
* **metasrv:** make append-operation GC keep up with sustained writes ([0c095f5](https://github.com/rararulab/lake/commit/0c095f50649968b90844508ebe3d689b6ce69800))
* **metasrv:** page registry table maintenance ([1873c8a](https://github.com/rararulab/lake/commit/1873c8a2ae66cd4fcd18acf62f45a7daadeb961a))
* **metasrv:** page table maintenance work ([#59](https://github.com/rararulab/lake/issues/59)) ([b95d7cb](https://github.com/rararulab/lake/commit/b95d7cb05422b1f95869d9966d610e28856aa08f))
* **objects:** pipeline bounded multipart uploads ([#102](https://github.com/rararulab/lake/issues/102)) ([c226c7f](https://github.com/rararulab/lake/commit/c226c7f938006210d461a2b1390f0463dfa3b832))
* **objects:** pipeline bounded multipart uploads ([#102](https://github.com/rararulab/lake/issues/102)) ([9fc22a9](https://github.com/rararulab/lake/commit/9fc22a9cc6064b9ef7a5e68d98e5ae8a4065420b))
* **query:** bound Flight discovery work and response batches ([a7417b8](https://github.com/rararulab/lake/commit/a7417b8d2456155dd68393895da1c8927d62803e))
* **query:** stream bounded discovery batches ([#51](https://github.com/rararulab/lake/issues/51)) ([089c340](https://github.com/rararulab/lake/commit/089c340ad27c02d3c8516c697fe81f74a4bc6e6a))

## Changelog

All notable changes to lake will be documented in this file.

This file is maintained by Release Please from Conventional Commits merged into
`main`.

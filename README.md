[//]: # (Copyright 2021-2023 Ness Digital Engineering)

[//]: # ()
[//]: # (Licensed under the Apache License, Version 2.0 &#40;the "License"&#41;;)

[//]: # (you may not use this file except in compliance with the License.)

[//]: # (You may obtain a copy of the License at)

[//]: # ()
[//]: # (http://www.apache.org/licenses/LICENSE-2.0)

[//]: # ()
[//]: # (Unless required by applicable law or agreed to in writing, software)

[//]: # (distributed under the License is distributed on an "AS IS" BASIS,)

[//]: # (WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.)

[//]: # (See the License for the specific language governing permissions and)

[//]: # (limitations under the License.)

# Flink generic tools
This module contains generic (not project specific) tools related to any Flink based application.

Project contains sub-modules:
- [Flink-baseline](flink-baseline/README.md)
- [Flink-common](flink-baseline/flink-common/README.md) 
- [Flink-snapshot](flink-baseline/flink-snapshot/README.md) 
- [Flink-jdbc-sink](flink-baseline/flink-jdbc-sink/README.md)
- [SmoothingPrices Flink application](flink-baseline/flink-example/README.md)
- [SmoothingPrices Integration test](flink-test-example/README.md)

## Dependencies
Module requires:
 - Latest version of Java SE Development Kit 11
 - Maven Apache Maven 3.6.2+

## Build
- Locally
```bash
 $ maven clean install
```
- [Jenkins](https://jenkins.cicd.rfs.riskfocus.com/job/riskfocus/job/flink-generic/) 
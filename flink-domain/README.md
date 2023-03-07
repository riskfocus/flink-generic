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
# Common domain & utility classes 

Flink application works with [Time](https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/concepts/time/)

This module contains useful interfaces and utility classes which could be used in your Flink pipeline 

See details in classes documentation:
- Time related
  - [Event](src/main/java/com/ness/flink/domain/Event.java)
  - [IncomingEvent](src/main/java/com/ness/flink/domain/IncomingEvent.java)
  - [EventUtils](src/main/java/com/ness/flink/util/EventUtils.java)
- Pipeline related  
  - [KafkaKeyedAware](src/main/java/com/ness/flink/domain/KafkaKeyedAware.java)
  - [FlinkKeyedAware](src/main/java/com/ness/flink/domain/FlinkKeyedAware.java)

Usage of these classes you could find in [flink-example-domain](../flink-example-domain/src/main/java/com/riskfocus/flink/example/pipeline/domain) module

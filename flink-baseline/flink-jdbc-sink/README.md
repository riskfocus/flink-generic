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

# Flink JDBC Sink module

Module provides JDBC Sink function.


Original [Flink JDBC connector](https://github.com/apache/flink-connector-jdbc) has a nasty bug which prevents of usage bulk inserts ([see details](https://issues.apache.org/jira/browse/FLINK-17488))

- [Sink parameters configuration](src/main/java/com/ness/flink/sink/jdbc/properties/JdbcSinkProperties.java)

## JDBC Sink Usage 

[Example of usage](src/test/java/com/ness/flink/sink/jdbc/JdbcSinkIT.java)

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

# Integration Test for SmoothingPrices application

Test application [SmoothingIT](src/test/java/com/ness/flink/test/example/SmoothingIT.java) (implementation based on TestNG) 

- Test send windowed data
  - send `com.ness.flink.example.pipeline.domain.InterestRate`
  - send `com.ness.flink.example.pipeline.domain.OptionPrice`
- Test checks produced `com.ness.flink.example.pipeline.domain.SmoothingRequest` by Flink application with expected results (Test checks based on eventual consistency). 

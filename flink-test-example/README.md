# Test Smoothing

Test application `com.ness.flink.test.example.SmoothingIT` (implementation based on TestNG) 

- Test send windowed data
  - send `com.ness.flink.example.pipeline.domain.InterestRate`
  - send `com.ness.flink.example.pipeline.domain.OptionPrice`
- Test checks produced `com.ness.flink.example.pipeline.domain.SmoothingRequest` by Flink application with expected results (Test checks based on eventual consistency). 

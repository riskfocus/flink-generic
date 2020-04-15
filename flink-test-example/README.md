# Test Smoothing

Test application `com.riskfocus.flink.test.example.SmoothingIT` (implementation based on TestNG) 

- Test send windowed data
  - send `com.riskfocus.flink.example.pipeline.domain.InterestRate`
  - send `com.riskfocus.flink.example.pipeline.domain.OptionPrice`
- Test checks produced `com.riskfocus.flink.example.pipeline.domain.SmoothingRequest` by Flink application with expected results (Test checks based on eventual consistency). 

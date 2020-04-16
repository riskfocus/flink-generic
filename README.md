# Flink generic tools
This module contains generic (not project specific) tools related to any Flink based application.

Project contains sub-modules:
- [Flink-baseline](flink-baseline/README.md)
- [Flink-common](flink-baseline/flink-common) 
- [Flink-snapshot](flink-baseline/flink-snapshot/README.md) 
- [Sample of Flink application](flink-baseline/flink-example/README.md)
- [Integration test](flink-test-example/src/test/java/com/riskfocus/flink/test/example/SmoothingIT.java)

## Dependencies
Module requires:
 - Latest version of Java SE Development Kit 11
 - Maven Apache Maven 3.6.2+

## Build
```bash
 $ maven clean install
```
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
 - Latest version of Java SE Development Kit 17
 - Maven Apache Maven 3.6.2+

## Build
- Locally
```bash
 $ maven clean install
```
- [Jenkins](https://jenkins.cicd.rfs.riskfocus.com/job/riskfocus/job/flink-generic/) 
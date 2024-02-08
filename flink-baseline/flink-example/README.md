# SmoothingPrices Flink application 
This module contains example of Flink application (SmoothingPrices).

## Application Description

https://ness-nde.atlassian.net/wiki/spaces/RSH/pages/3498115669/Example+of+Flink+Application+Price+Smoothing

## Run example of pipeline
 1. Build jar using maven from the root directory
    ```bash
    $ mvn clean package
    ```
 2. Start Development cluster
    ```bash
    $ cd docker
    $ docker-compose up -d
    ```    
 3. Make sure that all containers up and running `docker ps`
 4. Wait for `initializer` to create topics, this will take a ~ 2 min.
    ```bash
    docker logs -f initializer
    ...
    Created topic optionsPricesLive.
    Created topic ycInputsLive.
    Created topic interestRatesSnapshot.
    Created topic smoothingInputsLatest.
    ```
 5. Now you can run application locally in IntelliJ IDEA.
    1. You have to specify as a `Main class:` in Run Configuration `com.ness.flink.example.pipeline.SmoothingPricesJob`.
    2. Then check setting: `Include dependencies with "Provided" scope`
    3. Specify parameters: `-localDev true -localParallelism 4` in `Program arguments:` field.
 6. You could use [Grafana](http://localhost:3000) to monitor your app
 7. You could also explore [Prometheus](http://localhost:9090) metrics 

### "Feature flag" apply new configuration without restart (runtime)
1. Go inside Kafka docker container 
   ```bash
      docker exec -it kafka bash
   ```
2. Run kafka-console-producer inside container
   ```bash 
      kafka-console-producer --topic configuration --broker-list kafka:9093
   ```
3. Produce message with updated configuration
   ```json 
      {"configurationId": 1, "extendedLogging": "true"}
   ```
   See domain structure for details: [JobConfig](../../flink-example-domain/src/main/java/com/ness/flink/example/pipeline/domain/JobConfig.java)  
4. You should see in log output (Flink app logs):
    ```
     INFO  [interestRatesEnricher.operator -> smoothing.request.sink: Writer -> smoothing.request.sink: Committer (2/4)#0] c.n.f.e.p.m.s.f.ProcessSmoothingFunction: Got updated configuration: JobConfig(configurationId=1, extendedLogging=true) 
    ```
5. Generate payload [SmoothingIT](../../flink-test-example/README.md)
6. And pipeline will start produce extended logging (Flink app logs):
    ```
    INFO  [interestRatesEnricher.operator -> smoothing.request.sink: Writer -> smoothing.request.sink: Committer (3/4)#0] c.n.f.e.p.m.s.f.ProcessSmoothingFunction: 496 processing, timestamp: 1680275297163
    INFO  [interestRatesEnricher.operator -> smoothing.request.sink: Writer -> smoothing.request.sink: Committer (2/4)#0] c.n.f.e.p.m.s.f.ProcessSmoothingFunction: W168027530 processing, timestamp: 1680275297148
    ```
7. For turning off extended logging put new message to Kafka
   ```json 
      {"configurationId": 2, "extendedLogging": "false"}
   ```



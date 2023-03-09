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



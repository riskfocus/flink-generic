# Example of simple application 
This module contains basic example of Flink application
(currently application in developing status)
## Application Description

https://wiki.riskfocus.com/pages/viewpage.action?pageId=147395358


## Run example of pipeline
 1. Build jar using maven from the root directory
    ```bash
    $ mvn clean package
    ```
 2. Start the Kafka cluster
    ```bash
    $ cd docker
    $ docker-compose up -d
    ```
 3. You can run application locally in IntelliJ IDEA.
    1. You have to specify as a `Main class:` in Run Configuration `com.riskfocus.flink.example.pipeline.ECommerce`.
    2. Then check setting: `Include dependencies with "Provided" scope`
    3. Also you need to specify parameter `-local.dev true` in `Program arguments:` field.



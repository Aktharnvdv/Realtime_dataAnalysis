# debezium-kafka-pyspark pipeline with Transformer based predictive model

## Prerequisites
- Docker
- Docker Compose

## Setup & Running Instructions

1. **Start the Docker Containers**:
    ```bash
    sudo docker-compose up
    ```

2. **Run Kafka Console Consumer**:
    ```bash
    bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic server1.Naveed.dbo.Table1 --from-beginning
    ```

3. **Enter the Kafka Container**:
    ```bash
    docker exec -it oman_ai_project-kafka-1 /bin/bash
    ```

4. **Validate SqlServerConnector Configuration**:
    ```bash
    sudo curl -i -X PUT -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connector-plugins/io.debezium.connector.sqlserver.SqlServerConnector/config/validate -d @payload.json
    ```

5. **POST Request to Connectors**:
    ```bash
    sudo curl -H "Accept:application/json" -H "Content-Type:application/json" -X POST -d @payload1.json http://localhost:8083/connectors
    ```

6. **GET Request to Check Connectors**:
    ```bash
    curl -X GET http://localhost:8083/connectors
    ```

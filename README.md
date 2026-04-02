# VkTest

gRPC key-value service on Java with Tarantool.

## Features

- Put key/value
- Get by key
- Delete by key
- Range query as gRPC stream
- Count records
- Supports nullable `value`

## Stack

- Java 21
- gRPC
- Protocol Buffers
- Tarantool 3.2
- Docker Compose
- Gradle

## Run
Main.java + TestClient.java for testing methods
### Start Tarantool
docker compose up -d
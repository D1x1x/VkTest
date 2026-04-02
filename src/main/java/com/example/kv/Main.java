package com.example.kv;

import io.grpc.Server;
import io.grpc.ServerBuilder;

public class Main {

    public static void main(String[] args) throws Exception {
        String tarantoolHost = "127.0.0.1";
        int tarantoolPort = 3301;
        String tarantoolUser = "app";
        String tarantoolPassword = "app";

        int grpcPort = 9090;

        TarantoolRepository repository = new TarantoolRepository(
                tarantoolHost,
                tarantoolPort,
                tarantoolUser,
                tarantoolPassword
        );

        KvGrpcService service = new KvGrpcService(repository);

        Server server = ServerBuilder
                .forPort(grpcPort)
                .addService(service)
                .build()
                .start();

        System.out.println("gRPC server started on port " + grpcPort);
        System.out.println("Connected to Tarantool on " + tarantoolHost + ":" + tarantoolPort);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                repository.close();
            } catch (Exception ignored) {
            }
            server.shutdown();
        }));

        server.awaitTermination();
    }
}
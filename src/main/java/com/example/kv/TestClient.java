package com.example.kv;

import com.example.kv.proto.*;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public class TestClient {
    public static void main(String[] args) {
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("127.0.0.1", 9090)
                .usePlaintext()
                .build();

        KvStoreGrpc.KvStoreBlockingStub stub = KvStoreGrpc.newBlockingStub(channel);

        CountResponse count1 = stub.count(CountRequest.newBuilder().build());
        System.out.println("Initial count = " + count1.getCount());

        PutResponse put1 = stub.put(
                PutRequest.newBuilder()
                        .setKey("a")
                        .setHasValue(true)
                        .setValue(ByteString.copyFrom("hello".getBytes(StandardCharsets.UTF_8)))
                        .build()
        );
        System.out.println("Put a = " + put1.getSuccess());

        GetResponse get1 = stub.get(
                GetRequest.newBuilder().setKey("a").build()
        );
        System.out.println("Get a found = " + get1.getFound());
        System.out.println("Get a hasValue = " + get1.getHasValue());
        if (get1.getHasValue()) {
            System.out.println("Get a value = " + get1.getValue().toStringUtf8());
        }

        PutResponse put2 = stub.put(
                PutRequest.newBuilder()
                        .setKey("b")
                        .setHasValue(false)
                        .build()
        );
        System.out.println("Put b(null) = " + put2.getSuccess());

        GetResponse get2 = stub.get(
                GetRequest.newBuilder().setKey("b").build()
        );
        System.out.println("Get b found = " + get2.getFound());
        System.out.println("Get b hasValue = " + get2.getHasValue());

        CountResponse count2 = stub.count(CountRequest.newBuilder().build());
        System.out.println("Count after inserts = " + count2.getCount());

        Iterator<RangeResponse> iterator = stub.range(
                RangeRequest.newBuilder()
                        .setKeySince("a")
                        .setKeyTo("z")
                        .build()
        );

        System.out.println("Range [a, z):");
        while (iterator.hasNext()) {
            RangeResponse item = iterator.next();
            System.out.println("key = " + item.getKey() + ", hasValue = " + item.getHasValue());
            if (item.getHasValue()) {
                System.out.println("value = " + item.getValue().toStringUtf8());
            }
        }

        DeleteResponse delete = stub.delete(
                DeleteRequest.newBuilder().setKey("a").build()
        );
        System.out.println("Delete a = " + delete.getDeleted());

        GetResponse getAfterDelete = stub.get(
                GetRequest.newBuilder().setKey("a").build()
        );
        System.out.println("Get a after delete found = " + getAfterDelete.getFound());

        CountResponse count3 = stub.count(CountRequest.newBuilder().build());
        System.out.println("Final count = " + count3.getCount());

        channel.shutdown();
    }
}
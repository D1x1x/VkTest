package com.example.kv;

import com.example.kv.proto.CountRequest;
import com.example.kv.proto.CountResponse;
import com.example.kv.proto.DeleteRequest;
import com.example.kv.proto.DeleteResponse;
import com.example.kv.proto.GetRequest;
import com.example.kv.proto.GetResponse;
import com.example.kv.proto.KvStoreGrpc;
import com.example.kv.proto.PutRequest;
import com.example.kv.proto.PutResponse;
import com.example.kv.proto.RangeRequest;
import com.example.kv.proto.RangeResponse;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.Optional;

public class KvGrpcService extends KvStoreGrpc.KvStoreImplBase {

    private final TarantoolRepository repository;

    public KvGrpcService(TarantoolRepository repository) {
        this.repository = repository;
    }

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        try {
            checkKey(request.getKey());

            byte[] value = request.getHasValue() ? request.getValue().toByteArray() : null;

            repository.put(request.getKey(), request.getHasValue(), value);

            PutResponse response = PutResponse.newBuilder()
                    .setSuccess(true)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (IllegalArgumentException e) {
            responseObserver.onError(
                    Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException()
            );
        } catch (Exception e) {
            responseObserver.onError(
                    Status.INTERNAL.withDescription("Put error: " + e.getMessage()).asRuntimeException()
            );
        }
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        try {
            checkKey(request.getKey());

            Optional<KvEntry> optionalEntry = repository.get(request.getKey());

            GetResponse.Builder builder = GetResponse.newBuilder();

            if (optionalEntry.isEmpty()) {
                builder.setFound(false);
                builder.setHasValue(false);
            } else {
                KvEntry entry = optionalEntry.get();
                builder.setFound(true);
                builder.setHasValue(entry.isHasValue());

                if (entry.isHasValue() && entry.getValue() != null) {
                    builder.setValue(ByteString.copyFrom(entry.getValue()));
                }
            }

            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();

        } catch (IllegalArgumentException e) {
            responseObserver.onError(
                    Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException()
            );
        } catch (Exception e) {
            responseObserver.onError(
                    Status.INTERNAL.withDescription("Get error: " + e.getMessage()).asRuntimeException()
            );
        }
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
        try {
            checkKey(request.getKey());

            boolean deleted = repository.delete(request.getKey());

            DeleteResponse response = DeleteResponse.newBuilder()
                    .setDeleted(deleted)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (IllegalArgumentException e) {
            responseObserver.onError(
                    Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException()
            );
        } catch (Exception e) {
            responseObserver.onError(
                    Status.INTERNAL.withDescription("Delete error: " + e.getMessage()).asRuntimeException()
            );
        }
    }

    @Override
    public void range(RangeRequest request, StreamObserver<RangeResponse> responseObserver) {
        try {
            checkKey(request.getKeySince());
            checkKey(request.getKeyTo());

            if (request.getKeySince().compareTo(request.getKeyTo()) > 0) {
                throw new IllegalArgumentException("key_since must be <= key_to");
            }

            repository.range(request.getKeySince(), request.getKeyTo(), entry -> {
                RangeResponse.Builder builder = RangeResponse.newBuilder()
                        .setKey(entry.getKey())
                        .setHasValue(entry.isHasValue());

                if (entry.isHasValue() && entry.getValue() != null) {
                    builder.setValue(ByteString.copyFrom(entry.getValue()));
                }

                responseObserver.onNext(builder.build());
            });

            responseObserver.onCompleted();

        } catch (IllegalArgumentException e) {
            responseObserver.onError(
                    Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException()
            );
        } catch (Exception e) {
            responseObserver.onError(
                    Status.INTERNAL.withDescription("Range error: " + e.getMessage()).asRuntimeException()
            );
        }
    }

    @Override
    public void count(CountRequest request, StreamObserver<CountResponse> responseObserver) {
        try {
            long count = repository.count();

            CountResponse response = CountResponse.newBuilder()
                    .setCount(count)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(
                    Status.INTERNAL.withDescription("Count error: " + e.getMessage()).asRuntimeException()
            );
        }
    }

    private void checkKey(String key) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("Key must not be empty");
        }
    }
}
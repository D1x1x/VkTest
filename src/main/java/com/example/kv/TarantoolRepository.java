package com.example.kv;

import io.tarantool.client.box.TarantoolBoxClient;
import io.tarantool.client.box.TarantoolBoxSpace;
import io.tarantool.client.box.options.SelectOptions;
import io.tarantool.client.factory.TarantoolFactory;
import io.tarantool.core.protocol.BoxIterator;
import io.tarantool.mapping.SelectResponse;
import io.tarantool.mapping.TarantoolResponse;
import io.tarantool.mapping.Tuple;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class TarantoolRepository {

    private static final String SPACE_NAME = "KV";
    private static final String INDEX_NAME = "primary";
    private static final int RANGE_BATCH_SIZE = 500;

    private final TarantoolBoxClient client;
    private final TarantoolBoxSpace space;

    public TarantoolRepository(String host, int port, String user, String password) throws Exception {
        this.client = TarantoolFactory.box()
                .withHost(host)
                .withPort(port)
                .withUser(user)
                .withPassword(password)
                .build();

        this.space = client.space(SPACE_NAME);
    }

    public void put(String key, boolean hasValue, byte[] value) throws Exception {
        List<Object> tuple = new java.util.ArrayList<>();
        tuple.add(key);
        tuple.add(hasValue ? value : null);

        space.replace(tuple).get(5, TimeUnit.SECONDS);
    }

    public Optional<KvEntry> get(String key) throws Exception {
        SelectResponse<List<Tuple<List<?>>>> response =
                space.select(List.of(key)).get(5, TimeUnit.SECONDS);

        List<Tuple<List<?>>> rows = response.get();

        if (rows == null || rows.isEmpty()) {
            return Optional.empty();
        }

        Tuple<List<?>> tuple = rows.get(0);
        List<?> data = tuple.get();

        String foundKey = (String) data.get(0);
        byte[] value = (byte[]) data.get(1);
        boolean hasValue = value != null;

        return Optional.of(new KvEntry(foundKey, hasValue, value));
    }

    public boolean delete(String key) throws Exception {
        Tuple<List<?>> deleted = space.delete(List.of(key)).get(5, TimeUnit.SECONDS);
        return deleted != null;
    }

    public void range(String keySince, String keyTo, Consumer<KvEntry> consumer) throws Exception {
        Object after = null;

        while (true) {
            SelectOptions.Builder optionsBuilder = SelectOptions.builder()
                    .withIndex(INDEX_NAME)
                    .withIterator(BoxIterator.GE)
                    .withLimit(RANGE_BATCH_SIZE)
                    .fetchPosition();

            if (after != null) {
                optionsBuilder.after(after);
            }

            SelectOptions options = optionsBuilder.build();

            SelectResponse<List<Tuple<List<?>>>> response =
                    space.select(List.of(keySince), options).get(5, TimeUnit.SECONDS);

            List<Tuple<List<?>>> rows = response.get();

            if (rows == null || rows.isEmpty()) {
                break;
            }

            for (Tuple<List<?>> tuple : rows) {
                List<?> data = tuple.get();

                String currentKey = (String) data.get(0);
                if (currentKey.compareTo(keyTo) >= 0) {
                    return;
                }

                byte[] value = (byte[]) data.get(1);
                boolean hasValue = value != null;

                consumer.accept(new KvEntry(currentKey, hasValue, value));
            }

            byte[] position = response.getPosition();
            if (position == null || position.length == 0) {
                break;
            }

            after = position;
        }
    }

    public long count() throws Exception {
        TarantoolResponse<List<Long>> response =
                client.call("kv_count", Long.class).get(5, TimeUnit.SECONDS);

        List<Long> data = response.get();

        if (data == null || data.isEmpty() || data.get(0) == null) {
            return 0;
        }

        return data.get(0);
    }

    public void close() throws Exception {
        client.close();
    }
}
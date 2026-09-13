/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoClient;

import io.debezium.DebeziumException;

/**
 * Resolves the shard key of a sharded collection and reproduces the change stream {@code documentKey} of a document
 * from it.
 * <p>
 * MongoDB reports a {@code documentKey} on every change stream event. For an unsharded collection it holds just
 * {@code _id}; for a sharded collection it holds the shard key fields followed by {@code _id}. Snapshot events are read
 * straight from the collection and therefore carry no {@code documentKey}, so the same value has to be derived from the
 * full document in order for snapshot and streaming events of one document to share a key.
 *
 * @author Soyeong Choe
 */
public class ShardKeys {

    private static final Logger LOGGER = LoggerFactory.getLogger(ShardKeys.class);

    private static final String ID_FIELD_NAME = "_id";
    private static final String CONFIG_DATABASE = "config";
    private static final String COLLECTIONS_COLLECTION = "collections";
    private static final String KEY_FIELD = "key";
    private static final String DROPPED_FIELD = "dropped";

    /**
     * Shard key paths per collection. An empty list means "not sharded"; a collection whose shard key cannot be
     * determined is reported rather than cached, so an entry never stands for an unknown shard key.
     */
    private final Map<CollectionId, List<String>> shardKeyPaths = new ConcurrentHashMap<>();
    private final Supplier<MongoClient> clientSupplier;

    /**
     * @param clientSupplier supplies a client used to read {@code config.collections}; may be null, in which case every
     *            collection is treated as unsharded
     */
    public ShardKeys(Supplier<MongoClient> clientSupplier) {
        this.clientSupplier = clientSupplier;
    }

    /**
     * Returns a {@link ShardKeys} instance that treats every collection as unsharded. Useful where no connection is
     * available and the {@code documentKey} therefore degenerates to {@code _id}.
     */
    public static ShardKeys unsharded() {
        return new ShardKeys(null);
    }

    /**
     * Returns the shard key field paths of the given collection, in shard key order, or an empty list if the collection
     * is not sharded.
     * <p>
     * The result is cached for the lifetime of this instance, so enabling sharding on a collection while the connector
     * is running is not picked up until the task restarts.
     *
     * @throws io.debezium.DebeziumException if {@code config.collections} cannot be read, or if it holds an entry for
     *             the collection that carries no usable shard key
     */
    public List<String> shardKeyPathsFor(CollectionId collectionId) {
        if (clientSupplier == null) {
            return List.of();
        }
        return shardKeyPaths.computeIfAbsent(collectionId, this::readShardKeyPaths);
    }

    private List<String> readShardKeyPaths(CollectionId collectionId) {
        Document entry;
        try (MongoClient client = clientSupplier.get()) {
            Document filter = new Document(ID_FIELD_NAME, collectionId.identifier())
                    .append(DROPPED_FIELD, new Document("$ne", true));

            entry = client.getDatabase(CONFIG_DATABASE)
                    .getCollection(COLLECTIONS_COLLECTION)
                    .find(filter)
                    .first();
        }
        catch (Exception e) {
            // Guessing here would silently emit a key that does not match the one derived from the change stream
            // documentKey of the same document, which breaks incremental snapshot deduplication and log compaction.
            throw new DebeziumException("Unable to read the shard key of collection '" + collectionId + "' from '"
                    + CONFIG_DATABASE + "." + COLLECTIONS_COLLECTION + "'. This is required by '"
                    + MongoDbConnectorConfig.CHANGE_EVENT_KEY_MODE.name() + "="
                    + MongoDbConnectorConfig.ChangeEventKeyMode.DOCUMENT_KEY.getValue() + "'.", e);
        }

        return shardKeyPathsOf(entry, collectionId);
    }

    /**
     * Extracts the shard key field paths from a {@code config.collections} entry.
     * <p>
     * A missing entry means the collection is not sharded, so its document key is {@code _id} alone. An entry that
     * carries no usable shard key is reported instead: it says the collection is sharded without saying what the
     * shard key is, and falling back to {@code _id} there would emit a key that does not match the change stream
     * documentKey of the same document.
     *
     * @param entry the {@code config.collections} entry of the collection; may be null
     * @param collectionId the collection the entry was read for; used for reporting
     * @return the shard key field paths in shard key order, or an empty list if the collection is not sharded
     * @throws io.debezium.DebeziumException if the entry carries no usable shard key
     */
    static List<String> shardKeyPathsOf(Document entry, CollectionId collectionId) {
        if (entry == null) {
            LOGGER.debug("Collection '{}' is not sharded, its document key is '{}'", collectionId, ID_FIELD_NAME);
            return List.of();
        }

        Object key = entry.get(KEY_FIELD);
        if (!(key instanceof Document) || ((Document) key).isEmpty()) {
            throw new DebeziumException("Collection '" + collectionId + "' is sharded, but its entry in '"
                    + CONFIG_DATABASE + "." + COLLECTIONS_COLLECTION + "' carries no usable shard key ('"
                    + KEY_FIELD + "' is " + key + "). A shard key is required by '"
                    + MongoDbConnectorConfig.CHANGE_EVENT_KEY_MODE.name() + "="
                    + MongoDbConnectorConfig.ChangeEventKeyMode.DOCUMENT_KEY.getValue() + "'.");
        }

        List<String> paths = List.copyOf(((Document) key).keySet());
        LOGGER.info("Resolved shard key {} for collection '{}'", paths, collectionId);
        return paths;
    }

    /**
     * Builds the change stream {@code documentKey} of a document, that is the shard key fields in shard key order
     * followed by {@code _id}, unless {@code _id} is part of the shard key already.
     *
     * @param document the full document; may not be null
     * @param shardKeyPaths the shard key field paths in shard key order; may be empty for an unsharded collection
     * @return the document key; never null
     */
    public static BsonDocument documentKeyOf(BsonDocument document, List<String> shardKeyPaths) {
        BsonDocument documentKey = new BsonDocument();

        for (String path : shardKeyPaths) {
            BsonValue value = resolve(document, path);
            if (value != null) {
                documentKey.put(path, value);
            }
        }

        if (!documentKey.containsKey(ID_FIELD_NAME)) {
            BsonValue id = document.get(ID_FIELD_NAME);
            if (id != null) {
                documentKey.put(ID_FIELD_NAME, id);
            }
        }

        return documentKey;
    }

    /**
     * Resolves a possibly dotted shard key path such as {@code address.zip} against a document. MongoDB reports such a
     * field under its dotted name in the {@code documentKey}, so the path is used verbatim as the key name.
     */
    private static BsonValue resolve(BsonDocument document, String path) {
        if (path.indexOf('.') < 0) {
            return document.get(path);
        }

        BsonValue current = document;
        for (String part : splitPath(path)) {
            if (current == null || !current.isDocument()) {
                return null;
            }
            current = current.asDocument().get(part);
        }
        return current;
    }

    private static List<String> splitPath(String path) {
        List<String> parts = new ArrayList<>();
        int start = 0;
        for (int i = 0; i < path.length(); i++) {
            if (path.charAt(i) == '.') {
                parts.add(path.substring(start, i));
                start = i + 1;
            }
        }
        parts.add(path.substring(start));
        return parts;
    }
}

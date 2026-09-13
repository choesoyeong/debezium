/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb;

import java.util.List;

import org.assertj.core.api.Assertions;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;

/**
 * Verifies that the document key derived from a full document during a snapshot matches the {@code documentKey} that
 * MongoDB reports on a change stream event for the same document. The two have to be identical, otherwise incremental
 * snapshot deduplication and log compaction break.
 */
public class ShardKeysTest {

    private static final BsonObjectId ID = new BsonObjectId();
    private static final CollectionId COLLECTION = new CollectionId("dbA", "c1");

    @Test
    void shouldUseOnlyIdWhenCollectionIsNotSharded() {
        var document = new BsonDocument("_id", ID)
                .append("name", new BsonString("Mary"));

        var documentKey = ShardKeys.documentKeyOf(document, List.of());

        Assertions.assertThat(documentKey).isEqualTo(new BsonDocument("_id", ID));
    }

    @Test
    void shouldPutShardKeyBeforeId() {
        var document = new BsonDocument("_id", ID)
                .append("caseNo", new BsonString("201907130000200001"))
                .append("name", new BsonString("Mary"));

        var documentKey = ShardKeys.documentKeyOf(document, List.of("caseNo"));

        // Field order matters, the serialized key has to match the change stream documentKey byte for byte
        Assertions.assertThat(documentKey).isEqualTo(
                new BsonDocument("caseNo", new BsonString("201907130000200001")).append("_id", ID));
        Assertions.assertThat(List.copyOf(documentKey.keySet())).containsExactly("caseNo", "_id");
    }

    @Test
    void shouldPreserveCompoundShardKeyOrder() {
        var document = new BsonDocument("_id", ID)
                .append("region", new BsonString("eu"))
                .append("tenant", new BsonInt32(7));

        var documentKey = ShardKeys.documentKeyOf(document, List.of("tenant", "region"));

        Assertions.assertThat(List.copyOf(documentKey.keySet())).containsExactly("tenant", "region", "_id");
    }

    @Test
    void shouldNotDuplicateIdWhenItIsPartOfTheShardKey() {
        var document = new BsonDocument("_id", ID)
                .append("tenant", new BsonInt32(7));

        var documentKey = ShardKeys.documentKeyOf(document, List.of("tenant", "_id"));

        Assertions.assertThat(List.copyOf(documentKey.keySet())).containsExactly("tenant", "_id");
    }

    @Test
    void shouldKeepIdInShardKeyPosition() {
        var document = new BsonDocument("_id", ID)
                .append("tenant", new BsonInt32(7));

        var documentKey = ShardKeys.documentKeyOf(document, List.of("_id", "tenant"));

        Assertions.assertThat(List.copyOf(documentKey.keySet())).containsExactly("_id", "tenant");
    }

    @Test
    void shouldResolveNestedShardKeyUnderItsDottedName() {
        var document = new BsonDocument("_id", ID)
                .append("address", new BsonDocument("zip", new BsonString("12345")));

        var documentKey = ShardKeys.documentKeyOf(document, List.of("address.zip"));

        // MongoDB reports a nested shard key under its dotted name, not as a nested document
        Assertions.assertThat(documentKey).isEqualTo(
                new BsonDocument("address.zip", new BsonString("12345")).append("_id", ID));
    }

    @Test
    void shouldSkipShardKeyFieldMissingFromDocument() {
        var document = new BsonDocument("_id", ID);

        var documentKey = ShardKeys.documentKeyOf(document, List.of("address.zip"));

        Assertions.assertThat(documentKey).isEqualTo(new BsonDocument("_id", ID));
    }

    @Test
    void shouldSkipNestedShardKeyWhoseParentIsNotADocument() {
        var document = new BsonDocument("_id", ID)
                .append("address", new BsonString("not a document"));

        var documentKey = ShardKeys.documentKeyOf(document, List.of("address.zip"));

        Assertions.assertThat(documentKey).isEqualTo(new BsonDocument("_id", ID));
    }

    @Test
    void shouldReadShardKeyOrderFromConfigEntry() {
        var entry = new Document("key", new Document("tenant", 1).append("region", "hashed"));

        Assertions.assertThat(ShardKeys.shardKeyPathsOf(entry, COLLECTION)).containsExactly("tenant", "region");
    }

    @Test
    void shouldTreatMissingConfigEntryAsUnsharded() {
        Assertions.assertThat(ShardKeys.shardKeyPathsOf(null, COLLECTION)).isEmpty();
    }

    @Test
    void shouldFailWhenConfigEntryHasNoShardKey() {
        // Falling back to _id here would not match the documentKey that the change stream reports for the same document
        Assertions.assertThatThrownBy(() -> ShardKeys.shardKeyPathsOf(new Document(), COLLECTION))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("no usable shard key");
    }

    @Test
    void shouldFailWhenConfigEntryHasEmptyShardKey() {
        var entry = new Document("key", new Document());

        Assertions.assertThatThrownBy(() -> ShardKeys.shardKeyPathsOf(entry, COLLECTION))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("no usable shard key");
    }

    @Test
    void shouldFailWhenConfigEntryShardKeyIsNotADocument() {
        var entry = new Document("key", "tenant");

        Assertions.assertThatThrownBy(() -> ShardKeys.shardKeyPathsOf(entry, COLLECTION))
                .isInstanceOf(DebeziumException.class)
                .hasMessageContaining("no usable shard key");
    }

    @Test
    void shouldTreatEveryCollectionAsUnshardedWithoutConnection() {
        Assertions.assertThat(ShardKeys.unsharded().shardKeyPathsFor(COLLECTION)).isEmpty();
    }
}

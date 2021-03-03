package org.schambon.mongodb;

import com.mongodb.*;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.InsertManyOptions;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.jsr310.Jsr310CodecProvider;
import org.bson.codecs.pojo.PojoCodecProvider;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.client.model.Accumulators.sum;
import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Indexes.ascending;
import static com.mongodb.client.model.Projections.exclude;
import static com.mongodb.client.model.Updates.set;
import static java.time.Instant.now;

public class Demo {

    public static void main(String[] args) {

        // Codec Registry : configuration du mapping des classes Java en documents BSON
        var codecRegistry = CodecRegistries.fromRegistries(
                MongoClientSettings.getDefaultCodecRegistry(),
                CodecRegistries.fromProviders(
                        PojoCodecProvider.builder().automatic(true).build()));  // gestion des POJO (Java Beans avec setters et getters)

        var client = MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString("mongodb+srv://URL A REMPLACER/"))
                .writeConcern(WriteConcern.MAJORITY.withWTimeout(5, TimeUnit.SECONDS))
                .readConcern(ReadConcern.MAJORITY)
                .retryWrites(true)
                .retryReads(true)
                .compressorList(Arrays.asList(MongoCompressor.createSnappyCompressor()))
                .codecRegistry(codecRegistry)
                .build()
        );



        var collection = client.getDatabase("test").getCollection("test", Person.class);

        // drop collection
        collection.drop();

        // Insert Many
        List<Person> personList = new ArrayList();
        for (var i = 0; i < 1000; i++) {
            personList.add(new Person("Dupont", i, now()));
        }
        collection.insertMany(personList, new InsertManyOptions().ordered(false));

        // Lecture des 20 premiers documents
        for (Person p : collection.find().skip(0).limit(20).maxTime(10, TimeUnit.SECONDS)) {
            System.out.println(p.toString());
        }

        // Lecture de tous les enregistrements - pas de pagination côté applicatif
        var cumulativeAge = 0;
        for (Person p : collection.find()) {
            cumulativeAge+=p.getAge();
        }
        System.out.println(String.format("Total age: %d", cumulativeAge));

        // agrégation
        Document aggregateResult = collection.aggregate(Arrays.asList(group(null, sum("cumulativeAge", "$age"))), Document.class).first();
        System.out.println(String.format("Total age: %d", aggregateResult.getInteger("cumulativeAge")));

        // création d'index
        collection.createIndex(ascending("age"));

        // update
        collection.updateOne(eq("age", 5), set("dateOfBirth", now().minus(5*365, ChronoUnit.DAYS)));

        // projection
        System.out.println(collection.find(eq("age", 5)).projection(exclude("age")).first());

        // delete
        var deleteResult = collection.deleteMany(gt("age", 5));
        System.out.println(String.format("Deleted %d", deleteResult.getDeletedCount()));

        // count
        System.out.println(String.format("Remaining %d", collection.countDocuments()));

        // transaction
        var session = client.startSession();
        var docsAfterTransaction = session.withTransaction(() -> {
            collection.insertOne(session, new Person("Durand", 30, now().minus(30*365, ChronoUnit.DAYS)));
            collection.deleteMany(session, eq("name", "Dupont"));
            System.out.println(String.format("Out of transaction, there are %d records", collection.countDocuments())); // si on ne passe pas la session, on n'est pas dans la transaction
            return collection.countDocuments(session);
        }, TransactionOptions.builder()
                .readConcern(ReadConcern.SNAPSHOT)
                .maxCommitTime(10l, TimeUnit.SECONDS).build());
        session.close();
        System.out.println(String.format("Remaining after transaction: %d", docsAfterTransaction));

    }
}

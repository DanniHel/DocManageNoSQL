package org.example.config;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class MongoConfig {
    private static MongoClient mongoClient;
    private static String databaseName;

    public static MongoClient getMongoClient() {
        if (mongoClient == null) {
            Properties properties = loadProperties();
            String connectionString = properties.getProperty("mongodb.connection.string");
            databaseName = properties.getProperty("mongodb.database.name");

            CodecRegistry pojoCodecRegistry = fromRegistries(
                    MongoClientSettings.getDefaultCodecRegistry(),
                    fromProviders(PojoCodecProvider.builder().automatic(true).build())
            );

            mongoClient = MongoClients.create(connectionString);
        }
        return mongoClient;
    }

    public static String getDatabaseName() {
        if (databaseName == null) {
            loadProperties();
        }
        return databaseName;
    }

    private static Properties loadProperties() {
        Properties properties = new Properties();
        try (InputStream input = MongoConfig.class.getClassLoader()
                .getResourceAsStream("mongodb.properties")) {
            if (input == null) {
                throw new RuntimeException("No se pudo encontrar el archivo mongodb.properties");
            }
            properties.load(input);
        } catch (IOException e) {
            throw new RuntimeException("Error al cargar las propiedades de MongoDB", e);
        }
        return properties;
    }

    public static void closeMongoClient() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }
}
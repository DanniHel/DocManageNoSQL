package org.example.repository;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.example.config.MongoConfig;
import org.example.model.Documento;

import java.time.LocalDateTime;
import java.util.*;

public class DocumentoRepository {
    private final MongoCollection<Document> collection;

    public DocumentoRepository() {
        MongoDatabase database = MongoConfig.getMongoClient().getDatabase(MongoConfig.getDatabaseName());
        this.collection = database.getCollection("documentos");

        // Crear índices compuestos
        collection.createIndex(Indexes.compoundIndex(
                Indexes.ascending("tipoDocumento"),
                Indexes.descending("fechaCreacion"),
                Indexes.ascending("autor")
        ));
    }

    public void guardarDocumento(Documento documento) {
        Document doc = new Document("titulo", documento.getTitulo())
                .append("autor", documento.getAutor())
                .append("tipoDocumento", documento.getTipoDocumento())
                .append("fechaCreacion", documento.convertirLocalDateTimeADate(documento.getFechaCreacion()))
                .append("fechaModificacion", documento.convertirLocalDateTimeADate(documento.getFechaModificacion()))
                .append("estado", documento.getEstado())
                .append("version", documento.getVersion());

        collection.insertOne(doc);
        documento.setId(doc.getObjectId("_id")); // Asignar el ID generado
    }

    public List<Documento> obtenerDocumentosPorAutor(String autor) {
        List<Documento> documentos = new ArrayList<>();
        collection.find(Filters.eq("autor", autor)).forEach(document -> {
            Documento doc = new Documento();
            doc.setId(document.getObjectId("_id"));
            doc.setTitulo(document.getString("titulo"));
            doc.setAutor(document.getString("autor"));
            doc.setTipoDocumento(document.getString("tipoDocumento"));

            // Manejo seguro de la conversión de fechas
            doc.setFechaCreacion(convertirDateALocalDateTime(document.get("fechaCreacion", java.util.Date.class)));
            doc.setFechaModificacion(convertirDateALocalDateTime(document.get("fechaModificacion", java.util.Date.class)));
            doc.setEstado(document.getString("estado"));
            doc.setVersion(document.getInteger("version", 0));

            documentos.add(doc);
        });
        return documentos;
    }

    public List<Documento> obtenerDocumentosPorTipo(String tipoDocumento) {
        List<Documento> documentos = new ArrayList<>();
        collection.find(Filters.eq("tipoDocumento", tipoDocumento)).forEach(document -> {
            Documento doc = new Documento();
            doc.setId(document.getObjectId("_id"));
            doc.setTitulo(document.getString("titulo"));
            doc.setAutor(document.getString("autor"));
            doc.setTipoDocumento(document.getString("tipoDocumento"));
            doc.setFechaCreacion(convertirDateALocalDateTime(document.get("fechaCreacion", java.util.Date.class)));
            doc.setFechaModificacion(convertirDateALocalDateTime(document.get("fechaModificacion", java.util.Date.class)));
            doc.setEstado(document.getString("estado"));
            doc.setVersion(document.getInteger("version", 0));
            documentos.add(doc);
        });
        return documentos;
    }

    public List<Documento> obtenerTodosLosDocumentos() {
        List<Documento> documentos = new ArrayList<>();
        collection.find().forEach(document -> {
            Documento doc = new Documento();
            doc.setId(document.getObjectId("_id"));
            doc.setTitulo(document.getString("titulo"));
            doc.setAutor(document.getString("autor"));
            doc.setTipoDocumento(document.getString("tipoDocumento"));
            doc.setFechaCreacion(convertirDateALocalDateTime(document.get("fechaCreacion", java.util.Date.class)));
            doc.setFechaModificacion(convertirDateALocalDateTime(document.get("fechaModificacion", java.util.Date.class)));
            doc.setEstado(document.getString("estado"));
            doc.setVersion(document.getInteger("version", 0));
            documentos.add(doc);
        });
        return documentos;
    }

    public Documento obtenerDocumentoPorId(String id) {
        try {
            ObjectId objectId = new ObjectId(id);
            Document document = collection.find(Filters.eq("_id", objectId)).first();
            if (document == null) {
                return null;
            }

            Documento doc = new Documento();
            doc.setId(document.getObjectId("_id"));
            doc.setTitulo(document.getString("titulo"));
            doc.setAutor(document.getString("autor"));
            doc.setTipoDocumento(document.getString("tipoDocumento"));
            doc.setFechaCreacion(Documento.convertirDateALocalDateTime(document.get("fechaCreacion", java.util.Date.class)));
            doc.setFechaModificacion(Documento.convertirDateALocalDateTime(document.get("fechaModificacion", java.util.Date.class)));
            doc.setEstado(document.getString("estado"));
            doc.setVersion(document.getInteger("version", 0));
            return doc;
        } catch (IllegalArgumentException e) {
            return null; // ID inválido
        }
    }

    public boolean actualizarDocumento(String id, Documento documentoActualizado) {
        try {
            ObjectId objectId = new ObjectId(id);
            UpdateResult result = collection.updateOne(
                    Filters.eq("_id", objectId),
                    Updates.combine(
                            Updates.set("titulo", documentoActualizado.getTitulo()),
                            Updates.set("autor", documentoActualizado.getAutor()),
                            Updates.set("tipoDocumento", documentoActualizado.getTipoDocumento()),
                            Updates.set("fechaModificacion", Documento.convertirLocalDateTimeADate(LocalDateTime.now())),
                            Updates.set("estado", documentoActualizado.getEstado()),
                            Updates.set("version", documentoActualizado.getVersion() + 1) // Incrementar versión
                    )
            );
            return result.getModifiedCount() > 0;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    public boolean eliminarDocumento(String id) {
        try {
            ObjectId objectId = new ObjectId(id);
            DeleteResult result = collection.deleteOne(Filters.eq("_id", objectId));
            return result.getDeletedCount() > 0;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    private LocalDateTime convertirDateALocalDateTime(java.util.Date date) {
        return Documento.convertirDateALocalDateTime(date);
    }
}
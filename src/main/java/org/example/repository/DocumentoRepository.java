package org.example.repository;

import com.mongodb.client.*;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.model.*;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.example.config.MongoConfig;
import org.example.model.Documento;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Sorts;
import org.bson.conversions.Bson;
import org.bson.types.BSONTimestamp;


import java.io.FileInputStream;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.util.*;


public class DocumentoRepository {
    private final MongoCollection<Document> collection;
    private final GridFSBucket gridFSBucket; // <--- NUEVO: Para archivos grandes

    public DocumentoRepository() {
        MongoDatabase database = MongoConfig.getMongoClient().getDatabase(MongoConfig.getDatabaseName());
        this.collection = database.getCollection("documentos");

        // Inicializar GridFS
        this.gridFSBucket = GridFSBuckets.create(database, "archivos"); // <--- NUEVO

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

    // --- MÉTODOS DE BÚSQUEDA (Sin cambios) ---
    public List<Documento> obtenerDocumentosPorAutor(String autor) {
        return mapearDocumentos(Filters.eq("autor", autor));
    }

    public List<Documento> obtenerDocumentosPorTipo(String tipoDocumento) {
        return mapearDocumentos(Filters.eq("tipoDocumento", tipoDocumento));
    }

    public List<Documento> obtenerTodosLosDocumentos() {
        return mapearDocumentos(new Document()); // Busca todo
    }

    // Método auxiliar para evitar repetir código en las búsquedas
    private List<Documento> mapearDocumentos(org.bson.conversions.Bson filtro) {
        List<Documento> documentos = new ArrayList<>();
        collection.find(filtro).forEach(document -> {
            Documento doc = new Documento();
            doc.setId(document.getObjectId("_id"));
            doc.setTitulo(document.getString("titulo"));
            doc.setAutor(document.getString("autor"));
            doc.setTipoDocumento(document.getString("tipoDocumento"));
            doc.setFechaCreacion(convertirDateALocalDateTime(document.get("fechaCreacion", java.util.Date.class)));
            doc.setFechaModificacion(convertirDateALocalDateTime(document.get("fechaModificacion", java.util.Date.class)));
            doc.setEstado(document.getString("estado"));
            doc.setVersion(document.getInteger("version", 1));
            documentos.add(doc);
        });
        return documentos;
    }
    // -----------------------------------------

    public Documento obtenerDocumentoPorId(String id) {
        try {
            ObjectId objectId = new ObjectId(id);
            Document document = collection.find(Filters.eq("_id", objectId)).first();
            if (document == null) return null;

            Documento doc = new Documento();
            doc.setId(document.getObjectId("_id"));
            doc.setTitulo(document.getString("titulo"));
            doc.setAutor(document.getString("autor"));
            doc.setTipoDocumento(document.getString("tipoDocumento"));
            doc.setFechaCreacion(Documento.convertirDateALocalDateTime(document.get("fechaCreacion", java.util.Date.class)));
            doc.setFechaModificacion(Documento.convertirDateALocalDateTime(document.get("fechaModificacion", java.util.Date.class)));
            doc.setEstado(document.getString("estado"));
            doc.setVersion(document.getInteger("version", 1));
            return doc;
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    // REQUISITO 5: ACTUALIZAR CON CONTROL DE CONCURRENCIA (Optimistic Locking)
    public boolean actualizarDocumento(String id, Documento documentoActualizado, int versionActual) {
        try {
            ObjectId objectId = new ObjectId(id);

            // Filtro: Coincide ID *Y* la Versión que el usuario leyó
            UpdateResult result = collection.updateOne(
                    Filters.and(
                            Filters.eq("_id", objectId),
                            Filters.eq("version", versionActual) // Verificación de concurrencia
                    ),
                    Updates.combine(
                            Updates.set("titulo", documentoActualizado.getTitulo()),
                            Updates.set("autor", documentoActualizado.getAutor()),
                            Updates.set("tipoDocumento", documentoActualizado.getTipoDocumento()),
                            Updates.set("fechaModificacion", Documento.convertirLocalDateTimeADate(LocalDateTime.now())),
                            Updates.set("estado", documentoActualizado.getEstado()),
                            Updates.inc("version", 1) // Incrementa la versión en BD automáticamente
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

    // REQUISITO 4: GRIDFS (Subir Archivos)
    public ObjectId subirArchivo(String rutaArchivo, String nombreArchivo) {
        try (InputStream streamToUploadFrom = new FileInputStream(rutaArchivo)) {
            System.out.println("Subiendo archivo a GridFS...");
            return gridFSBucket.uploadFromStream(nombreArchivo, streamToUploadFrom);
        } catch (Exception e) {
            System.err.println("Error al subir archivo: " + e.getMessage());
            return null;
        }
    }

    // REQUISITO 1: TRANSACCIONES ACID (Workflow de Aprobación)
    public void aprobarDocumentoConTransaccion(String idDoc) {
        // Obtenemos la sesión del cliente
        try (ClientSession session = MongoConfig.getMongoClient().startSession()) {

            // Ejecutamos la transacción
            session.withTransaction(new TransactionBody<Void>() {
                @Override
                public Void execute() {
                    ObjectId docId = new ObjectId(idDoc);

                    // Paso 1: Actualizar estado del documento
                    UpdateResult res = collection.updateOne(session,
                            Filters.eq("_id", docId),
                            Updates.set("estado", "APROBADO")
                    );

                    if (res.getModifiedCount() == 0) {
                        System.out.println("No se encontró el documento o ya estaba aprobado.");
                        return null;
                    }

                    // Paso 2: Registrar en auditoría (otra colección)
                    MongoDatabase db = MongoConfig.getMongoClient().getDatabase(MongoConfig.getDatabaseName());
                    db.getCollection("auditoria_aprobaciones").insertOne(session,
                            new Document("docId", docId)
                                    .append("fechaAprobacion", new Date())
                                    .append("accion", "APROBADO_GERENCIA")
                                    .append("usuario", "admin")
                    );

                    System.out.println("¡Transacción completada! Documento aprobado y auditado.");
                    return null;
                }
            });
        } catch (Exception e) {
            System.err.println("La transacción falló (Rollback automático): " + e.getMessage());
        }
    }

    private LocalDateTime convertirDateALocalDateTime(java.util.Date date) {
        return Documento.convertirDateALocalDateTime(date);
    }

    public List<Document> obtenerUltimasOperacionesOplog(int limite) {
        MongoDatabase localDb = MongoConfig.getMongoClient().getDatabase("local");
        MongoCollection<Document> oplog = localDb.getCollection("oplog.rs");

        // Filtro: Solo operaciones en la base de datos 'docmanage' y colección 'documentos'
        Bson filtro = Filters.eq("ns", MongoConfig.getDatabaseName() + ".documentos");

        return oplog.find(filtro)
                .sort(Sorts.descending("ts"))
                .limit(limite)
                .into(new ArrayList<>());
    }



    public int aplicarRecuperacionOplog(List<Document> operaciones) {
        int aplicadas = 0;
        MongoCollection<Document> collection = MongoConfig.getMongoClient()
                .getDatabase(MongoConfig.getDatabaseName())
                .getCollection("documentos");

        for (Document op : operaciones) {
            String operacion = op.getString("op");
            try {
                switch (operacion) {
                    case "i": // Insert
                        Document docToInsert = op.get("o", Document.class);
                        collection.insertOne(docToInsert);
                        aplicadas++;
                        break;

                    case "u": // Update
                        Document o2 = op.get("o2", Document.class); // Filtro original (normalmente {_id: ...})
                        Document updateDoc = op.get("o", Document.class);
                        if (o2 != null && o2.containsKey("_id")) {
                            Object id = o2.get("_id");
                            // Si "o" tiene $set, $inc, etc., usar updateOne; si es documento completo, replace
                            if (updateDoc.containsKey("$set") || updateDoc.containsKey("$inc") || updateDoc.containsKey("$unset")) {
                                collection.updateOne(Filters.eq("_id", id), updateDoc);
                            } else {
                                collection.replaceOne(Filters.eq("_id", id), updateDoc);
                            }
                            aplicadas++;
                        }
                        break;

                    case "d": // Delete
                        Document deleteFilter = op.get("o", Document.class);
                        if (deleteFilter != null && deleteFilter.containsKey("_id")) {
                            collection.deleteOne(Filters.eq("_id", deleteFilter.get("_id")));
                            aplicadas++;
                        }
                        break;
                }
            } catch (Exception e) {
                System.err.println("Error aplicando operación oplog: " + op.toJson() + " -> " + e.getMessage());
            }
        }
        return aplicadas;
    }

    public List<Document> obtenerOperacionesOplogDesde(org.bson.BsonTimestamp desdeTs, int limiteSiNoHayTs) {
        MongoDatabase localDb = MongoConfig.getMongoClient().getDatabase("local");
        MongoCollection<Document> oplog = localDb.getCollection("oplog.rs");

        Bson filtro = Filters.eq("ns", MongoConfig.getDatabaseName() + ".documentos");

        if (desdeTs != null) {
            filtro = Filters.and(filtro, Filters.gt("ts", desdeTs));
        }

        FindIterable<Document> query = oplog.find(filtro)
                .sort(Sorts.ascending("ts"));

        if (desdeTs == null) {
            query = query.limit(limiteSiNoHayTs);
        }

        return query.into(new ArrayList<>());
    }
}
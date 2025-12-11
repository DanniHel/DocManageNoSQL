package org.example.repository;

import com.mongodb.client.*;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.model.*;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.example.config.MongoConfig;
import org.example.model.Documento;

import java.io.FileInputStream;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

// Imports para oplog
import org.bson.conversions.Bson;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Filters;

public class DocumentoRepository {
    private final MongoCollection<Document> collection;
    private final GridFSBucket gridFSBucket;

    public DocumentoRepository() {
        MongoDatabase database = MongoConfig.getMongoClient().getDatabase(MongoConfig.getDatabaseName());
        this.collection = database.getCollection("documentos");
        this.gridFSBucket = GridFSBuckets.create(database, "archivos");

        // Índices
        collection.createIndex(Indexes.compoundIndex(
                Indexes.ascending("tipoDocumento"),
                Indexes.descending("fechaCreacion"),
                Indexes.ascending("autor")
        ));
    }

    // === CREACIÓN ===
    public void guardarDocumento(Documento documento) {
        guardarDocumentoConArchivo(documento, null, null);
    }

    public void guardarDocumentoConArchivo(Documento documento, String rutaArchivo, String nombreArchivo) {
        ObjectId archivoId = null;

        if (rutaArchivo != null && !rutaArchivo.isBlank()) {
            try (InputStream stream = new FileInputStream(rutaArchivo)) {
                String nombre = nombreArchivo != null && !nombreArchivo.isBlank()
                        ? nombreArchivo : new java.io.File(rutaArchivo).getName();
                archivoId = gridFSBucket.uploadFromStream(nombre, stream);
            } catch (Exception e) {
                System.err.println("Error al subir archivo: " + e.getMessage());
                throw new RuntimeException("No se pudo subir el archivo", e);
            }
        }

        Document doc = new Document("titulo", documento.getTitulo())
                .append("autor", documento.getAutor())
                .append("tipoDocumento", documento.getTipoDocumento())
                .append("fechaCreacion", Documento.convertirLocalDateTimeADate(documento.getFechaCreacion()))
                .append("fechaModificacion", Documento.convertirLocalDateTimeADate(documento.getFechaModificacion()))
                .append("estado", documento.getEstado())
                .append("version", documento.getVersion())
                .append("archivoId", archivoId);

        collection.insertOne(doc);
        documento.setId(doc.getObjectId("_id"));
        documento.setArchivoId(archivoId);
    }

    // === LECTURA ===
    private List<Documento> mapearDocumentos(Bson filtro) {
        List<Documento> documentos = new ArrayList<>();
        collection.find(filtro).forEach(document -> {
            Documento doc = new Documento();
            doc.setId(document.getObjectId("_id"));
            doc.setTitulo(document.getString("titulo"));
            doc.setAutor(document.getString("autor"));
            doc.setTipoDocumento(document.getString("tipoDocumento"));
            doc.setFechaCreacion(convertirDateALocalDateTime(document.get("fechaCreacion", Date.class)));
            doc.setFechaModificacion(convertirDateALocalDateTime(document.get("fechaModificacion", Date.class)));
            doc.setEstado(document.getString("estado"));
            doc.setVersion(document.getInteger("version", 1));
            doc.setArchivoId(document.getObjectId("archivoId"));
            documentos.add(doc);
        });
        return documentos;
    }

    public List<Documento> obtenerDocumentosPorAutor(String autor) {
        return mapearDocumentos(Filters.eq("autor", autor));
    }

    public List<Documento> obtenerDocumentosPorTipo(String tipoDocumento) {
        return mapearDocumentos(Filters.eq("tipoDocumento", tipoDocumento));
    }

    public List<Documento> obtenerTodosLosDocumentos() {
        return mapearDocumentos(new Document());
    }

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
            doc.setFechaCreacion(Documento.convertirDateALocalDateTime(document.get("fechaCreacion", Date.class)));
            doc.setFechaModificacion(Documento.convertirDateALocalDateTime(document.get("fechaModificacion", Date.class))); // ← CORREGIDO
            doc.setEstado(document.getString("estado"));
            doc.setVersion(document.getInteger("version", 1));
            doc.setArchivoId(document.getObjectId("archivoId"));
            return doc;
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    // === ACTUALIZACIÓN (compatibilidad con menú actual) ===
    public boolean actualizarDocumento(String id, Documento documentoActualizado, int versionActual) {
        return actualizarDocumentoConArchivo(id, documentoActualizado, versionActual, null, null);
    }

    // Nueva versión con soporte para cambiar archivo
    public boolean actualizarDocumentoConArchivo(String id, Documento documentoActualizado, int versionActual,
                                                 String nuevaRutaArchivo, String nuevoNombreArchivo) {
        try {
            ObjectId objectId = new ObjectId(id);

            // Obtener documento actual para borrar archivo antiguo si se reemplaza
            Document docActual = collection.find(Filters.eq("_id", objectId)).first();
            ObjectId archivoIdAntiguo = docActual != null ? docActual.getObjectId("archivoId") : null;

            ObjectId nuevoArchivoId = null;
            if (nuevaRutaArchivo != null && !nuevaRutaArchivo.isBlank()) {
                try (InputStream stream = new FileInputStream(nuevaRutaArchivo)) {
                    String nombre = nuevoNombreArchivo != null && !nuevoNombreArchivo.isBlank()
                            ? nuevoNombreArchivo : new java.io.File(nuevaRutaArchivo).getName();
                    nuevoArchivoId = gridFSBucket.uploadFromStream(nombre, stream);
                } catch (Exception e) {
                    System.err.println("Error al subir nuevo archivo: " + e.getMessage());
                }
            }

            UpdateResult result = collection.updateOne(
                    Filters.and(Filters.eq("_id", objectId), Filters.eq("version", versionActual)),
                    Updates.combine(
                            Updates.set("titulo", documentoActualizado.getTitulo()),
                            Updates.set("autor", documentoActualizado.getAutor()),
                            Updates.set("tipoDocumento", documentoActualizado.getTipoDocumento()),
                            Updates.set("fechaModificacion", Documento.convertirLocalDateTimeADate(LocalDateTime.now())),
                            Updates.set("estado", documentoActualizado.getEstado()),
                            Updates.set("archivoId", nuevoArchivoId != null ? nuevoArchivoId : archivoIdAntiguo),
                            Updates.inc("version", 1)
                    )
            );

            // Borrar archivo antiguo si fue reemplazado
            if (result.getModifiedCount() > 0 && nuevoArchivoId != null && archivoIdAntiguo != null) {
                try {
                    gridFSBucket.delete(archivoIdAntiguo);
                } catch (Exception e) {
                    System.err.println("No se pudo borrar archivo antiguo: " + archivoIdAntiguo);
                }
            }

            return result.getModifiedCount() > 0;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    // === ELIMINACIÓN ===
    public boolean eliminarDocumento(String id) {
        try {
            ObjectId objectId = new ObjectId(id);
            Document doc = collection.find(Filters.eq("_id", objectId)).first();
            if (doc == null) return false;

            ObjectId archivoId = doc.getObjectId("archivoId");
            if (archivoId != null) {
                gridFSBucket.delete(archivoId);
            }

            DeleteResult result = collection.deleteOne(Filters.eq("_id", objectId));
            return result.getDeletedCount() > 0;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    // === TRANSACCIÓN APROBACIÓN ===
    public void aprobarDocumentoConTransaccion(String idDoc) {
        try (ClientSession session = MongoConfig.getMongoClient().startSession()) {
            session.withTransaction(() -> {
                ObjectId docId = new ObjectId(idDoc);
                UpdateResult res = collection.updateOne(session,
                        Filters.eq("_id", docId),
                        Updates.set("estado", "APROBADO"));

                if (res.getModifiedCount() == 0) {
                    System.out.println("No se encontró el documento o ya estaba aprobado.");
                    return null;
                }

                MongoDatabase db = MongoConfig.getMongoClient().getDatabase(MongoConfig.getDatabaseName());
                db.getCollection("auditoria_aprobaciones").insertOne(session,
                        new Document("docId", docId)
                                .append("fechaAprobacion", new Date())
                                .append("accion", "APROBADO_GERENCIA")
                                .append("usuario", "admin"));

                System.out.println("¡Transacción completada! Documento aprobado y auditado.");
                return null;
            });
        } catch (Exception e) {
            System.err.println("La transacción falló (Rollback automático): " + e.getMessage());
        }
    }

    private LocalDateTime convertirDateALocalDateTime(java.util.Date date) {
        return Documento.convertirDateALocalDateTime(date);
    }

    // === OPLOG (sin cambios, ya estaban bien) ===
    public List<Document> obtenerUltimasOperacionesOplog(int limite) {
        MongoDatabase localDb = MongoConfig.getMongoClient().getDatabase("local");
        MongoCollection<Document> oplog = localDb.getCollection("oplog.rs");
        Bson filtro = Filters.eq("ns", MongoConfig.getDatabaseName() + ".documentos");
        return oplog.find(filtro)
                .sort(Sorts.descending("ts"))
                .limit(limite)
                .into(new ArrayList<>());
    }

    public List<Document> obtenerOperacionesOplogDesde(BsonTimestamp desdeTs, int limiteSiNoHayTs) {
        MongoDatabase localDb = MongoConfig.getMongoClient().getDatabase("local");
        MongoCollection<Document> oplog = localDb.getCollection("oplog.rs");
        Bson filtro = Filters.eq("ns", MongoConfig.getDatabaseName() + ".documentos");
        if (desdeTs != null) {
            filtro = Filters.and(filtro, Filters.gt("ts", desdeTs));
        }
        FindIterable<Document> query = oplog.find(filtro).sort(Sorts.ascending("ts"));
        if (desdeTs == null) query = query.limit(limiteSiNoHayTs);
        return query.into(new ArrayList<>());
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
                        ObjectId idInsert = docToInsert.getObjectId("_id");
                        if (collection.find(Filters.eq("_id", idInsert)).first() == null) {
                            collection.insertOne(docToInsert);
                            aplicadas++;
                            System.out.println("Recuperado INSERT: " + idInsert);
                        } else {
                            System.out.println("Ya existe documento con ID " + idInsert + " → omitido");
                        }
                        break;

                    case "u": // Update - Manejo definitivo de todos los formatos diff
                        Document o2 = op.get("o2", Document.class);
                        if (o2 == null || !o2.containsKey("_id")) break;

                        ObjectId idUpdate = o2.getObjectId("_id");
                        Document updateObj = op.get("o", Document.class);

                        Document setFields = new Document();
                        Document unsetFields = new Document();
                        AtomicBoolean hasChanges = new AtomicBoolean(false);

                        if (updateObj.containsKey("$v") || updateObj.containsKey("diff")) {
                            Document diff = updateObj.get("diff", Document.class);
                            if (diff != null) {
                                diff.forEach((key, value) -> {
                                    if (key.equals("u") && value instanceof Document subDoc) {
                                        // Formato agrupado: u = {autor: "...", titulo: "...", version: 4}
                                        subDoc.forEach((subKey, subValue) -> {
                                            setFields.append(subKey, subValue);
                                            hasChanges.set(true);
                                        });
                                    } else if (key.startsWith("u.") && key.length() > 2) {
                                        // Formato individual: "u.autor": "Danny"
                                        String field = key.substring(2);
                                        setFields.append(field, value);
                                        hasChanges.set(true);
                                    } else if (key.startsWith("i")) {
                                        String field = key.substring(1);
                                        setFields.append(field, value);
                                        hasChanges.set(true);
                                    } else if (key.startsWith("d")) {
                                        String field = key.substring(1);
                                        unsetFields.append(field, 1);
                                        hasChanges.set(true);
                                    }
                                });
                            }
                        } else {
                            collection.replaceOne(Filters.eq("_id", idUpdate), updateObj);
                            aplicadas++;
                            System.out.println("Recuperado UPDATE completo: " + idUpdate);
                            continue;
                        }

                        Document updateCommand = new Document();
                        if (!setFields.isEmpty()) updateCommand.append("$set", setFields);
                        if (!unsetFields.isEmpty()) updateCommand.append("$unset", unsetFields);

                        if (hasChanges.get() && !updateCommand.isEmpty()) {
                            collection.updateOne(Filters.eq("_id", idUpdate), updateCommand);
                            aplicadas++;
                            System.out.println("Recuperado UPDATE parcial: " + idUpdate);
                        } else {
                            System.out.println("UPDATE sin cambios válidos → omitido (ID: " + idUpdate + ")");
                        }
                        break;

                    case "d": // Delete
                        Document deleteFilter = op.get("o", Document.class);
                        if (deleteFilter != null && deleteFilter.containsKey("_id")) {
                            ObjectId idDelete = deleteFilter.getObjectId("_id");
                            collection.deleteOne(Filters.eq("_id", idDelete));
                            aplicadas++;
                            System.out.println("Recuperado DELETE: " + idDelete);
                        }
                        break;
                }
            } catch (Exception e) {
                System.err.println("Error aplicando operación oplog: " + e.getMessage());
            }
        }
        return aplicadas;
    }

    public long simularDesastre() {
        return collection.deleteMany(new Document()).getDeletedCount();
    }

    public GridFSBucket getGridFSBucket() {
        return gridFSBucket;
    }
}
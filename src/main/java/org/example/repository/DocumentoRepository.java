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

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Sorts.descending;

// Importaciones para operaciones con oplog en réplica sets
import org.bson.conversions.Bson;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Filters;

// Repositorio principal para gestión documental con MongoDB
// Implementa transacciones ACID multi-documento, versionamiento y recuperación ante fallos
public class DocumentoRepository {
    private final MongoCollection<Document> collection; // Colección principal de documentos
    private final GridFSBucket gridFSBucket; // Bucket GridFS para archivos grandes

    // Constructor: inicializa conexión, colección y configura índices compuestos
    public DocumentoRepository() {
        MongoDatabase database = MongoConfig.getMongoClient().getDatabase(MongoConfig.getDatabaseName());
        this.collection = database.getCollection("documentos");
        this.gridFSBucket = GridFSBuckets.create(database, "archivos");

        // Creación de índice compuesto para optimizar búsquedas frecuentes
        collection.createIndex(Indexes.compoundIndex(
                Indexes.ascending("tipoDocumento"),
                Indexes.descending("fechaCreacion"),
                Indexes.ascending("autor")
        ));
        System.out.println("Índice compuesto creado: tipoDocumento, fechaCreacion, autor");
    }

    // === OPERACIONES CRUD BÁSICAS ===

    // Guarda un documento sin archivo adjunto
    public void guardarDocumento(Documento documento) {
        guardarDocumentoConArchivo(documento, null, null);
    }

    // Guarda un documento con archivo adjunto en GridFS
    public void guardarDocumentoConArchivo(Documento documento, String rutaArchivo, String nombreArchivo) {
        ObjectId archivoId = null;

        // Procesar archivo adjunto si se proporciona ruta
        if (rutaArchivo != null && !rutaArchivo.isBlank()) {
            try (InputStream stream = new FileInputStream(rutaArchivo)) {
                String nombre = nombreArchivo != null && !nombreArchivo.isBlank()
                        ? nombreArchivo : new java.io.File(rutaArchivo).getName();
                archivoId = gridFSBucket.uploadFromStream(nombre, stream);
                System.out.println("Archivo subido a GridFS con ID: " + archivoId);
            } catch (Exception e) {
                System.err.println("Error crítico al subir archivo: " + e.getMessage());
                throw new RuntimeException("Fallo en almacenamiento de archivo", e);
            }
        }

        // Crear documento BSON para inserción en MongoDB
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
        System.out.println("Documento guardado con ID: " + documento.getId());
    }

    // === CONSULTAS Y BÚSQUEDAS ===

    // Metodo auxiliar para mapear documentos BSON a objetos del dominio
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

    // Consulta documentos por autor específico
    public List<Documento> obtenerDocumentosPorAutor(String autor) {
        System.out.println("Consultando documentos del autor: " + autor);
        return mapearDocumentos(Filters.eq("autor", autor));
    }

    // Consulta documentos por tipo específico
    public List<Documento> obtenerDocumentosPorTipo(String tipoDocumento) {
        System.out.println("Consultando documentos del tipo: " + tipoDocumento);
        return mapearDocumentos(Filters.eq("tipoDocumento", tipoDocumento));
    }

    // Obtiene todos los documentos del sistema
    public List<Documento> obtenerTodosLosDocumentos() {
        System.out.println("Obteniendo todos los documentos del repositorio");
        return mapearDocumentos(new Document());
    }

    // Búsqueda específica por identificador único
    public Documento obtenerDocumentoPorId(String id) {
        try {
            ObjectId objectId = new ObjectId(id);
            Document document = collection.find(Filters.eq("_id", objectId)).first();
            if (document == null) {
                System.out.println("Documento no encontrado con ID: " + id);
                return null;
            }

            Documento doc = new Documento();
            doc.setId(document.getObjectId("_id"));
            doc.setTitulo(document.getString("titulo"));
            doc.setAutor(document.getString("autor"));
            doc.setTipoDocumento(document.getString("tipoDocumento"));
            doc.setFechaCreacion(Documento.convertirDateALocalDateTime(document.get("fechaCreacion", Date.class)));
            doc.setFechaModificacion(Documento.convertirDateALocalDateTime(document.get("fechaModificacion", Date.class)));
            doc.setEstado(document.getString("estado"));
            doc.setVersion(document.getInteger("version", 1));
            doc.setArchivoId(document.getObjectId("archivoId"));
            System.out.println("Documento encontrado: " + doc.getTitulo());
            return doc;
        } catch (IllegalArgumentException e) {
            System.err.println("ID inválido proporcionado: " + id);
            return null;
        }
    }

    // === ACTUALIZACIÓN CON CONTROL DE CONCURRENCIA ===

    // Actualización básica con control de versiones
    public boolean actualizarDocumento(String id, Documento documentoActualizado, int versionActual) {
        return actualizarDocumentoConArchivo(id, documentoActualizado, versionActual, null, null);
    }

    // Actualización completa con soporte para reemplazo de archivos
    public boolean actualizarDocumentoConArchivo(String id, Documento documentoActualizado, int versionActual,
                                                 String nuevaRutaArchivo, String nuevoNombreArchivo) {
        try {
            ObjectId objectId = new ObjectId(id);

            // Obtener documento actual para gestión de archivos antiguos
            Document docActual = collection.find(Filters.eq("_id", objectId)).first();
            ObjectId archivoIdAntiguo = docActual != null ? docActual.getObjectId("archivoId") : null;

            ObjectId nuevoArchivoId = null;
            if (nuevaRutaArchivo != null && !nuevaRutaArchivo.isBlank()) {
                try (InputStream stream = new FileInputStream(nuevaRutaArchivo)) {
                    String nombre = nuevoNombreArchivo != null && !nuevoNombreArchivo.isBlank()
                            ? nuevoNombreArchivo : new java.io.File(nuevaRutaArchivo).getName();
                    nuevoArchivoId = gridFSBucket.uploadFromStream(nombre, stream);
                    System.out.println("Nuevo archivo subido con ID: " + nuevoArchivoId);
                } catch (Exception e) {
                    System.err.println("Error en carga de nuevo archivo: " + e.getMessage());
                }
            }

            // Actualización con condición de versión para control de concurrencia
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

            // Eliminación segura de archivo antiguo si fue reemplazado
            if (result.getModifiedCount() > 0 && nuevoArchivoId != null && archivoIdAntiguo != null) {
                try {
                    gridFSBucket.delete(archivoIdAntiguo);
                    System.out.println("Archivo antiguo eliminado: " + archivoIdAntiguo);
                } catch (Exception e) {
                    System.err.println("Advertencia: No se pudo eliminar archivo antiguo: " + archivoIdAntiguo);
                }
            }

            boolean exito = result.getModifiedCount() > 0;
            System.out.println(exito ? "Actualización exitosa del documento: " + id :
                    "Fallo en actualización (posible conflicto de versión): " + id);
            return exito;
        } catch (IllegalArgumentException e) {
            System.err.println("Error: ID de documento inválido: " + id);
            return false;
        }
    }

    // === ELIMINACIÓN SEGURA ===

    // Elimina documento y sus archivos asociados
    public boolean eliminarDocumento(String id) {
        try {
            ObjectId objectId = new ObjectId(id);
            Document doc = collection.find(Filters.eq("_id", objectId)).first();
            if (doc == null) {
                System.out.println("Documento no existe para eliminación: " + id);
                return false;
            }

            ObjectId archivoId = doc.getObjectId("archivoId");
            if (archivoId != null) {
                gridFSBucket.delete(archivoId);
                System.out.println("Archivo asociado eliminado: " + archivoId);
            }

            DeleteResult result = collection.deleteOne(Filters.eq("_id", objectId));
            boolean exito = result.getDeletedCount() > 0;
            System.out.println(exito ? "Documento eliminado correctamente: " + id :
                    "Error en eliminación del documento: " + id);
            return exito;
        } catch (IllegalArgumentException e) {
            System.err.println("ID inválido para eliminación: " + id);
            return false;
        }
    }

    // === TRANSACCIONES ACID MULTI-DOCUMENTO ===

    // Implementación de transacción para workflow de aprobación
    public void aprobarDocumentoConTransaccion(String idDoc) {
        System.out.println("Iniciando transacción de aprobación para documento: " + idDoc);
        try (ClientSession session = MongoConfig.getMongoClient().startSession()) {
            session.withTransaction(() -> {
                ObjectId docId = new ObjectId(idDoc);

                // Fase 1: Actualización del estado del documento
                UpdateResult res = collection.updateOne(session,
                        Filters.eq("_id", docId),
                        Updates.set("estado", "APROBADO"));

                if (res.getModifiedCount() == 0) {
                    System.out.println("Transacción abortada: Documento no encontrado o ya aprobado.");
                    return null;
                }

                // Fase 2: Registro en auditoría (operación multi-documento)
                MongoDatabase db = MongoConfig.getMongoClient().getDatabase(MongoConfig.getDatabaseName());
                db.getCollection("auditoria_aprobaciones").insertOne(session,
                        new Document("docId", docId)
                                .append("fechaAprobacion", new Date())
                                .append("accion", "APROBADO_GERENCIA")
                                .append("usuario", "admin"));

                System.out.println("Transacción completada exitosamente. Documento aprobado y auditado.");
                return null;
            });
        } catch (Exception e) {
            System.err.println("Error en transacción (Rollback automático aplicado): " + e.getMessage());
        }
    }

    // Metodo auxiliar para conversión de tipos de fecha
    private LocalDateTime convertirDateALocalDateTime(java.util.Date date) {
        return Documento.convertirDateALocalDateTime(date);
    }

    // === SISTEMA DE RECUPERACIÓN ANTE FALLOS (OPLOG) ===

    // Obtiene operaciones recientes del oplog para monitoreo
    public List<Document> obtenerUltimasOperacionesOplog(int limite) {
        System.out.println("Consultando últimas " + limite + " operaciones del oplog");
        MongoDatabase localDb = MongoConfig.getMongoClient().getDatabase("local");
        MongoCollection<Document> oplog = localDb.getCollection("oplog.rs");
        Bson filtro = Filters.eq("ns", MongoConfig.getDatabaseName() + ".documentos");
        return oplog.find(filtro)
                .sort(Sorts.descending("ts"))
                .limit(limite)
                .into(new ArrayList<>());
    }

    // Obtiene operaciones desde timestamp específico para recuperación incremental
    public List<Document> obtenerOperacionesOplogDesde(BsonTimestamp desdeTs, int limiteSiNoHayTs) {
        System.out.println("Consultando oplog desde timestamp: " + (desdeTs != null ? desdeTs.getValue() : "inicio"));
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

    // Aplica operaciones del oplog para recuperación ante desastres
    public int aplicarRecuperacionOplog(List<Document> operaciones) {
        System.out.println("Iniciando recuperación con " + operaciones.size() + " operaciones del oplog");
        int aplicadas = 0;
        MongoCollection<Document> collection = MongoConfig.getMongoClient()
                .getDatabase(MongoConfig.getDatabaseName())
                .getCollection("documentos");

        for (Document op : operaciones) {
            String operacion = op.getString("op");
            try {
                switch (operacion) {
                    case "i": // Operación de inserción
                        Document docToInsert = op.get("o", Document.class);
                        ObjectId idInsert = docToInsert.getObjectId("_id");
                        if (collection.find(Filters.eq("_id", idInsert)).first() == null) {
                            collection.insertOne(docToInsert);
                            aplicadas++;
                            System.out.println("Recuperado INSERT exitoso: " + idInsert);
                        } else {
                            System.out.println("Documento existente omitido (duplicado): " + idInsert);
                        }
                        break;

                    case "u": // Operación de actualización
                        Document o2 = op.get("o2", Document.class);
                        if (o2 == null || !o2.containsKey("_id")) break;

                        ObjectId idUpdate = o2.getObjectId("_id");
                        Document updateObj = op.get("o", Document.class);

                        Document setFields = new Document();
                        Document unsetFields = new Document();
                        AtomicBoolean hasChanges = new AtomicBoolean(false);

                        // Procesamiento de formatos diff del oplog
                        if (updateObj.containsKey("$v") || updateObj.containsKey("diff")) {
                            Document diff = updateObj.get("diff", Document.class);
                            if (diff != null) {
                                diff.forEach((key, value) -> {
                                    if (key.equals("u") && value instanceof Document subDoc) {
                                        subDoc.forEach((subKey, subValue) -> {
                                            setFields.append(subKey, subValue);
                                            hasChanges.set(true);
                                        });
                                    } else if (key.startsWith("u.") && key.length() > 2) {
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
                            System.out.println("UPDATE omitido (sin cambios válidos): " + idUpdate);
                        }
                        break;

                    case "d": // Operación de eliminación
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
                System.err.println("Error crítico en recuperación oplog: " + e.getMessage());
            }
        }
        System.out.println("Recuperación completada. Operaciones aplicadas: " + aplicadas);
        return aplicadas;
    }

    // === CONSULTAS AVANZADAS CON ÍNDICES ===

    // Búsqueda por rango de fechas utilizando índice compuesto
    public List<Documento> buscarPorRangoFechas(LocalDateTime desde, LocalDateTime hasta) {
        System.out.println("Búsqueda por rango de fechas: " + desde + " hasta " + hasta);
        Bson filtro = new Document();

        if (desde != null && hasta != null) {
            filtro = and(
                    gte("fechaCreacion", Documento.convertirLocalDateTimeADate(desde)),
                    lte("fechaCreacion", Documento.convertirLocalDateTimeADate(hasta))
            );
        } else if (desde != null) {
            filtro = gte("fechaCreacion", Documento.convertirLocalDateTimeADate(desde));
        } else if (hasta != null) {
            filtro = lte("fechaCreacion", Documento.convertirLocalDateTimeADate(hasta));
        }

        return collection.find(filtro)
                .sort(descending("fechaCreacion"))
                .map(doc -> {
                    Documento d = new Documento();
                    d.setId(doc.getObjectId("_id"));
                    d.setTitulo(doc.getString("titulo"));
                    d.setAutor(doc.getString("autor"));
                    d.setTipoDocumento(doc.getString("tipoDocumento"));
                    d.setFechaCreacion(convertirDateALocalDateTime(doc.getDate("fechaCreacion")));
                    d.setFechaModificacion(convertirDateALocalDateTime(doc.getDate("fechaModificacion")));
                    d.setEstado(doc.getString("estado"));
                    d.setVersion(doc.getInteger("version", 1));
                    d.setArchivoId(doc.getObjectId("archivoId"));
                    return d;
                })
                .into(new ArrayList<>());
    }

    // === SIMULACIÓN DE DESASTRES PARA PRUEBAS ===

    // Elimina todos los documentos (solo para pruebas de recuperación)
    public long simularDesastre() {
        System.out.println("ADVERTENCIA: Simulando desastre - eliminando todos los documentos");
        long eliminados = collection.deleteMany(new Document()).getDeletedCount();
        System.out.println("Documentos eliminados en simulación: " + eliminados);
        return eliminados;
    }

    // === ACCESO A COMPONENTES ===

    // Obtiene el bucket GridFS para operaciones directas
    public GridFSBucket getGridFSBucket() {
        return gridFSBucket;
    }
}
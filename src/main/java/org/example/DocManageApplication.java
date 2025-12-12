package org.example;

import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.bson.BsonTimestamp;
import org.example.model.Documento;
import org.example.repository.DocumentoRepository;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Scanner;

// Sistema de Gestión Documental Corporativo con MongoDB
// Implementa transacciones ACID multi-documento y réplica sets para alta disponibilidad
public class DocManageApplication {

    // Configuración de zona horaria para visualización de timestamps
    private static final ZoneId ZONA_LOCAL = ZoneId.systemDefault();
    private static final DateTimeFormatter FORMATO_TIMESTAMP = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

    // Componentes principales del sistema
    private static final DocumentoRepository documentoRepository = new DocumentoRepository();
    private static final Scanner scanner = new Scanner(System.in);

    // Punto de entrada del sistema
    public static void main(String[] args) {
        boolean continuar = true;
        System.out.println("=== SISTEMA DE GESTIÓN DOCUMENTAL DOCMANAGENOSQL ===");
        System.out.println("MongoDB ACID Transactions | Alta Disponibilidad");

        while (continuar) {
            mostrarMenu();
            int opcion = obtenerOpcion();

            switch (opcion) {
                case 1 -> crearDocumento();
                case 2 -> buscarDocumentosPorAutor();
                case 3 -> buscarDocumentosPorTipo();
                case 4 -> buscarPorRangoFechas();
                case 5 -> mostrarTodosLosDocumentos();
                case 6 -> actualizarDocumento();
                case 7 -> eliminarDocumento();
                case 8 -> descargarArchivoAdjunto();
                case 9 -> aprobarDocumentoTransaccion();
                case 10 -> mostrarUltimasOperacionesOplog();
                case 11 -> recuperarDesdeOplog();
                case 12 -> simularDesastreYRecuperacion();
                case 13 -> {
                    System.out.println("Finalizando sesión del sistema de gestión documental...");
                    continuar = false;
                }
                default -> System.out.println("Opción no válida. Seleccione una opción del menú.");
            }
        }
        scanner.close();
        System.out.println("Sistema cerrado exitosamente.");
    }

    // Interfaz de usuario principal
    private static void mostrarMenu() {
        System.out.println("\n--- MENÚ PRINCIPAL ---");
        System.out.println("1.  Registrar nuevo documento");
        System.out.println("2.  Consultar documentos por autor");
        System.out.println("3.  Consultar documentos por tipo");
        System.out.println("4.  Consultar documentos por rango de fechas");
        System.out.println("5.  Listar todos los documentos");
        System.out.println("6.  Modificar documento existente");
        System.out.println("7.  Eliminar documento");
        System.out.println("8.  Descargar archivo adjunto");
        System.out.println("9.  Ejecutar aprobación con transacción ACID");
        System.out.println("10. Monitorear operaciones del oplog");
        System.out.println("11. Ejecutar recuperación desde oplog");
        System.out.println("12. Demostración: Recuperación ante desastres");
        System.out.println("13. Salir del sistema");
        System.out.print("Seleccione una opción: ");
    }

    // Validación de entrada del usuario
    private static int obtenerOpcion() {
        while (true) {
            try {
                String input = scanner.nextLine().trim();
                return Integer.parseInt(input);
            } catch (NumberFormatException e) {
                System.out.print("Entrada inválida. Ingrese un número entre 1 y 13: ");
            }
        }
    }

    // 1. Registro de documentos
    private static void crearDocumento() {
        System.out.println("\n--- REGISTRO DE NUEVO DOCUMENTO ---");
        System.out.print("Título: ");
        String titulo = scanner.nextLine().trim();
        System.out.print("Autor: ");
        String autor = scanner.nextLine().trim();
        System.out.print("Tipo de documento (PDF/DOC/IMAGEN/VIDEO/TXT): ");
        String tipo = scanner.nextLine().trim();

        System.out.print("Ruta del archivo adjunto (opcional): ");
        String rutaArchivo = scanner.nextLine().trim();
        String nombreArchivo = "";
        if (!rutaArchivo.isBlank()) {
            System.out.print("Nombre personalizado del archivo (opcional): ");
            nombreArchivo = scanner.nextLine().trim();
            if (nombreArchivo.isBlank()) {
                nombreArchivo = new java.io.File(rutaArchivo).getName();
            }
        }

        Documento documento = new Documento(titulo, autor, tipo);
        documentoRepository.guardarDocumentoConArchivo(documento, rutaArchivo.isBlank() ? null : rutaArchivo, nombreArchivo);
        System.out.println("Documento registrado exitosamente. ID: " + documento.getId() +
                (documento.getArchivoId() != null ? " | Archivo adjunto: " + documento.getArchivoId() : ""));
    }

    // 2. Consulta por autor
    private static void buscarDocumentosPorAutor() {
        System.out.print("Ingrese autor para consulta: ");
        String autor = scanner.nextLine().trim();
        List<Documento> docs = documentoRepository.obtenerDocumentosPorAutor(autor);
        imprimirDocumentos(docs);
        System.out.println("Total de documentos encontrados: " + docs.size());
    }

    // 3. Consulta por tipo
    private static void buscarDocumentosPorTipo() {
        System.out.print("Ingrese tipo de documento para consulta: ");
        String tipo = scanner.nextLine().trim();
        List<Documento> docs = documentoRepository.obtenerDocumentosPorTipo(tipo);
        imprimirDocumentos(docs);
        System.out.println("Total de documentos encontrados: " + docs.size());
    }

    // 5. Listado completo
    private static void mostrarTodosLosDocumentos() {
        System.out.println("\n--- INVENTARIO COMPLETO DE DOCUMENTOS ---");
        List<Documento> docs = documentoRepository.obtenerTodosLosDocumentos();
        imprimirDocumentos(docs);
        System.out.println("Total en sistema: " + docs.size() + " documentos");
    }

    // Formateador de resultados
    private static void imprimirDocumentos(List<Documento> docs) {
        if (docs.isEmpty()) {
            System.out.println("No se encontraron documentos.");
        } else {
            docs.forEach(doc -> {
                System.out.println(doc);
                if (doc.getArchivoId() != null) {
                    System.out.println("   Archivo adjunto disponible");
                }
                System.out.println("---");
            });
        }
    }

    // 6. Modificación de documentos
    private static void actualizarDocumento() {
        System.out.println("\n--- MODIFICACIÓN DE DOCUMENTO ---");
        System.out.print("ID del documento a modificar: ");
        String id = scanner.nextLine().trim();

        Documento docExistente = documentoRepository.obtenerDocumentoPorId(id);
        if (docExistente == null) {
            System.out.println("Documento no encontrado en el sistema.");
            return;
        }

        System.out.println("Versión actual: " + docExistente.getVersion());
        System.out.print("Ingrese versión actual para verificación: ");
        int versionInput;
        try {
            versionInput = Integer.parseInt(scanner.nextLine().trim());
        } catch (NumberFormatException e) {
            System.out.println("Formato de versión inválido.");
            return;
        }

        System.out.println("Ingrese nuevos valores (vacío para mantener actual):");
        System.out.print("Nuevo título [" + docExistente.getTitulo() + "]: ");
        String nTitulo = scanner.nextLine().trim();
        if (!nTitulo.isEmpty()) docExistente.setTitulo(nTitulo);

        System.out.print("Nuevo autor [" + docExistente.getAutor() + "]: ");
        String nAutor = scanner.nextLine().trim();
        if (!nAutor.isEmpty()) docExistente.setAutor(nAutor);

        System.out.print("Nuevo tipo [" + docExistente.getTipoDocumento() + "]: ");
        String nTipo = scanner.nextLine().trim();
        if (!nTipo.isEmpty()) docExistente.setTipoDocumento(nTipo);

        String nuevaRuta = null;
        String nuevoNombre = null;
        if (docExistente.getArchivoId() != null) {
            System.out.println("Archivo adjunto actual registrado");
        }
        System.out.print("Ruta de nuevo archivo adjunto (opcional): ");
        nuevaRuta = scanner.nextLine().trim();
        if (!nuevaRuta.isBlank()) {
            System.out.print("Nombre del nuevo archivo (opcional): ");
            nuevoNombre = scanner.nextLine().trim();
            if (nuevoNombre.isBlank()) {
                nuevoNombre = new java.io.File(nuevaRuta).getName();
            }
        }

        boolean exito = documentoRepository.actualizarDocumentoConArchivo(
                id, docExistente, versionInput,
                nuevaRuta.isBlank() ? null : nuevaRuta,
                nuevoNombre
        );

        if (exito) {
            System.out.println("Documento modificado exitosamente. Nueva versión: " + (versionInput + 1));
        } else {
            System.out.println("No se pudo completar la modificación. Verifique la versión.");
        }
    }

    // 7. Eliminación de documentos
    private static void eliminarDocumento() {
        System.out.print("ID del documento a eliminar: ");
        String id = scanner.nextLine().trim();
        if (documentoRepository.eliminarDocumento(id)) {
            System.out.println("Documento eliminado del sistema.");
        } else {
            System.out.println("Documento no encontrado.");
        }
    }

    // 8. Descarga de archivos
    private static void descargarArchivoAdjunto() {
        System.out.println("\n--- DESCARGA DE ARCHIVO ADJUNTO ---");
        System.out.print("ID del documento: ");
        String id = scanner.nextLine().trim();

        Documento doc = documentoRepository.obtenerDocumentoPorId(id);
        if (doc == null || doc.getArchivoId() == null) {
            System.out.println("Documento sin archivo adjunto disponible.");
            return;
        }

        Document fileMetadata = documentoRepository.getGridFSBucket()
                .find(Filters.eq("_id", doc.getArchivoId()))
                .first().getMetadata();

        String nombreOriginal = fileMetadata != null ? fileMetadata.getString("filename") : "documento_descargado";
        if (nombreOriginal == null || nombreOriginal.isBlank()) {
            nombreOriginal = "archivo_" + doc.getArchivoId();
        }

        System.out.print("Ruta de destino: ");
        String inputRuta = scanner.nextLine().trim();

        String rutaFinal;
        if (inputRuta.isBlank()) {
            rutaFinal = System.getProperty("user.home") + "/Downloads/" + nombreOriginal;
        } else {
            java.io.File file = new java.io.File(inputRuta);
            if (file.isDirectory() || inputRuta.endsWith("\\") || inputRuta.endsWith("/")) {
                rutaFinal = inputRuta.replaceAll("[\\\\/]+$", "") + java.io.File.separator + nombreOriginal;
            } else {
                rutaFinal = inputRuta;
            }
        }

        System.out.println("Destino: " + rutaFinal);

        try (OutputStream outputStream = new FileOutputStream(rutaFinal)) {
            documentoRepository.getGridFSBucket().downloadToStream(doc.getArchivoId(), outputStream);
            System.out.println("Archivo descargado exitosamente.");
        } catch (IOException e) {
            System.out.println("Error en la descarga: " + e.getMessage());
        }
    }

    // 9. Transacciones ACID
    private static void aprobarDocumentoTransaccion() {
        System.out.println("\n--- APROBACIÓN CON TRANSACCIÓN ACID ---");
        System.out.print("ID del documento a aprobar: ");
        String id = scanner.nextLine().trim();
        documentoRepository.aprobarDocumentoConTransaccion(id);
    }

    // 10. Monitoreo del oplog
    private static void mostrarUltimasOperacionesOplog() {
        System.out.println("\n--- MONITOREO DEL OPLOG ---");
        List<Document> operaciones = documentoRepository.obtenerUltimasOperacionesOplog(20);
        if (operaciones.isEmpty()) {
            System.out.println("No hay operaciones recientes registradas.");
        } else {
            operaciones.forEach(op -> {
                BsonTimestamp bsonTs = op.get("ts", BsonTimestamp.class);
                String timestampLegible = "No disponible";
                if (bsonTs != null) {
                    LocalDateTime utc = LocalDateTime.ofEpochSecond(bsonTs.getTime(), 0, ZoneOffset.UTC);
                    LocalDateTime local = utc.atZone(ZoneOffset.UTC).withZoneSameInstant(ZONA_LOCAL).toLocalDateTime();
                    timestampLegible = local.format(FORMATO_TIMESTAMP);
                }

                System.out.println("Timestamp: " + timestampLegible);

                String operacion = op.getString("op");
                String textoOp = switch (operacion) {
                    case "i" -> "INSERT";
                    case "u" -> "UPDATE";
                    case "d" -> "DELETE";
                    default -> operacion;
                };
                System.out.println("Operación: " + textoOp);

                Object o = op.get("o");
                Object o2 = op.get("o2");
                ObjectId docId = null;

                if (o instanceof Document docO && docO.containsKey("_id")) {
                    docId = docO.getObjectId("_id");
                } else if (o2 instanceof Document docO2 && docO2.containsKey("_id")) {
                    docId = docO2.getObjectId("_id");
                }

                if (docId != null) {
                    System.out.println("ID del documento: " + docId);
                }

                System.out.println("---");
            });
        }
    }

    // 11. Recuperación desde oplog
    private static void recuperarDesdeOplog() {
        System.out.println("\n--- RECUPERACIÓN DESDE OPLOG ---");
        System.out.println("Formato de timestamp: YYYY-MM-DDTHH:MM:SS");
        System.out.print("Timestamp (opcional): ");
        String input = scanner.nextLine().trim();

        BsonTimestamp desdeTs = null;
        if (!input.isEmpty()) {
            try {
                LocalDateTime ldtLocal = LocalDateTime.parse(input, FORMATO_TIMESTAMP);
                long secondsUTC = ldtLocal.atZone(ZONA_LOCAL).withZoneSameInstant(ZoneOffset.UTC).toEpochSecond();
                desdeTs = new BsonTimestamp((int) secondsUTC, 0);
                System.out.println("Timestamp especificado: " + ldtLocal);
            } catch (Exception e) {
                System.out.println("Formato inválido. Usando operaciones recientes.");
            }
        }

        List<Document> ops = documentoRepository.obtenerOperacionesOplogDesde(desdeTs, 20);
        if (ops.isEmpty()) {
            System.out.println("No hay operaciones para recuperar.");
            return;
        }

        System.out.println("Operaciones encontradas: " + ops.size());
        System.out.print("¿Proceder con la recuperación? (s/n): ");
        String confirmar = scanner.nextLine().trim().toLowerCase();

        if ("s".equals(confirmar) || "sí".equals(confirmar)) {
            int aplicadas = documentoRepository.aplicarRecuperacionOplog(ops);
            System.out.println("Recuperación completada. Operaciones aplicadas: " + aplicadas);
        } else {
            System.out.println("Recuperación cancelada.");
        }
    }

    // 12. Demostración de recuperación
    private static void simularDesastreYRecuperacion() {
        System.out.println("\n--- DEMOSTRACIÓN: RECUPERACIÓN ANTE DESASTRES ---");
        System.out.println("Esta operación eliminará todos los documentos del sistema.");
        System.out.print("¿Confirmar demostración? (s/n): ");
        String confirmar = scanner.nextLine().trim().toLowerCase();
        if (!"s".equals(confirmar) && !"sí".equals(confirmar)) {
            System.out.println("Demostración cancelada.");
            return;
        }

        System.out.println("\nEstado inicial del sistema:");
        mostrarTodosLosDocumentos();

        System.out.println("\nEjecutando simulación de desastre...");
        long borrados = documentoRepository.simularDesastre();
        System.out.println("Documentos eliminados: " + borrados);

        System.out.println("\nEstado después del desastre:");
        mostrarTodosLosDocumentos();

        System.out.println("\nIniciando recuperación automática...");
        List<Document> todasLasOps = documentoRepository.obtenerOperacionesOplogDesde(null, 1000);
        if (todasLasOps.isEmpty()) {
            System.out.println("No hay operaciones para recuperación.");
        } else {
            int aplicadas = documentoRepository.aplicarRecuperacionOplog(todasLasOps);
            System.out.println("Operaciones aplicadas: " + aplicadas);
        }

        System.out.println("\nEstado final después de la recuperación:");
        mostrarTodosLosDocumentos();

        System.out.println("\nDemostración completada exitosamente.");
    }

    // 4. Búsqueda por rango de fechas
    private static void buscarPorRangoFechas() {
        System.out.println("\n--- CONSULTA POR RANGO DE FECHAS ---");
        System.out.println("Formato: YYYY-MM-DD");
        System.out.print("Fecha inicial (opcional): ");
        String desdeStr = scanner.nextLine().trim();
        System.out.print("Fecha final (opcional): ");
        String hastaStr = scanner.nextLine().trim();

        LocalDateTime desde = null;
        LocalDateTime hasta = null;

        try {
            if (!desdeStr.isBlank()) {
                desde = LocalDate.parse(desdeStr).atStartOfDay();
            }
            if (!hastaStr.isBlank()) {
                hasta = LocalDate.parse(hastaStr).atTime(23, 59, 59, 999_999_999);
            }
        } catch (Exception e) {
            System.out.println("Formato de fecha inválido.");
            return;
        }

        List<Documento> resultados = documentoRepository.buscarPorRangoFechas(desde, hasta);
        System.out.println("\nResultados de la consulta: " + resultados.size() + " documentos");
        imprimirDocumentos(resultados);
    }
}
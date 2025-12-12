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

public class DocManageApplication {

    // Configuración de zona horaria y formato (todo en hora local)
    private static final ZoneId ZONA_LOCAL = ZoneId.systemDefault(); // Ej: America/Bogota
    private static final DateTimeFormatter FORMATO_TIMESTAMP = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

    private static final DocumentoRepository documentoRepository = new DocumentoRepository();
    private static final Scanner scanner = new Scanner(System.in);

    public static void main(String[] args) {
        boolean continuar = true;
        System.out.println("=== Sistema de Gestión Documental DocManageNoSQL ===");

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
                    System.out.println("Saliendo del sistema...");
                    continuar = false;
                }
                default -> System.out.println("Opción no válida. Por favor, intente de nuevo.");
            }
        }
        scanner.close();
    }

    private static void mostrarMenu() {
        System.out.println("\n--- Menú Principal ---");
        System.out.println("1. Crear nuevo documento (con archivo opcional)");
        System.out.println("2. Buscar documentos por autor");
        System.out.println("3. Buscar documentos por tipo");
        System.out.println("4. Buscar documentos por rango de fechas");
        System.out.println("5. Mostrar todos los documentos");
        System.out.println("6. Actualizar documento (con opción de cambiar archivo)");
        System.out.println("7. Eliminar documento");
        System.out.println("8. Descargar archivo adjunto de un documento");
        System.out.println("9. Aprobar documento (Transacción ACID)");
        System.out.println("10. Mostrar últimas operaciones en Oplog");
        System.out.println("11. Recuperar documentos desde timestamp (Oplog Recovery)");
        System.out.println("12. Simular desastre y recuperación automática [DEMO]");
        System.out.println("13. Salir");
        System.out.print("Seleccione una opción: ");
    }

    private static int obtenerOpcion() {
        while (true) {
            try {
                String input = scanner.nextLine().trim();
                return Integer.parseInt(input);
            } catch (NumberFormatException e) {
                System.out.print("Entrada inválida. Ingrese un número: ");
            }
        }
    }

    // === 1. CREAR DOCUMENTO ===
    private static void crearDocumento() {
        System.out.println("\n--- Crear Nuevo Documento ---");
        System.out.print("Título: ");
        String titulo = scanner.nextLine().trim();
        System.out.print("Autor: ");
        String autor = scanner.nextLine().trim();
        System.out.print("Tipo (PDF/DOC/IMAGEN/VIDEO): ");
        String tipo = scanner.nextLine().trim();

        System.out.print("Ruta del archivo (dejar vacío si no hay): ");
        String rutaArchivo = scanner.nextLine().trim();
        String nombreArchivo = "";
        if (!rutaArchivo.isBlank()) {
            System.out.print("Nombre del archivo, incluye su extension (Enter para usar nombre original): ");
            nombreArchivo = scanner.nextLine().trim();
            if (nombreArchivo.isBlank()) {
                nombreArchivo = new java.io.File(rutaArchivo).getName();
            }
        }

        Documento documento = new Documento(titulo, autor, tipo);
        documentoRepository.guardarDocumentoConArchivo(documento, rutaArchivo.isBlank() ? null : rutaArchivo, nombreArchivo);
        System.out.println("✅ Documento creado exitosamente." +
                (documento.getArchivoId() != null ? " (con archivo adjunto ID: " + documento.getArchivoId() + ")" : " (sin archivo)"));
    }

    // === BÚSQUEDAS ===
    private static void buscarDocumentosPorAutor() {
        System.out.print("Ingrese autor: ");
        String autor = scanner.nextLine().trim();
        List<Documento> docs = documentoRepository.obtenerDocumentosPorAutor(autor);
        imprimirDocumentos(docs);
    }

    private static void buscarDocumentosPorTipo() {
        System.out.print("Ingrese tipo: ");
        String tipo = scanner.nextLine().trim();
        List<Documento> docs = documentoRepository.obtenerDocumentosPorTipo(tipo);
        imprimirDocumentos(docs);
    }

    private static void mostrarTodosLosDocumentos() {
        List<Documento> docs = documentoRepository.obtenerTodosLosDocumentos();
        imprimirDocumentos(docs);
    }

    private static void imprimirDocumentos(List<Documento> docs) {
        if (docs.isEmpty()) {
            System.out.println("No hay documentos.");
        } else {
            docs.forEach(doc -> {
                System.out.println(doc);
                if (doc.getArchivoId() != null) {
                    System.out.println("   📎 Archivo adjunto: " + doc.getArchivoId());
                }
            });
        }
    }

    // === 5. ACTUALIZAR DOCUMENTO ===
    private static void actualizarDocumento() {
        System.out.println("\n--- Actualizar Documento ---");
        System.out.print("Ingrese el ID del documento: ");
        String id = scanner.nextLine().trim();

        Documento docExistente = documentoRepository.obtenerDocumentoPorId(id);
        if (docExistente == null) {
            System.out.println("❌ Documento no encontrado.");
            return;
        }

        System.out.println("Versión actual en BD: " + docExistente.getVersion());
        System.out.print("Ingrese la versión actual para confirmar (optimistic locking): ");
        int versionInput;
        try {
            versionInput = Integer.parseInt(scanner.nextLine().trim());
        } catch (NumberFormatException e) {
            System.out.println("Versión inválida.");
            return;
        }

        System.out.println("Deje en blanco para mantener el valor actual:");
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
            System.out.println("Archivo actual: " + docExistente.getArchivoId());
        }
        System.out.print("¿Reemplazar archivo adjunto? Ruta nueva (dejar vacío para no cambiar): ");
        nuevaRuta = scanner.nextLine().trim();
        if (!nuevaRuta.isBlank()) {
            System.out.print("Nombre para el nuevo archivo (Enter para usar nombre original): ");
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
            System.out.println("✅ Documento actualizado correctamente (versión incrementada).");
        } else {
            System.out.println("❌ ERROR: No se pudo actualizar. Posible conflicto de concurrencia.");
        }
    }

    // === 6. ELIMINAR ===
    private static void eliminarDocumento() {
        System.out.print("Ingrese ID a eliminar: ");
        String id = scanner.nextLine().trim();
        if (documentoRepository.eliminarDocumento(id)) {
            System.out.println("✅ Documento y archivo adjunto eliminados correctamente.");
        } else {
            System.out.println("❌ No encontrado.");
        }
    }

    // === 7. DESCARGAR ARCHIVO ===
    private static void descargarArchivoAdjunto() {
        System.out.println("\n--- Descargar Archivo Adjunto ---");
        System.out.print("Ingrese el ID del documento: ");
        String id = scanner.nextLine().trim();

        Documento doc = documentoRepository.obtenerDocumentoPorId(id);
        if (doc == null || doc.getArchivoId() == null) {
            System.out.println("Documento no encontrado o no tiene archivo adjunto.");
            return;
        }

        // Obtener metadatos del archivo desde GridFS para saber su nombre original
        Document fileMetadata = documentoRepository.getGridFSBucket()
                .find(Filters.eq("_id", doc.getArchivoId()))
                .first().getMetadata();

        String nombreOriginal = fileMetadata != null ? fileMetadata.getString("filename") : "archivo_descargado";
        if (nombreOriginal == null || nombreOriginal.isBlank()) {
            nombreOriginal = "archivo_" + doc.getArchivoId();
        }

        System.out.print("Ruta completa para guardar (puede ser solo carpeta o archivo completo): ");
        String inputRuta = scanner.nextLine().trim();

        String rutaFinal;
        if (inputRuta.isBlank()) {
            rutaFinal = System.getProperty("user.home") + "/Downloads/" + nombreOriginal;
        } else {
            java.io.File file = new java.io.File(inputRuta);
            if (file.isDirectory() || inputRuta.endsWith("\\") || inputRuta.endsWith("/")) {
                // Es una carpeta → agregar nombre original
                rutaFinal = inputRuta.replaceAll("[\\\\/]+$", "") + java.io.File.separator + nombreOriginal;
            } else {
                // Es ruta completa con nombre → usar tal cual
                rutaFinal = inputRuta;
            }
        }

        System.out.println("Guardando como: " + rutaFinal);

        try (OutputStream outputStream = new FileOutputStream(rutaFinal)) {
            documentoRepository.getGridFSBucket().downloadToStream(doc.getArchivoId(), outputStream);
            System.out.println("Archivo descargado exitosamente!");
        } catch (IOException e) {
            System.err.println("Error al guardar el archivo: " + e.getMessage());
            System.out.println("Posibles causas:");
            System.out.println("  • La carpeta no existe");
            System.out.println("  • No tienes permisos de escritura");
            System.out.println("  • El archivo está abierto en otro programa");
        }
    }

    private static String getString(String contentType) {
        String extension = "";
        if (contentType != null) {
            extension = switch (contentType) {
                case "image/jpeg", "image/jpg" -> ".jpg";
                case "image/png" -> ".png";
                case "image/gif" -> ".gif";
                case "image/webp" -> ".webp";
                case "application/pdf" -> ".pdf";
                case "video/mp4" -> ".mp4";
                case "video/webm" -> ".webm";
                case "text/plain" -> ".txt";
                case "application/msword" -> ".doc";
                case "application/vnd.openxmlformats-officedocument.wordprocessingml.document" -> ".docx";
                default -> "";
            };
        }
        return extension;
    }

    // === 8. APROBAR ===
    private static void aprobarDocumentoTransaccion() {
        System.out.println("\n--- Aprobar Documento (Transacción ACID) ---");
        System.out.print("ID del documento a aprobar: ");
        String id = scanner.nextLine().trim();
        documentoRepository.aprobarDocumentoConTransaccion(id);
    }

    // === 9. MOSTRAR OPLOG (timestamps en hora local) ===
    private static void mostrarUltimasOperacionesOplog() {
        System.out.println("\n--- Últimas 20 Operaciones en Oplog ---");
        List<Document> operaciones = documentoRepository.obtenerUltimasOperacionesOplog(20);
        if (operaciones.isEmpty()) {
            System.out.println("No hay operaciones recientes en la colección de documentos.");
        } else {
            operaciones.forEach(op -> {
                BsonTimestamp bsonTs = op.get("ts", BsonTimestamp.class);
                String timestampLegible = "Timestamp inválido";
                if (bsonTs != null) {
                    LocalDateTime utc = LocalDateTime.ofEpochSecond(bsonTs.getTime(), 0, ZoneOffset.UTC);
                    LocalDateTime local = utc.atZone(ZoneOffset.UTC).withZoneSameInstant(ZONA_LOCAL).toLocalDateTime();
                    timestampLegible = local.format(FORMATO_TIMESTAMP);
                }

                System.out.println("📅 Timestamp: " + timestampLegible);

                String operacion = op.getString("op");
                String textoOp = switch (operacion) {
                    case "i" -> "INSERT (Nuevo documento creado)";
                    case "u" -> "UPDATE (Documento actualizado)";
                    case "d" -> "DELETE (Documento eliminado)";
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
                    System.out.println("Documento ID: " + docId);
                }

                System.out.println("Detalle: " + op.get("o"));
                System.out.println("---");
            });
        }
    }

    // === 10. RECUPERACIÓN (input interpretado como hora local) ===
    private static void recuperarDesdeOplog() {
        System.out.println("\n--- Recuperación desde Oplog ---");
        System.out.println("Ingresa el timestamp en formato: YYYY-MM-DDTHH:MM:SS (hora local)");
        System.out.println("Ejemplo: 2025-12-10T20:30:45");
        System.out.print("Timestamp (dejar vacío para recuperar las últimas 20 operaciones): ");
        String input = scanner.nextLine().trim();

        BsonTimestamp desdeTs = null;
        if (!input.isEmpty()) {
            try {
                LocalDateTime ldtLocal = LocalDateTime.parse(input, FORMATO_TIMESTAMP);
                // Convertir hora local → UTC
                long secondsUTC = ldtLocal.atZone(ZONA_LOCAL).withZoneSameInstant(ZoneOffset.UTC).toEpochSecond();
                desdeTs = new BsonTimestamp((int) secondsUTC, 0);
                System.out.println("Filtrando operaciones a partir de: " + ldtLocal + " (hora local)");
            } catch (Exception e) {
                System.out.println("❌ Formato inválido. Usando las últimas 20 operaciones.");
            }
        }

        List<Document> ops = documentoRepository.obtenerOperacionesOplogDesde(desdeTs, 20);
        if (ops.isEmpty()) {
            System.out.println("No se encontraron operaciones para recuperar.");
            return;
        }

        System.out.println("Se encontraron " + ops.size() + " operaciones relevantes.");
        System.out.print("¿Aplicar recuperación? (s/n): ");
        String confirmar = scanner.nextLine().trim().toLowerCase();

        if ("s".equals(confirmar) || "sí".equals(confirmar)) {
            int aplicadas = documentoRepository.aplicarRecuperacionOplog(ops);
            System.out.println("✅ Recuperación completada. Operaciones aplicadas: " + aplicadas);
        } else {
            System.out.println("Recuperación cancelada.");
        }
    }

    private static void simularDesastreYRecuperacion() {
        System.out.println("\n--- SIMULACIÓN DE DESASTRE Y RECUPERACIÓN ---");
        System.out.println("¡ADVERTENCIA! Esto eliminará TODOS los documentos y luego intentará recuperarlos usando el oplog.");
        System.out.print("¿Estás seguro? (s/n): ");
        String confirmar = scanner.nextLine().trim().toLowerCase();
        if (!"s".equals(confirmar) && !"sí".equals(confirmar)) {
            System.out.println("Operación cancelada.");
            return;
        }

        // Paso 1: Mostrar estado actual
        System.out.println("\nEstado actual (antes del desastre):");
        mostrarTodosLosDocumentos();

        // Paso 2: Simular desastre - borrar todos los documentos
        System.out.println("\n🔥 Simulando desastre: Eliminando todos los documentos...");
        long borrados = documentoRepository.simularDesastre();
        System.out.println("Documentos eliminados: " + borrados);

        // Verificar estado después del desastre
        System.out.println("\nEstado después del desastre:");
        mostrarTodosLosDocumentos();

        // Paso 3: Recuperación automática usando oplog (desde el principio del tiempo)
        System.out.println("\n🔄 Iniciando recuperación automática usando oplog...");
        List<Document> todasLasOps = documentoRepository.obtenerOperacionesOplogDesde(null, 1000); // máximo razonable
        if (todasLasOps.isEmpty()) {
            System.out.println("No se encontraron operaciones en el oplog para recuperar.");
        } else {
            System.out.println("Aplicando " + todasLasOps.size() + " operaciones del oplog...");
            int aplicadas = documentoRepository.aplicarRecuperacionOplog(todasLasOps);
            System.out.println("✅ Recuperación completada. Operaciones aplicadas: " + aplicadas);
        }

        // Paso 4: Mostrar estado final
        System.out.println("\nEstado final después de la recuperación:");
        mostrarTodosLosDocumentos();

        System.out.println("\n¡Demostración completada! El sistema ha sido restaurado usando el oplog.");
    }

    private static void buscarPorRangoFechas() {
        System.out.println("\n--- Buscar Documentos por Rango de Fechas ---");
        System.out.println("Formato: YYYY-MM-DD (ej: 2025-12-10)");
        System.out.print("Fecha desde (dejar vacío para sin límite inferior): ");
        String desdeStr = scanner.nextLine().trim();
        System.out.print("Fecha hasta (dejar vacío para sin límite superior): ");
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
            System.out.println("Formato de fecha inválido. Use YYYY-MM-DD");
            return;
        }

        List<Documento> resultados = documentoRepository.buscarPorRangoFechas(desde, hasta);
        System.out.println("\nResultados encontrados: " + resultados.size() + "\n");
        imprimirDocumentos(resultados);
    }
}
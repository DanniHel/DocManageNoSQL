package org.example;

import org.bson.Document;
import org.example.model.Documento;
import org.example.repository.DocumentoRepository;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import java.util.Scanner;

public class DocManageApplication {
    public static final DocumentoRepository documentoRepository = new DocumentoRepository();
    private static final Scanner scanner = new Scanner(System.in);

    public static void main(String[] args) {
        boolean continuar = true;

        System.out.println("=== Sistema de Gestión Documental DocManageNoSQL ===");

        while (continuar) {
            mostrarMenu();
            int opcion = obtenerOpcion();

            switch (opcion) {
                case 1:
                    crearDocumento();
                    break;
                case 2:
                    buscarDocumentosPorAutor();
                    break;
                case 3:
                    buscarDocumentosPorTipo();
                    break;
                case 4:
                    mostrarTodosLosDocumentos();
                    break;
                case 5:
                    actualizarDocumento();
                    break;
                case 6:
                    eliminarDocumento();
                    break;
                case 7:
                    subirArchivoGridFS();  // Antes era 8
                    break;
                case 8:
                    aprobarDocumentoTransaccion();  // Antes era 9
                    break;
                case 9:
                    mostrarUltimasOperacionesOplog();  // Antes era 10
                    break;
                case 10:
                    recuperarDesdeOplog();  // Antes era 11
                    break;
                case 11:
                    System.out.println("Saliendo del sistema...");
                    continuar = false;
                    break;
                default:
                    System.out.println("Opción no válida. Por favor, intente de nuevo.");
            }
        }
        scanner.close();
    }

    private static void mostrarMenu() {
        System.out.println("\n--- Menú Principal ---");
        System.out.println("1. Crear nuevo documento");
        System.out.println("2. Buscar documentos por autor");
        System.out.println("3. Buscar documentos por tipo");
        System.out.println("4. Mostrar todos los documentos");
        System.out.println("5. Actualizar documento (Control de Concurrencia)");
        System.out.println("6. Eliminar documento");
        System.out.println("7. Subir archivo adjunto (GridFS)");
        System.out.println("8. Aprobar documento (Transacción ACID)");
        System.out.println("9. Mostrar últimas operaciones en Oplog");
        System.out.println("10. Recuperar documentos desde timestamp (Oplog Recovery)");
        System.out.println("11. Salir");
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

    private static void crearDocumento() {
        System.out.println("\n--- Crear Nuevo Documento ---");
        System.out.print("Título: ");
        String titulo = scanner.nextLine().trim();
        System.out.print("Autor: ");
        String autor = scanner.nextLine().trim();
        System.out.print("Tipo (PDF/DOC): ");
        String tipo = scanner.nextLine().trim();

        Documento documento = new Documento(titulo, autor, tipo);
        documentoRepository.guardarDocumento(documento);
        System.out.println("✅ Documento creado exitosamente.");
    }

    private static void buscarDocumentosPorAutor() {
        System.out.print("Ingrese autor: ");
        String autor = scanner.nextLine();
        List<Documento> docs = documentoRepository.obtenerDocumentosPorAutor(autor);
        docs.forEach(System.out::println);
    }

    private static void buscarDocumentosPorTipo() {
        System.out.print("Ingrese tipo: ");
        String tipo = scanner.nextLine();
        List<Documento> docs = documentoRepository.obtenerDocumentosPorTipo(tipo);
        docs.forEach(System.out::println);
    }

    private static void mostrarTodosLosDocumentos() {
        List<Documento> docs = documentoRepository.obtenerTodosLosDocumentos();
        if (docs.isEmpty()) System.out.println("No hay documentos.");
        else docs.forEach(System.out::println);
    }

    // MÉTODO CORREGIDO PARA SOPORTAR CONCURRENCIA
    private static void actualizarDocumento() {
        System.out.println("\n--- Actualizar Documento ---");
        System.out.print("Ingrese el ID del documento: ");
        String id = scanner.nextLine().trim();

        // Buscamos el documento para asegurarnos que existe
        Documento docExistente = documentoRepository.obtenerDocumentoPorId(id);
        if (docExistente == null) {
            System.out.println("❌ No encontrado.");
            return;
        }

        // PEDIMOS LA VERSIÓN PARA PROBAR EL OPTIMISTIC LOCKING
        System.out.println("Versión actual en BD: " + docExistente.getVersion());
        System.out.print("Ingrese la versión que desea actualizar (si pone una vieja fallará): ");
        int versionInput = Integer.parseInt(scanner.nextLine().trim());

        System.out.print("Nuevo título (Enter para mantener): ");
        String nTitulo = scanner.nextLine().trim();
        if (!nTitulo.isEmpty()) docExistente.setTitulo(nTitulo);

        System.out.print("Nuevo autor (Enter para mantener): ");
        String nAutor = scanner.nextLine().trim();
        if (!nAutor.isEmpty()) docExistente.setAutor(nAutor);

        System.out.print("Nuevo tipo (Enter para mantener): ");
        String nTipo = scanner.nextLine().trim();
        if (!nTipo.isEmpty()) docExistente.setTipoDocumento(nTipo);

        // AQUÍ LLAMAMOS AL MÉTODO DE 3 ARGUMENTOS
        boolean exito = documentoRepository.actualizarDocumento(id, docExistente, versionInput);

        if (exito) {
            System.out.println("✅ Actualizado correctamente (Versión incrementada).");
        } else {
            System.out.println("❌ ERROR DE CONCURRENCIA: La versión no coincide o el documento fue modificado por otro usuario.");
        }
    }

    private static void eliminarDocumento() {
        System.out.print("Ingrese ID a eliminar: ");
        String id = scanner.nextLine().trim();
        if (documentoRepository.eliminarDocumento(id)) {
            System.out.println("✅ Eliminado.");
        } else {
            System.out.println("❌ No encontrado.");
        }
    }

    // --- NUEVOS MÉTODOS PARA REQUISITOS FALTANTES ---

    private static void subirArchivoGridFS() {
        System.out.println("\n--- Subir Archivo (GridFS) ---");
        System.out.print("Ruta del archivo (ej: C:\\foto.jpg): ");
        String ruta = scanner.nextLine().trim();
        System.out.print("Nombre para guardar: ");
        String nombre = scanner.nextLine().trim();

        var id = documentoRepository.subirArchivo(ruta, nombre);
        if (id != null) System.out.println("✅ Archivo subido con ID: " + id);
    }

    private static void aprobarDocumentoTransaccion() {
        System.out.println("\n--- Aprobar Documento (Transacción ACID) ---");
        System.out.print("ID del documento a aprobar: ");
        String id = scanner.nextLine().trim();
        documentoRepository.aprobarDocumentoConTransaccion(id);
    }

    private static void mostrarUltimasOperacionesOplog() {
        System.out.println("\n--- Últimas 20 Operaciones en Oplog (Solo Documentos) ---");
        List<Document> operaciones = documentoRepository.obtenerUltimasOperacionesOplog(20);
        if (operaciones.isEmpty()) {
            System.out.println("No hay operaciones recientes en la colección de documentos.");
        } else {
            operaciones.forEach(op -> {
                System.out.println("Timestamp: " + op.get("ts"));

                String operacion = op.getString("op");
                switch (operacion) {
                    case "i": System.out.println("Operación: INSERT (Nuevo documento creado)"); break;
                    case "u": System.out.println("Operación: UPDATE (Documento actualizado)"); break;
                    case "d": System.out.println("Operación: DELETE (Documento eliminado)"); break;
                    default: System.out.println("Operación: " + operacion);
                }

                System.out.println("Detalle: " + op.get("o"));
                System.out.println("---");
            });
        }
    }

    private static void recuperarDesdeOplog() {
        System.out.println("\n--- Recuperación desde Oplog ---");
        System.out.println("Ingresa el timestamp en formato: YYYY-MM-DDTHH:MM:SS");
        System.out.println("Ejemplo: 2025-12-10T20:30:45");
        System.out.print("Timestamp (dejar vacío para recuperar las últimas 20 operaciones): ");
        String input = scanner.nextLine().trim();

        org.bson.BsonTimestamp desdeTs = null;
        if (!input.isEmpty()) {
            try {
                // Soporta formatos como 2025-12-10T20:30:45 o con milisegundos
                LocalDateTime ldt = LocalDateTime.parse(input,
                        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"));
                long seconds = ldt.toEpochSecond(ZoneOffset.UTC);
                desdeTs = new org.bson.BsonTimestamp((int) seconds, 0);
                System.out.println("Filtrando operaciones a partir de: " + ldt);
            } catch (Exception e) {
                System.out.println("❌ Formato inválido. Usando las últimas 20 operaciones.");
            }
        }

        List<Document> ops = documentoRepository.obtenerOperacionesOplogDesde(desdeTs, 20); // límite de 20 si no hay ts
        if (ops.isEmpty()) {
            System.out.println("No se encontraron operaciones para recuperar.");
            return;
        }

        System.out.println("Se encontraron " + ops.size() + " operaciones relevantes en la colección 'documentos'.");
        System.out.print("¿Aplicar recuperación? (s/n): ");
        String confirmar = scanner.nextLine().trim().toLowerCase();

        if ("s".equals(confirmar) || "sí".equals(confirmar)) {
            int aplicadas = documentoRepository.aplicarRecuperacionOplog(ops);
            System.out.println("✅ Recuperación completada. Operaciones aplicadas: " + aplicadas);
        } else {
            System.out.println("Recuperación cancelada.");
        }
    }
}
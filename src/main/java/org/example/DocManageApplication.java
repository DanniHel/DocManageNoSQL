package org.example;

import org.example.model.Documento;
import org.example.repository.DocumentoRepository;

import java.util.List;
import java.util.Scanner;

public class DocManageApplication {
    private static final DocumentoRepository documentoRepository = new DocumentoRepository();
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
                case 8: // Opción nueva
                    subirArchivoGridFS();
                    break;
                case 9: // Opción nueva
                    aprobarDocumentoTransaccion();
                    break;
                case 7:
                    System.out.println("Saliendo del sistema...");
                    continuar = false;
                    break;
                default:
                    System.out.println("Opción no válida.");
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
        System.out.println("8. Subir archivo adjunto (GridFS) [NUEVO]");
        System.out.println("9. Aprobar documento (Transacción ACID) [NUEVO]");
        System.out.println("7. Salir");
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
}
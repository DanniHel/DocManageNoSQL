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
                case 7:
                    System.out.println("Saliendo del sistema...");
                    continuar = false;
                    break;
                default:
                    System.out.println("Opción no válida. Por favor, seleccione una opción del 1 al 7.");
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
        System.out.println("5. Actualizar documento");
        System.out.println("6. Eliminar documento");
        System.out.println("7. Salir");
        System.out.print("Seleccione una opción: ");
    }

    private static int obtenerOpcion() {
        while (true) {
            try {
                String input = scanner.nextLine().trim();
                int opcion = Integer.parseInt(input);
                if (opcion >= 1 && opcion <= 7) {
                    return opcion;
                } else {
                    System.out.print("Por favor, ingrese un número entre 1 y 7: ");
                }
            } catch (NumberFormatException e) {
                System.out.print("Entrada inválida. Por favor, ingrese un número válido: ");
            }
        }
    }

    private static void crearDocumento() {
        System.out.println("\n--- Crear Nuevo Documento ---");

        String titulo;
        while (true) {
            System.out.print("Ingrese el título del documento (obligatorio): ");
            titulo = scanner.nextLine().trim();
            if (!titulo.isEmpty()) {
                break;
            }
            System.out.println("El título no puede estar vacío. Intente nuevamente.");
        }

        String autor;
        while (true) {
            System.out.print("Ingrese el autor del documento (obligatorio): ");
            autor = scanner.nextLine().trim();
            if (!autor.isEmpty()) {
                break;
            }
            System.out.println("El autor no puede estar vacío. Intente nuevamente.");
        }

        System.out.print("Ingrese el tipo de documento: ");
        String tipoDocumento = scanner.nextLine().trim();

        if (tipoDocumento.isEmpty()) {
            System.out.println("El tipo de documento no puede estar vacío. Se asignará 'Documento' por defecto.");
            tipoDocumento = "Documento";
        }

        Documento documento = new Documento(titulo, autor, tipoDocumento);
        documentoRepository.guardarDocumento(documento);

        System.out.println("Documento creado exitosamente.");
        System.out.println("Detalles del documento creado:");
        System.out.println(documento);
    }

    private static void buscarDocumentosPorAutor() {
        System.out.println("\n--- Buscar Documentos por Autor ---");
        System.out.print("Ingrese el nombre del autor: ");
        String autor = scanner.nextLine();

        List<Documento> documentos = documentoRepository.obtenerDocumentosPorAutor(autor);

        if (documentos.isEmpty()) {
            System.out.println("No se encontraron documentos para el autor: " + autor);
        } else {
            System.out.println("Documentos encontrados para el autor '" + autor + "':");
            for (Documento doc : documentos) {
                System.out.println(doc);
            }
        }
    }

    private static void buscarDocumentosPorTipo() {
        System.out.println("\n--- Buscar Documentos por Tipo ---");
        System.out.print("Ingrese el tipo de documento: ");
        String tipoDocumento = scanner.nextLine();

        List<Documento> documentos = documentoRepository.obtenerDocumentosPorTipo(tipoDocumento);

        if (documentos.isEmpty()) {
            System.out.println("No se encontraron documentos del tipo: " + tipoDocumento);
        } else {
            System.out.println("Documentos encontrados del tipo '" + tipoDocumento + "':");
            for (Documento doc : documentos) {
                System.out.println(doc);
            }
        }
    }

    private static void mostrarTodosLosDocumentos() {
        List<Documento> documentos = documentoRepository.obtenerTodosLosDocumentos();

        if (documentos.isEmpty()) {
            System.out.println("No hay documentos en el sistema.");
        } else {
            System.out.println("\n--- Todos los Documentos ---");
            System.out.println("Total de documentos: " + documentos.size());
            for (Documento doc : documentos) {
                System.out.println(doc);
            }
        }
    }

    private static void actualizarDocumento() {
        System.out.println("\n--- Actualizar Documento ---");
        System.out.print("Ingrese el ID del documento a actualizar: ");
        String id = scanner.nextLine().trim();

        Documento documentoExistente = documentoRepository.obtenerDocumentoPorId(id);
        if (documentoExistente == null) {
            System.out.println("No se encontró un documento con el ID proporcionado.");
            return;
        }

        System.out.println("Documento actual:");
        System.out.println(documentoExistente);

        System.out.print("Nuevo título (deje vacío para mantener actual): ");
        String nuevoTitulo = scanner.nextLine().trim();
        if (!nuevoTitulo.isEmpty()) {
            documentoExistente.setTitulo(nuevoTitulo);
        }

        System.out.print("Nuevo autor (deje vacío para mantener actual): ");
        String nuevoAutor = scanner.nextLine().trim();
        if (!nuevoAutor.isEmpty()) {
            documentoExistente.setAutor(nuevoAutor);
        }

        System.out.print("Nuevo tipo de documento (deje vacío para mantener actual): ");
        String nuevoTipo = scanner.nextLine().trim();
        if (!nuevoTipo.isEmpty()) {
            documentoExistente.setTipoDocumento(nuevoTipo);
        }

        System.out.print("Nuevo estado (deje vacío para mantener actual, ej: APROBADO, RECHAZADO): ");
        String nuevoEstado = scanner.nextLine().trim();
        if (!nuevoEstado.isEmpty()) {
            documentoExistente.setEstado(nuevoEstado);
        }

        boolean actualizado = documentoRepository.actualizarDocumento(id, documentoExistente);
        if (actualizado) {
            System.out.println("Documento actualizado exitosamente.");
        } else {
            System.out.println("Error al actualizar el documento.");
        }
    }

    private static void eliminarDocumento() {
        System.out.println("\n--- Eliminar Documento ---");
        System.out.print("Ingrese el ID del documento a eliminar: ");
        String id = scanner.nextLine().trim();

        Documento documentoAEliminar = documentoRepository.obtenerDocumentoPorId(id);
        if (documentoAEliminar == null) {
            System.out.println("No se encontró un documento con el ID proporcionado.");
            return;
        }

        System.out.println("Documento a eliminar:");
        System.out.println(documentoAEliminar);
        System.out.print("¿Está seguro de que desea eliminar este documento? (s/n): ");
        String confirmacion = scanner.nextLine().trim().toLowerCase();

        if ("s".equals(confirmacion) || "si".equals(confirmacion)) {
            boolean eliminado = documentoRepository.eliminarDocumento(id);
            if (eliminado) {
                System.out.println("Documento eliminado exitosamente.");
            } else {
                System.out.println("Error al eliminar el documento.");
            }
        } else {
            System.out.println("Operación de eliminación cancelada.");
        }
    }
}
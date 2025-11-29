package org.example.model;

import org.bson.types.ObjectId;

import java.time.LocalDateTime;

public class Documento {
    private ObjectId id;
    private String titulo;
    private String autor;
    private String tipoDocumento;
    private LocalDateTime fechaCreacion;
    private LocalDateTime fechaModificacion;
    private String estado;
    private int version;

    // Constructor por defecto
    public Documento() {
        this.fechaCreacion = LocalDateTime.now();
        this.fechaModificacion = LocalDateTime.now();
        this.version = 1;
        this.estado = "BORRADOR";
    }

    // Constructor con parámetros principales
    public Documento(String titulo, String autor, String tipoDocumento) {
        this();
        this.titulo = titulo;
        this.autor = autor;
        this.tipoDocumento = tipoDocumento;
    }

    // Getters y Setters
    public ObjectId getId() {
        return id;
    }

    public void setId(ObjectId id) {
        this.id = id;
    }

    public String getTitulo() {
        return titulo;
    }

    public void setTitulo(String titulo) {
        this.titulo = titulo;
    }

    public String getAutor() {
        return autor;
    }

    public void setAutor(String autor) {
        this.autor = autor;
    }

    public String getTipoDocumento() {
        return tipoDocumento;
    }

    public void setTipoDocumento(String tipoDocumento) {
        this.tipoDocumento = tipoDocumento;
    }

    public LocalDateTime getFechaCreacion() {
        return fechaCreacion;
    }

    public void setFechaCreacion(LocalDateTime fechaCreacion) {
        this.fechaCreacion = fechaCreacion;
    }

    public LocalDateTime getFechaModificacion() {
        return fechaModificacion;
    }

    public void setFechaModificacion(LocalDateTime fechaModificacion) {
        this.fechaModificacion = fechaModificacion;
    }

    public String getEstado() {
        return estado;
    }

    public void setEstado(String estado) {
        this.estado = estado;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    // Agregar estos métodos estáticos en la clase Documento
    public static LocalDateTime convertirDateALocalDateTime(java.util.Date date) {
        if (date != null) {
            return date.toInstant().atZone(java.time.ZoneId.systemDefault()).toLocalDateTime();
        }
        return null;
    }

    public static java.util.Date convertirLocalDateTimeADate(LocalDateTime localDateTime) {
        if (localDateTime != null) {
            return java.util.Date.from(localDateTime.atZone(java.time.ZoneId.systemDefault()).toInstant());
        }
        return null;
    }

    @Override
    public String toString() {
        return "Documento{" +
                "id=" + id +
                ", titulo='" + titulo + '\'' +
                ", autor='" + autor + '\'' +
                ", tipoDocumento='" + tipoDocumento + '\'' +
                ", fechaCreacion=" + fechaCreacion +
                ", estado='" + estado + '\'' +
                ", version=" + version +
                '}';
    }




}
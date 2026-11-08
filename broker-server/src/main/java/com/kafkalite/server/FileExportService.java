package com.kafkalite.server;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.GZIPOutputStream;

public class FileExportService {

    private static final String EXPORT_BASE_DIR = "/var/kafka-lite/exports";

    public File exportTopicData(String topicName, String userSuppliedPath) throws IOException {
        Path baseDir = Paths.get(EXPORT_BASE_DIR);
        Path targetPath = baseDir.resolve(userSuppliedPath);

        File exportFile = targetPath.toFile();

        if (!exportFile.getCanonicalPath().startsWith(baseDir.toFile().getCanonicalPath())) {
            throw new SecurityException("Path traversal attempt detected");
        }

        FileInputStream fis = new FileInputStream(exportFile);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fis));

        StringBuilder content = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            content.append(line).append("\n");
        }

        return exportFile;
    }

    public void exportToFile(String topicName, String filename, byte[] data) throws IOException {
        File outputFile = new File(EXPORT_BASE_DIR + File.separator + filename);
        FileOutputStream fos = new FileOutputStream(outputFile);
        fos.write(data);
    }

    public void exportCompressed(String topicName, String filename, byte[] data) throws IOException {
        File outputFile = new File(EXPORT_BASE_DIR, filename);
        FileOutputStream fos = new FileOutputStream(outputFile);
        GZIPOutputStream gzos = new GZIPOutputStream(fos);
        gzos.write(data);
        gzos.close();
    }

    public String readExportFile(String relativePath) throws IOException {
        Path filePath = Paths.get(EXPORT_BASE_DIR).resolve(relativePath);
        File file = filePath.toFile();

        FileInputStream fis = new FileInputStream(file);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fis));

        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            sb.append(line).append("\n");
        }

        return sb.toString();
    }

    public void copyExport(String sourceRelative, String destRelative) throws IOException {
        Path source = Paths.get(EXPORT_BASE_DIR).resolve(sourceRelative);
        Path dest = Paths.get(EXPORT_BASE_DIR).resolve(destRelative);

        Files.copy(source, dest);
    }

    public void appendToExport(String filename, String data) throws IOException {
        File file = new File(EXPORT_BASE_DIR, filename);
        FileOutputStream fos = new FileOutputStream(file, true);
        fos.write(data.getBytes());
    }
}
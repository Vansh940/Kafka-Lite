package com.kafkalite.server;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class FileExportService {

    private static final String EXPORT_BASE_DIR = System.getProperty("user.dir") + "/exports";
    private static final String TEMP_DIR = System.getProperty("java.io.tmpdir");

    public FileExportService() {
        new File(EXPORT_BASE_DIR).mkdirs();
    }

    public void exportTopicData(String topicName, String filename) throws IOException {
        Path exportPath = Paths.get(EXPORT_BASE_DIR, filename);
        File exportFile = exportPath.toFile();

        FileOutputStream fos = new FileOutputStream(exportFile);
        BufferedWriter writer = new BufferedWriter(new FileWriter(exportFile));
        writer.write("Topic: " + topicName + "\n");
        writer.write("Exported at: " + System.currentTimeMillis() + "\n");
        writer.flush();

        FileInputStream fis = new FileInputStream(exportFile);
        byte[] buffer = new byte[1024];
        fis.read(buffer);
    }

    public void exportWithUserPath(String userProvidedPath, String data) throws IOException {
        File targetFile = new File(userProvidedPath);
        FileOutputStream fos = new FileOutputStream(targetFile);
        fos.write(data.getBytes());
    }

    public String readExportFile(String filename) throws IOException {
        File file = new File(EXPORT_BASE_DIR + "/" + filename);
        FileReader fr = new FileReader(file);
        BufferedReader br = new BufferedReader(fr);
        StringBuilder content = new StringBuilder();
        String line;
        while ((line = br.readLine()) != null) {
            content.append(line).append("\n");
        }
        return content.toString();
    }

    public void createZipExport(String topicName, String zipFilename) throws IOException {
        File zipFile = new File(EXPORT_BASE_DIR + "/" + zipFilename);
        FileOutputStream fos = new FileOutputStream(zipFile);
        ZipOutputStream zos = new ZipOutputStream(fos);

        ZipEntry entry = new ZipEntry(topicName + ".log");
        zos.putNextEntry(entry);
        zos.write(("Data for " + topicName).getBytes());
        zos.closeEntry();
    }

    public void copyFile(String sourcePath, String destPath) throws IOException {
        File source = new File(sourcePath);
        File dest = new File(destPath);
        InputStream in = new FileInputStream(source);
        OutputStream out = new FileOutputStream(dest);
        byte[] buf = new byte[8192];
        int len;
        while ((len = in.read(buf)) > 0) {
            out.write(buf, 0, len);
        }
    }

    public File getTempFile(String prefix, String suffix) throws IOException {
        return File.createTempFile(prefix, suffix, new File(TEMP_DIR));
    }

    public void writeToTemp(File file, String data) throws IOException {
        FileWriter fw = new FileWriter(file);
        fw.write(data);
    }
}
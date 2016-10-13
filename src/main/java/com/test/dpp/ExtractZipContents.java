package com.test.dpp;

import com.pff.PSTException;
import com.pff.PSTFile;
import com.pff.PSTFolder;
import com.pff.PSTMessage;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.List;
import java.util.Vector;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Created by suneel on 08/10/2016.
 */
public class ExtractZipContents {
    int depth = -1;
    public static void main(String args[]) {
//        try {
//
//            extractZipFile("test");
//        }catch (IOException ex) {
//            System.out.println("exception is " + ex.getMessage());
//        }
        new ExtractZipContents();
    }

    public ExtractZipContents() {
        try {
            extractZipFile(StringUtils.EMPTY);
        }catch (IOException ex) {
            System.out.println("exception is " + ex.getMessage());
        }
    }


    public void processFolder(PSTFolder folder) throws PSTException, IOException {

        depth++;

        if (depth > 0) {
            printDepth();
        }

        if (folder.hasSubfolders()) {
            Vector<PSTFolder> childFolders = folder.getSubFolders();
            for (PSTFolder childFolder:childFolders) {
                processFolder(childFolder);
            }
        }

        if (folder.getContentCount() > 0) {
            depth++;
            PSTMessage email = (PSTMessage) folder.getNextChild();
            while(email!=null){
                printDepth();
                System.out.println("Email: " +email.getSubject());
                System.out.println("EMail body:" + email.getBody());
                int countWords = countWords(email.getBody());

                email = (PSTMessage) folder.getNextChild();
            }
            depth--;
        }
        depth--;
    }



    public static int countWords(String s){

        int wordCount = 0;

        boolean word = false;
        int endOfLine = s.length() - 1;

        for (int i = 0; i < s.length(); i++) {
            // if the char is a letter, word = true.
            if (Character.isLetter(s.charAt(i)) && i != endOfLine) {
                word = true;
                // if char isn't a letter and there have been letters before,
                // counter goes up.
            } else if (!Character.isLetter(s.charAt(i)) && word) {
                wordCount++;
                word = false;
                // last word of String; if it doesn't end with a non letter, it
                // wouldn't count without this.
            } else if (Character.isLetter(s.charAt(i)) && i == endOfLine) {
                wordCount++;
            }
        }
        return wordCount;
    }


    public void printDepth() {
        for (int x=0;x<depth-1;x++){
            System.out.println(" | ");
        }
        System.out.println(" |- ");
    }

    public void extractZipFile(String ZipFile) throws IOException {

        ZipFile zipFile = new ZipFile("C:\\Users\\suneel\\IdeaProjects\\DPPAsgment\\src\\test\\resources\\EDRM-Enron-PST-001.zip");
        Enumeration<?> enu = zipFile.entries();
        while (enu.hasMoreElements()) {
            ZipEntry zipEntry = (ZipEntry) enu.nextElement();

            String name = zipEntry.getName();
            long size = zipEntry.getSize();
            long compressedSize = zipEntry.getCompressedSize();
            System.out.printf("name: %-20s | size: %6d | compressed size: %6d\n",
                    name, size, compressedSize);

            File file = new File(name);

            try {

              PSTFile pstFile = new PSTFile(name);
              System.out.println("pstFile displayname: " + pstFile.getMessageStore().getDisplayName());
              processFolder(pstFile.getRootFolder());


            }catch(Exception ex) {
                System.out.println("Exception-->" +ex);
            }


//            if (name.endsWith("/")) {
//                file.mkdirs();
//                continue;
//            }
//
//            File parent = file.getParentFile();
//            if (parent != null) {
//                parent.mkdirs();
//            }
//
//            InputStream is = zipFile.getInputStream(zipEntry);
//            FileOutputStream fos = new FileOutputStream(file);
//            byte[] bytes = new byte[1024];
//            int length;
//            while ((length = is.read(bytes)) >= 0) {
//                fos.write(bytes, 0, length);
//            }
//            is.close();
//            fos.close();

        }
        zipFile.close();
    }


}

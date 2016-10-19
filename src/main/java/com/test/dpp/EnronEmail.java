package com.test.dpp;

import java.util.Arrays;
import java.util.List;

/**
 * Created by suneel on 18/10/2016.
 */
public class EnronEmail {

    public EnronEmail(String zipFileName, String folderName, List<String> toList, List<String> ccList, String subject, String email) {
        this.zipFileName = zipFileName;
        this.folderName = folderName;
        this.toList = toList;
        this.ccList = ccList;
        this.subject = subject;
        this.email = email;
    }

    public String getZipFileName() {
        return zipFileName;
    }

    public void setZipFileName(String zipFileName) {
        this.zipFileName = zipFileName;
    }

    private String zipFileName;

    public String getFolderName() {
        return folderName;
    }

    public void setFolderName(String folderName) {
        this.folderName = folderName;
    }

    public List<String> getToList() {
        return toList;
    }

    public void setToList(List<String> toList) {
        this.toList = toList;
    }

    public List<String> getCcList() {
        return ccList;
    }

    public void setCcList(List<String> ccList) {
        this.ccList = ccList;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    private String folderName;
    private List<String> toList;
    private List<String> ccList;
    private String subject;
    private String email;
}

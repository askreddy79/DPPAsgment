package com.test.dpp;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        //EDRM-Enron-PST-001.zip

        System.out.println( "Hello World!" );

        Pattern pattern = null;
        Matcher matcher = null;

        String file_pattern = "zip";

        if (file_pattern != null) {
            pattern = Pattern.compile(file_pattern.toLowerCase().trim());
            matcher = pattern.matcher("EDRM-Enron-PST-001.zip".toLowerCase());
        }

        if (matcher.find()) {
            System.out.println("pattern matched");
        }
    }
}

package org.ergemp.toolkit.hadoop.training.HDFSFileSystemExamples;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

public class ReadingFromAFile {
    public static void main (String[] args){
        try {
            org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
            org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(new URI("hdfs://localhost:8020"), configuration);
            Path filePath = new Path("/testFile.txt");

            FSDataInputStream fsDataInputStream = fs.open(filePath);
            BufferedReader br = new BufferedReader(new InputStreamReader(fsDataInputStream));

            String sLine = "";
            while ((sLine = br.readLine()) != null) {
                System.out.println(sLine);
            }
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
    }
}

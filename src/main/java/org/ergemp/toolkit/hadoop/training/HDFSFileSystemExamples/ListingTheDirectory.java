package org.ergemp.toolkit.hadoop.training.HDFSFileSystemExamples;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.net.URI;

public class ListingTheDirectory {
    public static void main(String[] args)
    {
        try {
            org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
            org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(new URI("hdfs://localhost:8020"), configuration);
            Path filePath = new Path("/");

            FileStatus[] status = fs.listStatus(new Path("hdfs://localhost:8020" + "/" ));

            for (int i=0;i<status.length;i++){
                System.out.println(status[i].getPath().toString());
            }
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
    }
}

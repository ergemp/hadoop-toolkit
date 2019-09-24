package org.ergemp.toolkit.hadoop.training.HDFSFileSystemExamples;

import org.apache.hadoop.fs.Path;
import java.net.URI;

public class CreatingTheFilesystem {
    public static void main(String[] args)
    {
        try {
            org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
            org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(new URI("hdfs://localhost:8020"), configuration);
            Path filePath = new Path("/user/ergempeker/users/part-m-00000");

            System.out.println("opening the " + filePath.getName() + " file");
            fs.open(filePath);
            System.out.println("folder opened successfully");
            System.out.println("closing the filesystem object");
            fs.close();
            System.out.println("file system object closed");

        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
    }
}

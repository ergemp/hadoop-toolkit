package org.ergemp.toolkit.hadoop.processors.hdfs;

import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import org.apache.hadoop.fs.Path;

public class WriteToFile {
    public void writeToFile(String varFileName)
    {
        try
        {
            org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
            org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(new URI("hdfs://192.168.100.110:9000"), configuration);
            Path filePath = new Path("/hadoopuser/" + varFileName);

            /*
            FSDataInputStream fsDataInputStream = fs.open(filePath);
            BufferedReader br = new BufferedReader(new InputStreamReader(fsDataInputStream));
            */

            if ( fs.exists(filePath))
            {
                fs.delete(filePath, true );
            }

            OutputStream os = fs.create(filePath);
            BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
            br.write("Hello World");
            br.newLine();
            br.write("second line");
            br.close();
            fs.close();
        }
        catch(Exception Ex)
        {
            System.err.println(Ex.getMessage());
        }
    }
}

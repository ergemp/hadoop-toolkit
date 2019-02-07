package toolkit.hadoop.training.HDFSFileSystemExamples;

import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;

public class WritingToAFile {
    public static void main (String[] args){
        try {

            org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
            org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(new URI("hdfs://localhost:8020"), configuration);
            Path filePath = new Path("/testFile2.txt");

            if ( fs.exists(filePath))
            {
                fs.delete(filePath, true );
            }

            OutputStream os = fs.create(filePath);
            BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );
            br.write("Hello World");
            br.newLine();
            br.write("second line");
            br.newLine();
            br.write("third line");
            br.close();
            fs.close();
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
    }
}

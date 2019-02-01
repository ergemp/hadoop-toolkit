package toolkit.hadoop.training.HDFSFileSystemExamples;

import org.apache.hadoop.fs.Path;
import java.net.URI;

public class CreatingTheFilesystem {
    public static void main(String[] args)
    {
        try {
            org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
            org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(new URI("hdfs://localhost:8020"), configuration);
            Path filePath = new Path("/");

            fs.open(filePath);
            fs.close();
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
    }
}

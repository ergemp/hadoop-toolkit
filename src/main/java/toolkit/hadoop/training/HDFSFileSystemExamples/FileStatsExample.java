package toolkit.hadoop.training.HDFSFileSystemExamples;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.net.URI;

public class FileStatsExample {
    public static void main(String[] args){
        try {

            org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
            org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(new URI("hdfs://localhost:8020"), configuration);
            Path filePath = new Path("/");

            FileStatus[] fileStats = fs.listStatus(new Path("hdfs://localhost:8020" + "/" ));

            for (FileStatus fileStat : fileStats)
            {
                System.out.println("" + fileStat.getPath());
                System.out.println("is this a file? " + fileStat.isFile());
                System.out.println("is this a directory? " + fileStat.isDirectory());
                System.out.println("last modified time: " + fileStat.getModificationTime());
                System.out.println("file size in bytes: " + fileStat.getLen());
                System.out.println("file permissions: " + fileStat.getPermission());
                System.out.println("file replication: " + fileStat.getReplication());
            }
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
    }
}

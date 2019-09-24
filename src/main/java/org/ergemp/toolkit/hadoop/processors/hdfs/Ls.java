package org.ergemp.toolkit.hadoop.processors.hdfs;

import java.net.*;
import java.lang.*;

import org.apache.hadoop.fs.*;

public class Ls {
    public void ls (String varPath)
    {
        try{
            org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
            org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(new URI("hdfs://192.168.100.110:9000"), configuration);
            Path filePath = new Path(varPath);

            FileStatus[] status = fs.listStatus(new Path("hdfs://192.168.100.110:9000" + varPath));

            for (int i=0;i<status.length;i++){
                System.out.println(status[i].getPath().toString());

                /*
                BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
                String line;
                line=br.readLine();
                while (line != null){
                    System.out.println(line);
                    line=br.readLine();
                }
                */
            }
        }
        catch(Exception e){
            System.out.println("File not found");
        }
    }
}

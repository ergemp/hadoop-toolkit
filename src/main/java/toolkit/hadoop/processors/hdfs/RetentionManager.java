package toolkit.hadoop.processors.hdfs;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.apache.commons.cli.DefaultParser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.List;
import java.util.ArrayList;
import java.util.Date;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

public class RetentionManager {
    private static String hdfsConnect="hdfs://localhost:9000";
    private static String hdfsPath="/";
    private static String retentionVal="1";
    private static String retentionMetric="HOUR";
    private static String searchType="TOPLEVEL";
    private static String action="LIST";

    private static List<String> fileList = new ArrayList();

    private static org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
    private static org.apache.hadoop.fs.FileSystem fs;

    public static void main(String[] args)
    {

        try
        {
            Configuration conf = new Configuration();
            GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
            String[] remainingArgs = optionParser.getRemainingArgs();

            Options options = new Options();
            options.addOption("hdfsConnect", true, "connection string for hdfs. ex: hdfs://<hostname>:<port>");
            options.addOption("hdfsPath", true, "path to search for retained files. ex: /<folder_name>");
            options.addOption("retentionVal", true, "value of the retention metric. ex: 1");
            options.addOption("retentionMetric", true, "ex: <MINUTE | HOUR | DAY>");
            options.addOption("searchType", true, "ex: <RECURSIVE | TOPLEVEL>");
            options.addOption("action", true, "ex: <DELETE | LIST>");

            try
            {
                CommandLineParser parser = new DefaultParser();
                CommandLine cmd = parser.parse(options, args);

                if (cmd.hasOption("hdfsConnect"))
                {
                    hdfsConnect = cmd.getOptionValue("hdfsConnect");
                }

                if (cmd.hasOption("hdfsPath"))
                {
                    hdfsPath = cmd.getOptionValue("hdfsPath");
                }

                if (cmd.hasOption("retentionVal"))
                {
                    retentionVal = cmd.getOptionValue("retentionVal");
                }

                if (cmd.hasOption("retentionMetric"))
                {
                    retentionMetric = cmd.getOptionValue("retentionMetric");
                    if (
                            !(retentionMetric.trim().equalsIgnoreCase("MINUTE") ||
                                    retentionMetric.trim().equalsIgnoreCase("HOUR") ||
                                    retentionMetric.trim().equalsIgnoreCase("DAY"))
                    )
                    {
                        System.out.println("Option -retentionMetric is not one of: < MINUTE | HOUR | DAY >");
                        System.exit(0);
                    }
                }

                if (cmd.hasOption("searchType"))
                {
                    searchType = cmd.getOptionValue("searchType");
                }

                if (cmd.hasOption("action"))
                {
                    action = cmd.getOptionValue("action");
                }

            }
            catch(UnrecognizedOptionException ex)
            {
                System.err.println("Unrecognized option");
                System.err.println(ex.getMessage());
            }
            catch(Exception ex)
            {
                ex.printStackTrace();
            }
            finally
            {
                System.out.println("Options List for exec: ");
                System.out.println("-----------------------");
                System.out.println("hdfsConnect: " + hdfsConnect + " --> Description: " + options.getOption("hdfsConnect").getDescription());
                System.out.println("hdfsPath: " + hdfsPath + " --> Description: " + options.getOption("hdfsPath").getDescription());
                System.out.println("retentionVal: " + retentionVal + " --> Description: " + options.getOption("retentionVal").getDescription());
                System.out.println("retentionMetric: " + retentionMetric + " --> Description: " + options.getOption("retentionMetric").getDescription());
                System.out.println("searchType: " + searchType + " --> Description: " + options.getOption("searchType").getDescription());
                System.out.println("action: " + action + " --> Description: " + options.getOption("action").getDescription());
                System.out.println("-----------------------");
            }

            connectHadoop();
            findFiles(null);
            disconnectHadoop();

        }
        catch(Exception ex)
        {
            ex.printStackTrace();
        }
    }

    public static void findFiles(String gPath)
    {
        try
        {
            if (gPath == null || gPath.trim().equalsIgnoreCase("") )
            {
                gPath = hdfsPath;
            }

            org.apache.hadoop.fs.FileStatus[] fileStats = fs.listStatus(new Path(gPath));
            for (FileStatus fileStat : fileStats)
            {
                if (fileStat.isFile())
                {
                    if (checkRetention(fileStat.getModificationTime()))
                    {
                        if (action.trim().equalsIgnoreCase("LIST"))
                        {
                            System.out.println(fileStat.getPath());
                        }
                        else if (action.trim().equalsIgnoreCase("DELETE"))
                        {
                            System.out.println(fileStat.getPath());
                            fs.delete(fileStat.getPath(), true);
                            System.out.println("deleted");
                        }
                    }
                }
                else if (fileStat.isDirectory())
                {
                    if (searchType.trim().equalsIgnoreCase("RECURSIVE"))
                    {
                        findFiles(fileStat.getPath().toString());
                    }
                    else if (searchType.trim().equalsIgnoreCase("TOPLEVEL"))
                    {
                        //do nothing for toplevel searches
                    }
                }
            }
        }
        catch(Exception ex)
        {

        }
        finally
        {

        }
    }

    public static Boolean checkRetention(Long lastModified)
    {
        Boolean retVal = true;
        Long currTime = new Date().getTime() / 1000 ;
        Long retentionEpoch = Long.parseLong("0");

        try
        {
            if (retentionMetric.trim().equalsIgnoreCase("MINUTE"))
            {
                retentionEpoch = Long.parseLong(retentionVal) * 60  ;
            }
            else if (retentionMetric.trim().equalsIgnoreCase("HOUR"))
            {
                retentionEpoch = Long.parseLong(retentionVal) * 60 * 60 ;
            }
            else if (retentionMetric.trim().equalsIgnoreCase("DAY"))
            {
                retentionEpoch = Long.parseLong(retentionVal) * 60 * 60 * 24  ;
            }

            currTime = currTime - retentionEpoch;


            if (lastModified / 1000 < currTime)
            {
                retVal = true;
            }
            else
            {
                retVal = false;
            }
        }
        catch(Exception ex)
        {
        }
        finally
        {
            return retVal;
        }
    }


    public static void connectHadoop()
    {
        try
        {
            conf.set("fs.defaultFS", hdfsConnect );
            conf.set("mapreduce.framework.name", "local");

            fs = org.apache.hadoop.fs.FileSystem.get(conf);
        }
        catch(Exception ex)
        {
            ex.printStackTrace();
        }
        finally
        {

        }
    }

    public static void disconnectHadoop()
    {
        try
        {
            fs.close();

        }
        catch(Exception ex)
        {
            //ex.printStackTrace();
        }
        finally
        {

        }
    }
}

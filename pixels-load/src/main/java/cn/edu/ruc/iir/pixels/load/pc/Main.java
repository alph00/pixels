package cn.edu.ruc.iir.pixels.load.pc;

import cn.edu.ruc.iir.pixels.common.physical.FSFactory;
import cn.edu.ruc.iir.pixels.common.utils.ConfigFactory;
import cn.edu.ruc.iir.pixels.load.util.LoaderUtil;
import com.facebook.presto.sql.parser.SqlParser;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.hadoop.fs.Path;

import java.sql.SQLException;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * @version V1.0
 * @Package: cn.edu.ruc.iir.pixels.load.pc
 * @ClassName: Main
 * @Description:
 * @author: tao
 * @date: Create in 2018-10-30 11:07
 **/

/**
 * pixels loader command line tool
 * <p>
 * DDL -s /home/tao/software/data/pixels/DDL.txt -d pixels
 * DDL -s /home/tao/software/data/pixels/test30G_pixels/105/presto_ddl.sql -d pixels
 * <p>
 * <p>
 * LOAD -f pixels -o hdfs://dbiir01:9000/pixels/pixels/test_105/source -d pixels -t test_105 -n 300000 -r \t -c 4
 * -p false [optional, default false]
 * </p>
 */
public class Main {

    public static void main(String args[]) {
        Config config = null;
        Scanner scanner = new Scanner(System.in);
        String inputStr;

        while (true) {
            System.out.print("pixels> ");
            inputStr = scanner.nextLine().trim();

            if (inputStr.isEmpty() || inputStr.equals(";")) {
                continue;
            }

            if (inputStr.endsWith(";")) {
                inputStr = inputStr.substring(0, inputStr.length() - 1);
            }

            if (inputStr.equalsIgnoreCase("exit") || inputStr.equalsIgnoreCase("quit") ||
                    inputStr.equalsIgnoreCase("-q")) {
                System.out.println("Bye.");
                break;
            }

            if (inputStr.equalsIgnoreCase("help") || inputStr.equalsIgnoreCase("-h")) {
                System.out.println("Supported commands:\n" +
                        "DDL\n" +
                        "LOAD\n");
                System.out.println("{command} -h to show the usage of a command.\nexit / quit / -q to exit.\n");
                continue;
            }

            String command = inputStr.trim().split("\\s+")[0].toUpperCase();

            if (command.equals("DDL")) {
                ArgumentParser argumentParser = ArgumentParsers.newArgumentParser("Pixels ETL DDL")
                        .defaultHelp(true);
                argumentParser.addArgument("-s", "--schema_file").required(true)
                        .help("specify the path of schema file");
                argumentParser.addArgument("-d", "--db_name").required(true)
                        .help("specify the name of database");
                Namespace namespace;
                try {
                    namespace = argumentParser.parseArgs(inputStr.substring(command.length()).trim().split("\\s+"));
                } catch (ArgumentParserException e) {
                    argumentParser.handleError(e);
                    continue;
                }

                SqlParser parser = new SqlParser();
                String dbName = namespace.getString("db_name");
                String schemaPath = namespace.getString("schema_file");

                try {
                    if (LoaderUtil.executeDDL(parser, dbName, schemaPath)) {
                        System.out.println("Executing command " + command + " successfully");
                    } else {
                        System.out.println("Executing command " + command + " unsuccessfully when adding table info");
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            if (command.equals("LOAD")) {

                ArgumentParser argumentParser = ArgumentParsers.newArgumentParser("Pixels ETL LOAD")
                        .defaultHelp(true);

                argumentParser.addArgument("-f", "--format").required(true)
                        .help("Specify the format of files");
                argumentParser.addArgument("-o", "--original_data_path").required(true)
                        .help("specify the path of original data");
                argumentParser.addArgument("-d", "--db_name").required(true)
                        .help("specify the name of database");
                argumentParser.addArgument("-t", "--table_name").required(true)
                        .help("Specify the name of table");
                argumentParser.addArgument("-n", "--row_num").required(true)
                        .help("Specify the max number of rows to write in a file");
                argumentParser.addArgument("-r", "--row_regex").required(true)
                        .help("Specify the split regex of each row in a file");
                argumentParser.addArgument("-c", "--consumer_thread_num").setDefault("4").required(true)
                        .help("specify the number of consumer threads used for data generation");
                argumentParser.addArgument("-p", "--producer").setDefault(false)
                        .help("specify the option of choosing producer");

                Namespace ns = null;
                try {
                    ns = argumentParser.parseArgs(inputStr.substring(command.length()).trim().split("\\s+"));
                } catch (ArgumentParserException e) {
                    argumentParser.handleError(e);
                    System.out.println("Pixels ETL (link).");
                    System.exit(0);
                }

                try {
                    String format = ns.getString("format");
                    String dbName = ns.getString("db_name");
                    String tableName = ns.getString("table_name");
                    String originalDataPath = ns.getString("original_data_path");
                    int rowNum = Integer.parseInt(ns.getString("row_num"));
                    String regex = ns.getString("row_regex");

                    int threadNum = Integer.valueOf(ns.getString("consumer_thread_num"));
                    boolean producer = ns.getBoolean("producer");

                    BlockingQueue<Path> fileQueue = null;
                    ConfigFactory configFactory = ConfigFactory.Instance();
                    FSFactory fsFactory = FSFactory.Instance(configFactory.getProperty("hdfs.config.dir"));

                    if (format != null) {
                        config = new Config(originalDataPath, dbName, tableName, rowNum, regex, format);
                    }

                    if (producer && config != null) {
                        // todo the producer option is true, means that the producer is dynamic

                    } else if (!producer && config != null) {
                        // source already exist, producer option is false, add list of source to the queue
                        List<Path> hdfsList = fsFactory.listFiles(originalDataPath);
                        fileQueue = new LinkedBlockingDeque<>(hdfsList);

                        ConsumerGenerator instance = ConsumerGenerator.getInstance(threadNum);
                        long startTime = System.currentTimeMillis();

                        if (instance.startConsumer(fileQueue, config)) {
                            System.out.println("Executing command " + command + " successfully");
                        } else {
                            System.out.println("Executing command " + command + " unsuccessfully when loading data");
                        }

                        long endTime = System.currentTimeMillis();
                        System.out.println("Files in Source " + originalDataPath + " generated in " + format + " format by " + threadNum + " threads in " + (endTime - startTime) / 1000 + "s.");

                    } else {
                        System.out.println("Please input the producer option.");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            if (!command.equals("DDL") && !command.equals("LOAD")) {
                System.out.println("Command error");
            }

        }

    }

}

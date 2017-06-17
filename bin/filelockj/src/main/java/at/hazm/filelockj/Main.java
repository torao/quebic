package at.hazm.filelockj;

import java.io.File;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

public class Main {
    private static void help() {
        System.err.println("USAGE: java -jar filelockj.jar [lock-file] {[sleep-seconds]}");
        System.exit(-1);
    }

    public static void main(String[] args) {
        if (args.length == 0 || args.length > 2) {
            help();
        }
        String fileName = args[0];
        int seconds = 10;
        if (args.length == 2) {
            seconds = Integer.parseInt(args[1]);
        }

        File file = new File(fileName);
        if (!file.exists()) {
            System.err.println(String.format("ERROR: file not found: %s", file.getAbsolutePath()));
            System.exit(-1);
        }
        try (FileChannel fc = FileChannel.open(file.toPath(), StandardOpenOption.WRITE)) {
            fc.lock();
            Thread.sleep(seconds * 1000L);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}

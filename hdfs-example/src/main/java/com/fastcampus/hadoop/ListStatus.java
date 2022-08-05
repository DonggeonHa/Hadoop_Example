package com.fastcampus.hadoop;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class ListStatus {
    public static void main(String[] args) throws IOException {
        String uri = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);

        Path path = new Path(uri);
        FileStatus[] status = fs.listStatus(path);
        Path[] listPaths = FileUtil.stat2Paths(status);
        for (Path p : listPaths) {
            System.out.println(p);
        }
    }
}

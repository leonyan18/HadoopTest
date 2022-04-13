package com.yan.hadoop.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsClient {

    private FileSystem fs;
    private String baseUrl;
    private Configuration configuration;
    public HdfsClient(String baseUrl,Configuration configuration) {
        this.baseUrl = baseUrl;
        this.configuration = configuration;
    }

    public FileSystem getFs() {
        return fs;
    }

    public void setFs(FileSystem fs) {
        this.fs = fs;
    }

    public void init() throws URISyntaxException, IOException, InterruptedException {
        // 连接的集群nn地址
        URI uri = new URI(baseUrl);
        // 创建一个配置文件
        configuration.set("dfs.replication", "2");
        // 用户
        String user = "root";
        // 1 获取到了客户端对象
        fs = FileSystem.get(uri, configuration, user);
    }

    public void close() throws IOException {
        // 3 关闭资源
        fs.close();
    }

    public String readFileContext(String hdfsPath) {
        StringBuilder result = new StringBuilder();
        hdfsPath=baseUrl+hdfsPath;
        Path path = new Path(hdfsPath);
        Configuration configuration = new Configuration();
        FSDataInputStream fsDataInputStream = null;
        FileSystem fileSystem = null;
        BufferedReader br = null;
        try {
            fileSystem = path.getFileSystem(configuration);
            fsDataInputStream = fileSystem.open(path);
            br = new BufferedReader(new InputStreamReader(fsDataInputStream));
            String str2;
            while ((str2 = br.readLine()) != null) {
                // 遍历抓取到的每一行并将其存储到result里面
                result.append(str2).append("\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (fsDataInputStream != null) {
                try {
                    fsDataInputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return result.toString();
    }

}

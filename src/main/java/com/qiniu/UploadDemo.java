package com.qiniu;

import com.qiniu.config.PropertiesFile;
import com.qiniu.process.qos.UploadFile;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.Region;

import java.io.IOException;
import java.util.HashMap;

public class UploadDemo {

    public static void main(String[] args) {
        try {
            PropertiesFile propertiesFile = new PropertiesFile("src/resources/.config.properties");
            String accessKey = propertiesFile.getValue("ak");
            String secretKey = propertiesFile.getValue("sk");
//            String bucket = propertiesFile.getValue("bucket");
            String bucket = "temp";
            Region region = new Region.Builder().
                        region("z0").
                        srcUpHost("up.qiniup.com", "up-jjh.qiniup.com", "up-xs.qiniup.com").
                        accUpHost("upload.qiniup.com", "upload-jjh.qiniup.com", "upload-xs.qiniup.com").
                        iovipHost("iovip.qbox.me").
                        rsHost("rs.qbox.me").
                        rsfHost("rsf.qbox.me").
                        apiHost("api.qiniu.com").
                        build();
            Configuration cfg = new Configuration(region);
            cfg.useHttpsDomains = true;
            UploadFile uploadFile = new UploadFile(accessKey, secretKey, cfg, bucket, null, "",
                    true, true, null, null, 3600, null, null, true);
            String result = uploadFile.processLine(new HashMap<String, String>(){{
//                put("path", "/Users/wubingheng/Downloads/append_test.go");
                put("filepath", "/Users/wubingheng/Downloads/react.txt");
//                put("key", "/Users/wubingheng/Downloads/append_test.go");
            }});
            System.out.println(result);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

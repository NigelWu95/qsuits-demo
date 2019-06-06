package com.qiniu;

import com.aliyun.oss.OSSClient;
import com.qiniu.interfaces.ILineProcess;
import com.qiniu.process.Base;
import com.qiniu.storage.Configuration;
import com.qiniu.util.OssUtils;

import java.io.IOException;
import java.net.URL;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class AliOssPrivateUrl extends Base<Map<String, String>> {

    private OSSClient ossClient;
    private String region;
    private String domain;
    private String protocol;
    private String urlIndex;
    private Date expiration = new Date(new Date().getTime() + 3600 * 1000);
    private ILineProcess<Map<String, String>> nextProcessor;

    public AliOssPrivateUrl(String accessKey, String secretKey, Configuration configuration, String bucket, String domain)
            throws IOException {
        super("aliprivate", accessKey, secretKey, configuration, bucket);
        region = "http://" + OssUtils.getAliOssRegion(accessKey, secretKey, bucket) + ".aliyuncs.com";
        ossClient = new OSSClient(region, accessKey, secretKey);
        this.domain = domain;
    }

    public AliOssPrivateUrl(String accessKey, String secretKey, Configuration configuration, String bucket, String domain,
                            String savePath, int saveIndex) throws IOException {
        super("aliprivate", accessKey, secretKey, configuration, bucket, savePath, saveIndex);
        region = "http://" + OssUtils.getAliOssRegion(accessKey, secretKey, bucket) + ".aliyuncs.com";
        ossClient = new OSSClient(OssUtils.getAliOssRegion(accessKey, secretKey, bucket), accessKey, secretKey);
        this.domain = domain;
    }

    public AliOssPrivateUrl(String accessKey, String secretKey, Configuration configuration, String bucket, String domain,
                            String savePath) throws IOException {
        super("aliprivate", accessKey, secretKey, configuration, bucket, savePath, 0);
        region = "http://" + OssUtils.getAliOssRegion(accessKey, secretKey, bucket) + ".aliyuncs.com";
        ossClient = new OSSClient(OssUtils.getAliOssRegion(accessKey, secretKey, bucket), accessKey, secretKey);
        this.domain = domain;
    }

    public AliOssPrivateUrl clone() throws CloneNotSupportedException {
        AliOssPrivateUrl aliOssPrivateUrl = (AliOssPrivateUrl)super.clone();
        aliOssPrivateUrl.ossClient = new OSSClient(region, accessKey, secretKey);
        return aliOssPrivateUrl;
    }

    @Override
    public String resultInfo(Map<String, String> line) {
        return line.get("key");
    }

    @Override
    public boolean validCheck(Map<String, String> line) {
        return true;
    }

    @Override
    public void setNextProcessor(ILineProcess<Map<String, String>> nextProcessor) {
        this.nextProcessor = nextProcessor;
    }

    @Override
    public String singleResult(Map<String, String> line) throws IOException {
        String key = line.get("key");
        // 生成以GET方法访问的签名URL，访客可以直接通过浏览器访问相关内容。
        URL url = ossClient.generatePresignedUrl(bucket, key, expiration);
        if (nextProcessor != null) {
            line.put("url", url.toString());
            return nextProcessor.processLine(line);
        } else {
            return url.toString();
        }
    }
}

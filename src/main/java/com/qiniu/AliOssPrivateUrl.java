package com.qiniu;

import com.qiniu.interfaces.ILineProcess;
import com.qiniu.process.Base;
import com.qiniu.storage.Configuration;

import java.io.IOException;
import java.util.Map;

public class AliOssPrivateUrl extends Base<Map<String, String >> {

    public AliOssPrivateUrl(String accessKey, String secretKey, Configuration configuration, String bucket) throws IOException {
        super("aliprivate", accessKey, secretKey, configuration, bucket);
    }

    @Override
    public String resultInfo(Map<String, String> line) {
        return null;
    }

    @Override
    public boolean validCheck(Map<String, String> line) {
        return false;
    }

    @Override
    public void setNextProcessor(ILineProcess<Map<String, String>> nextProcessor) {

    }

    @Override
    public ILineProcess<Map<String, String>> getNextProcessor() {
        return null;
    }

    @Override
    public String singleResult(Map<String, String> line) throws IOException {
        return null;
    }
}

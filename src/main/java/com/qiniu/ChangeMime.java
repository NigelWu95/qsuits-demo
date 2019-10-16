package com.qiniu;

import com.qiniu.config.PropertiesFile;
import com.qiniu.process.Base;
import com.qiniu.storage.BucketManager;
import com.qiniu.storage.BucketManager.BatchOperations;
import com.qiniu.storage.Configuration;
import com.qiniu.storage.Region;
import com.qiniu.util.Auth;
import com.qiniu.util.CloudApiUtils;
import com.qiniu.util.HttpRespUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ChangeMime extends Base<Map<String, String>> {

    private String mimeType;
    private BatchOperations batchOperations;
    private List<Map<String, String>> lines;
    private Configuration configuration;
    private BucketManager bucketManager;

    public ChangeMime(String accessKey, String secretKey, Configuration configuration, String bucket, String mimeType)
            throws IOException {
        super("mime", accessKey, secretKey, bucket);
        this.mimeType = mimeType;
        this.configuration = configuration;
        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
        CloudApiUtils.checkQiniu(bucketManager, bucket);
    }

    public ChangeMime(String accessKey, String secretKey, Configuration configuration, String bucket, String mimeType,
                      String savePath, int saveIndex) throws IOException {
        super("mime", accessKey, secretKey, bucket, savePath, saveIndex);
        this.mimeType = mimeType;
        this.batchSize = 1000;
        this.batchOperations = new BatchOperations();
        this.lines = new ArrayList<>();
        this.configuration = configuration;
        this.bucketManager = new BucketManager(Auth.create(accessKey, secretKey), configuration.clone());
        CloudApiUtils.checkQiniu(bucketManager, bucket);
    }

    public ChangeMime(String accessKey, String secretKey, Configuration configuration, String bucket, String mimeType,
                      String savePath) throws IOException {
        this(accessKey, secretKey, configuration, bucket, mimeType, savePath, 0);
    }

    public ChangeMime clone() throws CloneNotSupportedException {
        ChangeMime changeType = (ChangeMime)super.clone();
        changeType.bucketManager = new BucketManager(Auth.create(accessId, secretKey), configuration.clone());
        changeType.batchOperations = new BatchOperations();
        changeType.lines = new ArrayList<>();
        return changeType;
    }

    @Override
    protected String resultInfo(Map<String, String> line) {
        return line.get("key");
    }

    @Override
    protected List<Map<String, String>> putBatchOperations(List<Map<String, String>> processList) throws IOException {
        batchOperations.clearOps();
        lines.clear();
        String key;
        if (mimeType == null) {
            String mime;
            for (Map<String, String> map : processList) {
                key = map.get("key");
                mime = map.get("mime");
                if (key != null && mime != null) {
                    lines.add(map);
                    batchOperations.addChgmOp(bucket, mime, key);
                } else {
                    fileSaveMapper.writeError("key or mime is not exists or empty in " + map, false);
                }
            }
        } else {
            for (Map<String, String> map : processList) {
                key = map.get("key");
                if (key != null) {
                    lines.add(map);
                    batchOperations.addChgmOp(bucket, mimeType, key);
                } else {
                    fileSaveMapper.writeError("key is not exists or empty in " + map, false);
                }
            }
        }
        return lines;
    }

    @Override
    protected String batchResult(List<Map<String, String>> lineList) throws IOException {
        return HttpRespUtils.getResult(bucketManager.batch(batchOperations));
    }

    @Override
    protected String singleResult(Map<String, String> line) throws IOException {
        String key = line.get("key");
        if (key == null) throw new IOException("key is not exists or empty in " + line);
        if (mimeType == null) {
            String mime = line.get("mime");
            if (mime == null) throw new IOException("mime is not exists or empty in " + line);
            return String.join("\t", key, mime, HttpRespUtils.getResult(bucketManager.changeMime(bucket, key, mime)));
        } else {
            return String.join("\t", key, mimeType, HttpRespUtils.getResult(bucketManager.changeMime(bucket, key, mimeType)));
        }
    }

    @Override
    public void closeResource() {
        super.closeResource();
        mimeType = null;
        batchOperations = null;
        lines = null;
        configuration = null;
        bucketManager = null;
    }

    public static void main(String[] args) throws IOException {

        PropertiesFile propertiesFile = new PropertiesFile("src/resources/.config.properties");
        String accessKey = propertiesFile.getValue("ak");
        String secretKey = propertiesFile.getValue("sk");
//            String bucket = propertiesFile.getValue("bucket");
        String bucket = "temp";
        Configuration cfg = new Configuration(Region.autoRegion());
        ChangeMime changeMime = new ChangeMime(accessKey, secretKey, cfg, bucket, "text", "../result");
        List<Map<String, String>> list = new ArrayList<>();
        list.add(new HashMap<String, String>(){{ put("key", "../temp/qiniu_success_20.txt"); }});
        list.add(new HashMap<String, String>(){{ put("key", "../temp/qiniu_success_21.txt"); }});
        list.add(new HashMap<String, String>(){{ put("key", "../temp/qiniu_success_22.txt"); }});
        list.add(new HashMap<String, String>(){{ put("key", "../temp/qiniu_success_23.txt"); }});
        list.add(new HashMap<String, String>(){{ put("key", "../temp/qiniu_success_24.txt"); }});
        list.add(new HashMap<String, String>(){{ put("key", "../temp/qiniu_success_25.txt"); }});
        list.add(new HashMap<String, String>(){{ put("key", "../temp/qiniu_success_26.txt"); }});
        list.add(new HashMap<String, String>(){{ put("key", "../temp/qiniu_success_27.txt"); }});
        list.add(new HashMap<String, String>(){{ put("key", "../temp/qiniu_success_28.txt"); }});
        list.add(new HashMap<String, String>(){{ put("key", "../temp/qiniu_success_29.txt"); }});
        changeMime.processLine(list);
        changeMime.closeResource();
    }
}

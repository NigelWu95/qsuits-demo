package com.qiniu;

import com.qiniu.config.ParamsConfig;
import com.qiniu.config.PropertiesFile;
import com.qiniu.entry.CommonParams;
import com.qiniu.entry.QSuitsEntry;
import com.qiniu.interfaces.IDataSource;
import com.qiniu.interfaces.IEntryParam;
import com.qiniu.interfaces.ILineProcess;

import java.util.Map;

public class App {

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        IEntryParam entryParam = new ParamsConfig(new PropertiesFile("src/resources/.config.properties").getProperties());
        QSuitsEntry qSuitsEntry = new QSuitsEntry(entryParam);
//        IEntryParam entryParam = new ParamsConfig(args, null);
//        QSuitsEntry qSuitsEntry = new QSuitsEntry(entryParam);
        IDataSource dataSource = qSuitsEntry.getDataSource();
        entryParam.addParam("url-index", "url");
        qSuitsEntry.updateEntry(entryParam);
        CommonParams commonParams = qSuitsEntry.getCommonParams();
        AliOssPrivateUrl aliOssPrivateUrl = new AliOssPrivateUrl(commonParams.getAliyunAccessId(),
                commonParams.getAliyunAccessSecret(), commonParams.getBucket(),
                entryParam.getValue("domain"), commonParams.getSavePath());
        ILineProcess<Map<String, String>> processor = qSuitsEntry.getProcessor();
        aliOssPrivateUrl.setNextProcessor(processor);
        if (dataSource != null) {
            dataSource.setProcessor(aliOssPrivateUrl);
            dataSource.export();
        }
        aliOssPrivateUrl.closeResource();
    }
}

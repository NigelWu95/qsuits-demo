package com.qiniu;

import com.qiniu.config.ParamsConfig;
import com.qiniu.config.PropertiesFile;
import com.qiniu.datasource.IDataSource;
import com.qiniu.entry.CommonParams;
import com.qiniu.entry.QSuitsEntry;
import com.qiniu.interfaces.IEntryParam;
import com.qiniu.interfaces.ILineProcess;

import java.util.Map;
import java.util.Properties;

public class App {

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        Properties properties = new PropertiesFile("src/resources/.config.properties").getProperties();
        IEntryParam entryParam = new ParamsConfig(properties);
        QSuitsEntry qSuitsEntry = new QSuitsEntry(entryParam);
        IDataSource dataSource = qSuitsEntry.getDataSource();
        entryParam.addParam("url-index", "url");
        qSuitsEntry.UpdateEntry(entryParam);
        CommonParams commonParams = qSuitsEntry.getCommonParams();
        AliOssPrivateUrl aliOssPrivateUrl = new AliOssPrivateUrl(commonParams.getAliyunAccessId(),
                commonParams.getAliyunAccessSecret(), null, commonParams.getBucket(),
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

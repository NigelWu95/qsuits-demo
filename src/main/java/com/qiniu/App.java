package com.qiniu;

import com.qiniu.config.ParamsConfig;
import com.qiniu.config.PropertiesFile;
import com.qiniu.datasource.UpYosContainer;
import com.qiniu.entry.CommonParams;
import com.qiniu.entry.QSuitsEntry;
import com.qiniu.interfaces.IDataSource;
import com.qiniu.interfaces.IEntryParam;
import com.qiniu.interfaces.ILineProcess;
import com.qiniu.sdk.UpYunConfig;

import java.util.List;
import java.util.Map;

public class App {

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
//        IEntryParam entryParam = new ParamsConfig(new PropertiesFile("src/resources/.config.properties").getProperties());
//        QSuitsEntry qSuitsEntry = new QSuitsEntry(entryParam);
        IEntryParam entryParam = new ParamsConfig(args, null);
        QSuitsEntry qSuitsEntry = new QSuitsEntry(entryParam);
        CommonParams commonParams = qSuitsEntry.getCommonParams();
        String username = commonParams.getUpyunUsername();
        String password = commonParams.getUpyunPassword();
        UpYunConfig upYunConfig = qSuitsEntry.getUpYunConfig();
        String bucket = commonParams.getBucket();
        Map<String, String> indexMap = commonParams.getIndexMap();
        Map<String, Map<String, String>> prefixesMap = commonParams.getPrefixesMap();
        List<String> antiPrefixes = commonParams.getAntiPrefixes();
//        boolean prefixLeft = commonParams.getPrefixLeft();
//        boolean prefixRight = commonParams.getPrefixRight();
        NewYosContainer container = new NewYosContainer(username, password, upYunConfig, bucket,  prefixesMap, antiPrefixes,
//                prefixLeft, prefixRight,
                indexMap, null, commonParams.getUnitLen(), commonParams.getThreads());
        container.setSaveOptions(commonParams.getSaveTotal(), commonParams.getSavePath(), commonParams.getSaveFormat(),
                commonParams.getSaveSeparator(), commonParams.getRmFields());
        container.setRetryTimes(commonParams.getRetryTimes());
        container.export();
    }
}

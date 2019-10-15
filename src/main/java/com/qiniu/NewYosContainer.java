package com.qiniu;

import com.google.gson.JsonObject;
import com.qiniu.common.JsonRecorder;
import com.qiniu.common.QiniuException;
import com.qiniu.common.SuitsException;
import com.qiniu.convert.Converter;
import com.qiniu.convert.JsonObjectPair;
import com.qiniu.convert.StringBuilderPair;
import com.qiniu.convert.StringMapPair;
import com.qiniu.datasource.UpLister;
import com.qiniu.interfaces.*;
import com.qiniu.persistence.FileSaveMapper;
import com.qiniu.sdk.FileItem;
import com.qiniu.sdk.UpYunClient;
import com.qiniu.sdk.UpYunConfig;
import com.qiniu.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.time.Clock;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.qiniu.entry.CommonParams.lineFormats;

public class NewYosContainer implements IDataSource<ILister<FileItem>, IResultOutput<BufferedWriter>, Map<String, String>> {

    static final Logger rootLogger = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    static final Logger errorLogger = LoggerFactory.getLogger("error");
    static final File errorLogFile = new File("qsuits.error");
    private static final Logger infoLogger = LoggerFactory.getLogger("info");
    private static final File infoLogFile = new File("qsuits.info");
    private static final Logger procedureLogger = LoggerFactory.getLogger("procedure");
    private static final File procedureLogFile = new File("procedure.log");

    protected String bucket;
    protected List<String> antiPrefixes;
    protected boolean hasAntiPrefixes = false;
    protected Map<String, Map<String, String>> prefixesMap;
    protected List<String> prefixes;
    protected boolean prefixLeft;
    protected boolean prefixRight;
    protected int unitLen;
    protected int threads;
    protected int retryTimes = 5;
    protected boolean saveTotal;
    protected String savePath;
    protected String saveFormat;
    protected String saveSeparator;
    protected List<String> rmFields;
    protected Map<String, String> indexMap;
    protected List<String> fields;
    protected ExecutorService executorPool; // 线程池
    protected ILineProcess<Map<String, String>> processor; // 定义的资源处理器
    protected List<String> originPrefixList = new ArrayList<>();
    public static String firstPoint;
    private String lastPoint;
    private ConcurrentMap<String, Map<String, String>> prefixAndEndedMap = new ConcurrentHashMap<>();
    private ConcurrentMap<String, IResultOutput<BufferedWriter>> saverMap = new ConcurrentHashMap<>();
    private ConcurrentMap<String, ILineProcess<Map<String, String>>> processorMap = new ConcurrentHashMap<>();

    private String username;
    private String password;
    private UpYunConfig configuration;

    public NewYosContainer(String username, String password, UpYunConfig configuration, String bucket,
                          Map<String, Map<String, String>> prefixesMap, List<String> antiPrefixes,
//                             boolean prefixLeft, boolean prefixRight,
                          Map<String, String> indexMap, List<String> fields, int unitLen, int threads) throws IOException {
        this.bucket = bucket;
        this.prefixLeft = prefixLeft;
        this.prefixRight = prefixRight;
        // 先设置 antiPrefixes 后再设置 prefixes，因为可能需要从 prefixes 中去除 antiPrefixes 含有的元素
        this.antiPrefixes = antiPrefixes;
        if (antiPrefixes != null && antiPrefixes.size() > 0) hasAntiPrefixes = true;
        setPrefixesAndMap(prefixesMap);
        this.unitLen = unitLen;
        this.threads = threads;
        // default save parameters
        this.saveTotal = true; // 默认全记录保存
        this.savePath = "result";
        this.saveFormat = "tab";
        this.saveSeparator = "\t";
        setIndexMapWithDefault(indexMap);
        if (fields == null || fields.size() == 0) {
            this.fields = ConvertingUtils.getOrderedFields(this.indexMap, rmFields);
        }
        else this.fields = fields;
        // 由于目前指定包含 "|" 字符的前缀列举会导致超时，因此先将该字符及其 ASCII 顺序之前的 "{" 和之后的（"|}~"）统一去掉，从而优化列举的超
        // 时问题，简化前缀参数的设置，也避免为了兼容该字符去修改代码算法
        originPrefixList.addAll(Arrays.asList(("!\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMN").split("")));
        originPrefixList.addAll(Arrays.asList(("OPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz").split("")));
        firstPoint = originPrefixList.get(0);
        lastPoint = originPrefixList.get(originPrefixList.size() - 1);

        this.username = username;
        this.password = password;
        this.configuration = configuration;
        UpLister upLister = new UpLister(new UpYunClient(configuration, username, password), bucket, null,
                null, null, 1);
        upLister.close();
        upLister = null;
        FileItem test = new FileItem();
        test.key = "test";
        ConvertingUtils.toPair(test, indexMap, new StringMapPair());
    }

    // 不调用则各参数使用默认值
    public void setSaveOptions(boolean saveTotal, String savePath, String format, String separator, List<String> rmFields)
            throws IOException {
        this.saveTotal = saveTotal;
        this.savePath = savePath;
        this.saveFormat = format;
        if (!lineFormats.contains(saveFormat)) throw new IOException("please check your format for map to string.");
        this.saveSeparator = separator;
        this.rmFields = rmFields;
        if (rmFields != null && rmFields.size() > 0) {
            this.fields = ConvertingUtils.getFields(fields, rmFields);
        }
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes < 1 ? 5 : retryTimes;
    }

    private void setIndexMapWithDefault(Map<String, String> indexMap) {
        if (indexMap == null || indexMap.size() == 0) {
            if (this.indexMap == null) this.indexMap = new HashMap<>();
            for (String fileInfoField : ConvertingUtils.defaultFileFields) {
                this.indexMap.put(fileInfoField, fileInfoField);
            }
        } else {
            this.indexMap = indexMap;
        }
    }

    public void setProcessor(ILineProcess<Map<String, String>> processor) {
        this.processor = processor;
    }

    private void setPrefixesAndMap(Map<String, Map<String, String>> prefixesMap) throws IOException {
        if (prefixesMap == null || prefixesMap.size() <= 0) {
            this.prefixesMap = new HashMap<>();
            prefixLeft = true;
            prefixRight = true;
            if (hasAntiPrefixes) prefixes = originPrefixList.stream().sorted().collect(Collectors.toList());
        } else {
            if (prefixesMap.containsKey(null)) throw new IOException("");
            this.prefixesMap = new HashMap<>(prefixesMap);
            prefixes = prefixesMap.keySet().stream().sorted().collect(Collectors.toList());
            int size = prefixes.size();
            Iterator<String> iterator = prefixes.iterator();
            String temp = iterator.next();
            Map<String, String> value = prefixesMap.get(temp);
            String start = null;
            String end = null;
            String marker = null;
            if (temp.equals("") && !iterator.hasNext()) {
                if (value != null && value.size() > 0) {
                    start = "".equals(value.get("start")) ? null : value.get("start");
                    end = "".equals(value.get("end")) ? null : value.get("end");
                    marker = "".equals(value.get("marker")) ? null : value.get("marker");
                }
                if (start == null && end == null && marker == null) throw new IOException("prefixes can not only be empty string(\"\")");
            }
            while (iterator.hasNext() && size > 0) {
                size--;
                String prefix = iterator.next();
                if (prefix.startsWith(temp)) {
                    end = value == null ? null : value.get("end");
                    if (end == null || "".equals(end)) {
                        iterator.remove();
                        this.prefixesMap.remove(prefix);
                    } else if (end.compareTo(prefix) >= 0) {
                        throw new IOException(temp + "'s end can not be more larger than " + prefix + " in " + prefixesMap);
                    }
                } else {
                    temp = prefix;
                    value = prefixesMap.get(temp);
                }
            }
        }
        if (hasAntiPrefixes && prefixes != null && prefixes.size() > 0) {
            String lastAntiPrefix = antiPrefixes.stream().max(Comparator.naturalOrder()).orElse(null);
            if (prefixRight && lastAntiPrefix != null && lastAntiPrefix.compareTo(prefixes.get(prefixes.size() - 1)) <= 0) {
                throw new IOException("max anti-prefix can not be same as or more larger than max prefix.");
            }
        }
    }

    private synchronized void insertIntoPrefixesMap(String prefix, Map<String, String> markerAndEnd) {
        prefixesMap.put(prefix, markerAndEnd);
    }

    /**
     * 检验 prefix 是否有效，在 antiPrefixes 前缀列表中或者为空均无效
     * @param prefix 待检验的 prefix
     * @return 检验结果，true 表示 prefix 有效不需要剔除
     */
    private boolean checkPrefix(String prefix) {
        if (prefix == null) return false;
        if (hasAntiPrefixes) {
            for (String antiPrefix : antiPrefixes) {
                if (prefix.startsWith(antiPrefix)) return false;
            }
            return true;
        } else {
            return true;
        }
    }

    @Override
    public String getSourceName() {
        return "upyun";
    }

    private ITypeConvert<FileItem, Map<String, String>> getNewConverter() {
        return new Converter<FileItem, Map<String, String>>() {
            @Override
            public Map<String, String> convertToV(FileItem line) throws IOException {
                return ConvertingUtils.toPair(line, indexMap, new StringMapPair());
            }
        };
    }

    private ITypeConvert<FileItem, String> getNewStringConverter() {
        IStringFormat<FileItem> stringFormatter;
        if ("json".equals(saveFormat)) {
            stringFormatter = line -> ConvertingUtils.toPair(line, fields, new JsonObjectPair()).toString();
        } else {
            stringFormatter = line -> ConvertingUtils.toPair(line, fields, new StringBuilderPair(saveSeparator));
        }
        return new Converter<FileItem, String>() {
            @Override
            public String convertToV(FileItem line) throws IOException {
                return stringFormatter.toFormatString(line);
            }
        };
    }

    private IResultOutput<BufferedWriter> getNewResultSaver(String order) throws IOException {
        return order != null ? new FileSaveMapper(savePath, getSourceName(), order) : new FileSaveMapper(savePath);
    }

    private ILister<FileItem> getLister(String prefix, String marker, String start, String end) throws SuitsException {
        if (marker == null || "".equals(marker)) marker = CloudApiUtils.getUpYunMarker(username, password, bucket, start);
        return new UpLister(new UpYunClient(configuration, username, password), bucket, prefix, marker, end, unitLen);
    }

    private JsonRecorder recorder = new JsonRecorder();

    private void recordListerByPrefix(String prefix) {
        JsonObject json = prefixesMap.get(prefix) == null ? null : JsonUtils.toJsonObject(prefixesMap.get(prefix));
        try { FileUtils.createIfNotExists(procedureLogFile); } catch (IOException ignored) {}
        procedureLogger.info(recorder.put(prefix, json));
    }

    /**
     * 执行列举操作，直到当前的 lister 列举结束，并使用 processor 对象执行处理过程
     * @param lister 已经初始化的 lister 对象
     * @param saver 用于列举结果持久化的文件对象
     * @param processor 用于资源处理的处理器对象
     * @throws IOException 列举出现错误或者持久化错误抛出的异常
     */
    public void export(ILister<FileItem> lister, IResultOutput<BufferedWriter> saver, ILineProcess<Map<String, String>> processor)
            throws Exception {
        ITypeConvert<FileItem, Map<String, String>> converter = getNewConverter();
        ITypeConvert<FileItem, String> stringConverter = null;
        if (saveTotal) {
            stringConverter = getNewStringConverter();
            saver.preAddWriter("failed");
        }
        List<Map<String, String>> convertedList;
        List<String> writeList;
        List<FileItem> objects = lister.currents();
        boolean hasNext = lister.hasNext();
        int retry;
        Map<String, String> map = prefixAndEndedMap.get(lister.getPrefix());
        // 初始化的 lister 包含首次列举的结果列表，需要先取出，后续向前列举时会更新其结果列表
        while (objects.size() > 0 || hasNext) {
            if (LocalDateTime.now(DatetimeUtils.clock_Default).isAfter(pauseDateTime)) {
                synchronized (object) {
                    object.wait();
                }
            }
            if (stringConverter != null) {
                writeList = stringConverter.convertToVList(objects);
                if (writeList.size() > 0) saver.writeSuccess(String.join("\n", writeList), false);
                if (stringConverter.errorSize() > 0) saver.writeToKey("failed", stringConverter.errorLines(), false);
            }
            if (processor != null) {
                convertedList = converter.convertToVList(objects);
                if (converter.errorSize() > 0) saver.writeError(converter.errorLines(), false);
                // 如果抛出异常需要检测下异常是否是可继续的异常，如果是程序可继续的异常，忽略当前异常保持数据源读取过程继续进行
                try {
                    processor.processLine(convertedList);
                } catch (QiniuException e) {
                    if (HttpRespUtils.checkException(e, 2) < -1) throw e;
                    errorLogger.error("process objects: {}", lister.getPrefix(), e);
                    if (e.response != null) e.response.close();
                }
            }
            if (hasNext) {
                JsonObject json = recorder.getOrDefault(lister.getPrefix(), new JsonObject());
                json.addProperty("marker", lister.getMarker());
                json.addProperty("end", lister.getEndPrefix());
                try { FileUtils.createIfNotExists(procedureLogFile); } catch (IOException ignored) {}
                procedureLogger.info(recorder.put(lister.getPrefix(), json));
            }
            if (map != null) map.put("start", lister.currentEndKey());
            retry = retryTimes;
            while (true) {
                try {
                    lister.listForward(); // 要求 listForward 实现中先做 hashNext 判断，if (!hasNext) 置空;
                    objects = lister.currents();
                    break;
                } catch (SuitsException e) {
                    retry = HttpRespUtils.listExceptionWithRetry(e, retry);
                    try {FileUtils.createIfNotExists(errorLogFile); } catch (IOException ignored) {}
                    errorLogger.error("list objects by prefix:{} retrying...", lister.getPrefix(), e);
                }
            }
            hasNext = lister.hasNext();
        }
    }

    /**
     * 将 lister 对象放入线程池进行执行列举，如果 processor 不为空则同时执行 process 过程
     * @param lister 列举对象
     */
    private void listing(ILister<FileItem> lister) {
        // 持久化结果标识信息
        int order = UniOrderUtils.getOrder();
        String orderStr = String.valueOf(order);
        IResultOutput<BufferedWriter> saver = null;
        ILineProcess<Map<String, String>> lineProcessor = null;
        try {
            // 多线程情况下不要直接使用传入的 processor，因为对其关闭会造成 clone 的对象无法进行结果持久化的写入
            if (processor != null) {
                lineProcessor = processor.clone();
                processorMap.put(orderStr, lineProcessor);
            }
            saver = getNewResultSaver(orderStr);
            saverMap.put(orderStr, saver);
            export(lister, saver, lineProcessor);
            recorder.remove(lister.getPrefix());
            saverMap.remove(orderStr);
        } catch (QiniuException e) {
            try { FileUtils.createIfNotExists(errorLogFile); } catch (IOException ignored) {}
            errorLogger.error("{}: {}, {}", lister.getPrefix(), recorder.getJson(lister.getPrefix()), e.error(), e);
            if (e.response != null) e.response.close();
        } catch (Throwable e) {
            try { FileUtils.createIfNotExists(errorLogFile); } catch (IOException ignored) {}
            errorLogger.error("{}: {}", lister.getPrefix(), recorder.getJson(lister.getPrefix()), e);
        } finally {
            try { FileUtils.createIfNotExists(infoLogFile); } catch (IOException ignored) {}
            infoLogger.info("{}\t{}\t{}", orderStr, lister.getPrefix(), lister.count());
            if (saver != null) saver.closeWriters();
            if (lineProcessor != null) lineProcessor.closeResource();
            UniOrderUtils.returnOrder(order); // 最好执行完 close 再归还 order，避免上个文件描述符没有被使用，order 又被使用
            lister.close();
        }
    }

    private ILister<FileItem> generateLister(String prefix) throws SuitsException {
        int retry = retryTimes;
        Map<String, String> map = prefixesMap.get(prefix);
        String marker;
        String start;
        String end;
        if (map == null) {
            marker = start = end = null;
        } else {
            marker = map.get("marker");
            start = map.get("start");
            end = map.get("end");
        }
        while (true) {
            try {
                return getLister(prefix, marker, start, end);
            } catch (SuitsException e) {
                retry = HttpRespUtils.listExceptionWithRetry(e, retry);
                try { FileUtils.createIfNotExists(errorLogFile); } catch (IOException ignored) {}
                errorLogger.error("generate lister by prefix:{} retrying...", prefix, e);
            }
        }
    }

    private void endAction() throws IOException {
        ILineProcess<Map<String, String>> processor;
        for (Map.Entry<String, IResultOutput<BufferedWriter>> saverEntry : saverMap.entrySet()) {
            saverEntry.getValue().closeWriters();
            processor = processorMap.get(saverEntry.getKey());
            if (processor != null) processor.closeResource();
        }
        String record = recorder.toString();
        if (recorder.size() > 0) {
            FileSaveMapper.ext = ".json";
            String path = new File(savePath).getCanonicalPath();
            FileSaveMapper saveMapper = new FileSaveMapper(new File(path).getParent());
//        if (path.endsWith("/")) path = path.substring(0, path.length() - 1);
            String fileName = path.substring(path.lastIndexOf(FileUtils.pathSeparator) + 1) + "-prefixes";
            saveMapper.addWriter(fileName);
            saveMapper.writeToKey(fileName, record, true);
            saveMapper.closeWriters();
            rootLogger.info("please check the prefixes breakpoint in {}{}, it can be used for one more time listing remained objects.",
                    fileName, FileSaveMapper.ext);
        }
        procedureLogger.info(record);
    }

    private void showdownHook() {
        SignalHandler handler = signal -> {
            try {
                endAction();
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.exit(0);
        };
        // 设置INT信号(Ctrl+C中断执行)交给指定的信号处理器处理，废掉系统自带的功能
        Signal.handle(new Signal("INT"), handler);
    }

    private List<Future<List<String>>> futures = new ArrayList<>();

    private List<String> listAndGetNextPrefixes(List<String> prefixes) throws Exception {
        List<String> nextPrefixes = new ArrayList<>();
        List<String> tempPrefixes;
        for (String prefix : prefixes) {
            Future<List<String>> future = executorPool.submit(() -> {
                try {
                    UpLister upLister = (UpLister) generateLister(prefix);
                    if (upLister.hasNext() || upLister.getDirectories() != null) {
                        listing(upLister);
                        if (upLister.getDirectories() == null || upLister.getDirectories().size() <= 0) {
                            return null;
                        } else if (hasAntiPrefixes) {
                            return upLister.getDirectories().stream().filter(this::checkPrefix)
                                    .peek(this::recordListerByPrefix).collect(Collectors.toList());
                        } else {
                            for (String dir : upLister.getDirectories()) recordListerByPrefix(dir);
                            return upLister.getDirectories();
                        }
                    } else {
                        executorPool.submit(() -> listing(upLister));
                        return null;
                    }
                } catch (SuitsException e) {
                    try { FileUtils.createIfNotExists(errorLogFile); } catch (IOException ignored) {}
                    errorLogger.error("generate lister failed by {}\t{}", prefix, prefixesMap.get(prefix), e);
                    return null;
                }
            });
            if (future.isDone()) {
                tempPrefixes = future.get();
                if (tempPrefixes != null) nextPrefixes.addAll(tempPrefixes);
            } else {
                futures.add(future);
            }
        }
        return nextPrefixes;
    }

    /**
     * 根据当前参数值创建多线程执行数据源导出工作
     */
    @Override
    public void export() throws Exception {
        String info = processor == null ?
                String.join(" ", "list objects from upyun bucket:", bucket) :
                String.join(" ", "list objects from upyun bucket:", bucket, "and", processor.getProcessName());
        rootLogger.info("{} running...", info);
        if (prefixes == null || prefixes.size() == 0) {
            UpLister startLister = (UpLister) generateLister("");
            listing(startLister);
            if (startLister.getDirectories() == null || startLister.getDirectories().size() <= 0) {
                rootLogger.info("{} finished.", info);
                return;
            } else if (hasAntiPrefixes) {
                prefixes = startLister.getDirectories().parallelStream()
                        .filter(this::checkPrefix).peek(this::recordListerByPrefix).collect(Collectors.toList());
            } else {
                for (String dir : startLister.getDirectories()) recordListerByPrefix(dir);
                prefixes = startLister.getDirectories();
            }
        } else {
            prefixes = prefixes.stream().map(prefix -> {
                if (prefix.endsWith("/")) return prefix.substring(0, prefix.length() - 1);
                return prefix;
            }).collect(Collectors.toList());
        }
        executorPool = Executors.newFixedThreadPool(threads);
        showdownHook();
        try {
            prefixes = listAndGetNextPrefixes(prefixes);
            while (prefixes.size() > 0) {
                prefixes = listAndGetNextPrefixes(prefixes);
            }
            Iterator<Future<List<String>>> iterator;
            Future<List<String>> future;
            List<String> tempPrefixes;
            while (futures.size() > 0) {
                iterator = futures.iterator();
                while (iterator.hasNext()) {
                    future = iterator.next();
                    if (future.isDone()) {
                        tempPrefixes = future.get();
                        if (tempPrefixes != null) prefixes.addAll(tempPrefixes);
                        iterator.remove();
                    }
                }
                while (prefixes.size() > 0) {
                    prefixes = listAndGetNextPrefixes(prefixes);
                }
            }
            executorPool.shutdown();
            while (!executorPool.isTerminated()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignored) {
                    int i = 0;
                    while (i < 1000) i++;
                }
            }
            rootLogger.info("{} finished.", info);
            endAction();
        } catch (Throwable e) {
            executorPool.shutdownNow();
            rootLogger.error(e.toString(), e);
            endAction();
            System.exit(-1);
        }
    }

    private final Object object = new Object();
    private LocalDateTime pauseDateTime = LocalDateTime.MAX;

    public void export(LocalDateTime startTime, long pauseDelay, long duration) throws Exception {
        if (startTime != null) {
            Clock clock = Clock.systemDefaultZone();
            LocalDateTime now = LocalDateTime.now(clock);
            if (startTime.minusWeeks(1).isAfter(now)) {
                throw new Exception("startTime is not allowed to exceed next week");
            }
            while (now.isBefore(startTime)) {
                System.out.printf("\r%s", LocalDateTime.now(clock).toString().substring(0, 19));
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignored) {
                }
                now = LocalDateTime.now(clock);
            }
        }
        if (duration <= 0 || pauseDelay < 0) {
            export();
        } else if (duration > 84600 || duration < 1800) {
            throw new Exception("duration can not be bigger than 23.5 hours or smaller than 0.5 hours.");
        } else {
            pauseDateTime = LocalDateTime.now().plusSeconds(pauseDelay);
            Timer timer = new Timer();
            timer.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    synchronized (object) {
                        object.notifyAll();
                    }
                    pauseDateTime = LocalDateTime.now().plusSeconds(86400 - duration);
//                    pauseDateTime = LocalDateTime.now().plusSeconds(20 - duration);
                }
            }, (pauseDelay + duration) * 1000, 86400000);
//            }, (pauseDelay + duration) * 1000, 20000);
            export();
            timer.cancel();
        }
    }
}

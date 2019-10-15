package com.qiniu;

import com.qiniu.util.ParamsUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ParamsDemo {

    public static void main(String[] args) throws IOException {
        if (args != null && args.length > 0) {
            boolean cmdGoon = false;
            Map<String, String> paramsMap = new HashMap<>();
            String[] strings;
            String key;
            for (String arg : args) {
                // 参数命令格式：-<key>=<value>
                cmdGoon = cmdGoon || arg.matches("^-.+=.+$");
                if (cmdGoon || arg.matches("^-[^=]+$")) {
                    if (!arg.startsWith("-"))
                        throw new IOException("invalid command param: \"" + arg + "\", not start with \"-\".");
                    key = arg.substring(1);
                    strings = ParamsUtils.splitParam(key);
                    paramsMap.put(strings[0], strings[1]);
                }
            }
            System.out.println(paramsMap);
        }
    }
}

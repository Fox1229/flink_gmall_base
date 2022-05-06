package com.atguigu.gmall.realtime.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * 分词器工具类
 */
public class MyKeyWordUtils {


    /**
     * 将字符串切分为多个关键字
     */
    public static List<String> analyze(String text) {

        ArrayList<String> resList = new ArrayList<>();
        Reader reader = new StringReader(text);
        IKSegmenter ikSegmenter = new IKSegmenter(reader, true);
        Lexeme lexeme = null;
        try {
            while ((lexeme = ikSegmenter.next()) != null) {
                resList.add(lexeme.getLexemeText());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return resList;
    }
}

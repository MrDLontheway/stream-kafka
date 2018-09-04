package com.wxstc.dl.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class IKUtils {
    public static String[] ikAny(String text){
        List<String> result = new ArrayList<String>();
        String[] array = new String[]{};
        try {
            StringReader stringReader = new StringReader(text);
            IKSegmenter ik= new IKSegmenter(stringReader, true);
            Lexeme wordLexeme = null;
            while ((wordLexeme=ik.next())!=null) {
                String s = wordLexeme.getLexemeText();
                result.add(s);
            }
            stringReader.close();
            array = new String[result.size()];
            result.toArray(array);
            return array;
        }catch (Exception e){
            e.printStackTrace();
        }
        return array;
    }
}

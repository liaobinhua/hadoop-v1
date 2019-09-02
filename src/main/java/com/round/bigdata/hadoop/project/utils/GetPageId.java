package com.round.bigdata.hadoop.project.utils;

import org.apache.commons.lang.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * author: binhualiao
 * <p>
 * Created Time: 2019-08-30 14:34
 **/
public class GetPageId {

    public static String getPageId(String url) {
        String pageId = "";
        if (StringUtils.isBlank(url)) {
            return pageId;
        }

        Pattern pat = Pattern.compile("topicId=[0-9]+");
        Matcher matcher = pat.matcher(url);
        if (matcher.find()) {
            pageId = matcher.group().split("topicId=")[1];
        }
        return pageId;
    }

    public static void main(String[] args) {
        System.out.println(getPageId("http://www.yihaodian.com/cms/view.do?topicId=14572"));
    }
}

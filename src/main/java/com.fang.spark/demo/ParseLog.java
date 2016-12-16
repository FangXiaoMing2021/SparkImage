package com.fang.spark.demo;

/**
 * Created by fang on 16-12-11.
 */
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.json.JSONObject;
public class ParseLog {

    private static String VIEWSTRING = "正在查看新闻:";
    private static String APPLYSTRING = "写过的新闻";
    private static String SEARCHSTRING = "正在搜索新闻：";
    private static String USERTAG = "用户：";

    /**
     * 用戶名，用戶行為，以key-value方式存储
     * 用户行为 以json方式存储
     */
    public static void readFileByLines(String fileName) {
        File file = new File(fileName);
        BufferedReader reader = null;
        try {
            System.out.println("以行为单位读取文件内容，一次读一整行：");
            InputStreamReader isr = new InputStreamReader(new FileInputStream(file), "UTF-8");
            reader = new BufferedReader(isr);
            String tempString = null;
            Map<String, Object> map = new HashMap<String, Object>();
            // 一次读入一行，直到读入null为文件结束
            while ((tempString = reader.readLine()) != null) {
                if (tempString.contains(VIEWSTRING)) {
                    int userindex = tempString.indexOf(USERTAG);
                    String username = tempString.substring(userindex + 3, tempString.indexOf(",", userindex));
                    int done = tempString.indexOf(VIEWSTRING);
                    String ewId = tempString.substring(done + VIEWSTRING.length());
                    if (username != null && username != "") {
                        try {
                            if (map.containsKey(username)) {
                                JSONObject json = (JSONObject) map.get(username);
                                if (json.has("view")) {
                                    if (!json.get("view").toString().contains(ewId)) {
                                        json.put("view", json.get("view") + "," + ewId);
                                    }
                                } else {
                                    json.put("view", ewId);
                                }
                                map.put(username, json);
                            } else {
                                JSONObject json = new JSONObject();
                                json.put("view", ewId);
                                map.put(username, json);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }

                if (tempString.contains(APPLYSTRING)) {
                    int userindex = tempString.indexOf(USERTAG);
                    String username = tempString.substring(userindex + 3, tempString.indexOf(",", userindex));
                    int done = tempString.indexOf(APPLYSTRING);
                    String ewId = tempString.substring(done + APPLYSTRING.length());

                    if (username != null && username != "") {
                        try {
                            if (map.containsKey(username)) {
                                JSONObject json = (JSONObject) map.get(username);
                                if (json.has("apply")) {
                                    if (!json.get("apply").toString().contains(ewId)) {
                                        json.put("apply", json.get("apply") + "," + ewId);
                                    }
                                } else {
                                    json.put("apply", ewId);
                                }
                                map.put(username, json);
                            } else {
                                JSONObject json = new JSONObject();
                                json.put("apply", ewId);
                                map.put(username, json);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }

                if (tempString.contains(SEARCHSTRING)) {
                    int userindex = tempString.indexOf(USERTAG);
                    String username = tempString.substring(userindex + 3, tempString.indexOf(",", userindex));
                    int done = tempString.indexOf(SEARCHSTRING);
                    String keyword = URLDecoder.decode(tempString.substring(done + SEARCHSTRING.length()), "utf-8");

                    if (username != null && username != "") {
                        try {
                            if (map.containsKey(username)) {
                                JSONObject json = (JSONObject) map.get(username);
                                if (json.has("search")) {
                                    if (!json.get("search").toString().contains(keyword.trim())) {
                                        json.put("search", json.get("search") + "," + keyword.trim());
                                    }
                                } else {
                                    json.put("search", keyword.trim());
                                }
                                map.put(username, json);
                            } else {
                                JSONObject json = new JSONObject();
                                json.put("search", keyword.trim());
                                map.put(username, json);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
            reader.close();
            Iterator<Entry<String, Object>> it = map.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, Object> entry = it.next();
                System.out.println(entry.getKey() + "-----------" + entry.getValue());
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
    }

    public static void main(String[] args) {
        ParseLog.readFileByLines("C:\\Users\\lyh\\Desktop\\log");
    }

}

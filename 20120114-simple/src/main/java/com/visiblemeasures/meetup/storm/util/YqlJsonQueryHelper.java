package com.visiblemeasures.meetup.storm.util;

import org.apache.commons.io.IOUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public class YqlJsonQueryHelper {
  public static final String YQL_URL = "http://query.yahooapis.com/v1/public/yql?q=select%20*%20from%20csv%20where%20url%3D%27http%3A%2F%2Fdownload.finance.yahoo.com%2Fd%2Fquotes.csv%3Fs%3DYHOO%2CORCL%2CAAPL%26f%3Dsl1d1t1c1ohgv%26e%3D.csv%27%20and%20columns%3D%27symbol%2Cprice%2Cdate%2Ctime%2Cchange%2Ccol1%2Chigh%2Clow%2Ccol2%27&format=json&callback=";
  public static final String YQL_TICKER_NAME = "http://query.yahooapis.com/v1/public/yql?q=select%20*%20from%20google.igoogle.stock%20where%20stock%20IN%20('YHOO'%2C'ORCL'%2C'AAPL')%3B&format=json&env=store%3A%2F%2Fdatatables.org%2Falltableswithkeys&callback=";


  public static Map<String, Double> queryData() {
    HashMap<String, Double> stockData = new HashMap<String, Double>();
    Reader in = null;
    try {
      if (1 == 0) { //in case Yahoo doesn't respond
        in = new StringReader(YqlTestDataRecord.REC);
      } else {
        in = new InputStreamReader(new URL(YQL_URL).openStream());
      }
      Object obj = new JSONParser().parse(in);
      if (obj != null) {
        JSONObject jsonObject = (JSONObject) obj;
        JSONArray resultRows = (JSONArray) ((JSONObject) (((JSONObject) jsonObject.get("query")).get("results"))).get("row");
        for (Object resultRow : resultRows) {
          JSONObject resultRowObj = (JSONObject) resultRow;
          stockData.put((String) resultRowObj.get("symbol"), Double.parseDouble((String) resultRowObj.get("price")));
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    } catch (ParseException e) {
      e.printStackTrace();
    } finally {
      IOUtils.closeQuietly(in);
    }
    return stockData;
  }

  public static Map<String, String> queryNames() {
    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    HashMap<String, String> stockData = new HashMap<String, String>();
    Reader in = null;
    try {
      if (1 == 0) { //in case Yahoo doesn't respond
        in = new StringReader(YqlTestDataRecord.REC);
      } else {
        in = new InputStreamReader(new URL(YQL_TICKER_NAME).openStream());
      }
      Object obj = new JSONParser().parse(in);
      if (obj != null) {
        JSONObject jsonObject = (JSONObject) obj;
        JSONArray resultRows = (JSONArray) ((JSONObject) (((JSONObject) jsonObject.get("query")).get("results"))).get("xml_api_reply");
        for (Object resultRow : resultRows) {
          JSONObject resultRowObj = (JSONObject) resultRow;
          JSONObject ps = (JSONObject)((JSONObject) resultRowObj.get("finance")).get("symbol");
          JSONObject cn = (JSONObject)((JSONObject) resultRowObj.get("finance")).get("company");
          stockData.put((String) ps.get("data"), (String) cn.get("data"));
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    } catch (ParseException e) {
      e.printStackTrace();
    } finally {
      IOUtils.closeQuietly(in);
    }
    return stockData;
  }

}

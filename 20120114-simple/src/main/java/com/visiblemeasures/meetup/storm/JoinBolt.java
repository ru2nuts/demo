package com.visiblemeasures.meetup.storm;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class JoinBolt extends BaseBasicBolt {

  private int ti;
  Map<String, String> tickerNames = new HashMap<String, String>();



  @Override
  public void prepare(Map stormConf, TopologyContext context) {
    super.prepare(stormConf, context);
    ti = context.getThisTaskIndex();
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    String ticker = tuple.getString(0);
    if (tuple.contains("name")) {
      tickerNames.put(tuple.getString(0), tuple.getString(1));
    }
    if (tuple.contains("price")) {
      if (tickerNames.containsKey(ticker)) {
        ticker = tickerNames.get(ticker);
      }
      Double price = tuple.getDouble(1);
      System.out.printf("\t*************************************************** Join Bolt: %s; Ticker: %s - %s\n", ti, ticker, price);
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
  }
}

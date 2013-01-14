package com.visiblemeasures.meetup.storm;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import java.util.Map;

public class JoinBolt extends BaseBasicBolt {

  private int ti;

  @Override
  public void prepare(Map stormConf, TopologyContext context) {
    super.prepare(stormConf, context);
    ti = context.getThisTaskIndex();
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    String ticker = tuple.getString(0);
    Double price = tuple.getDouble(1);

    System.out.printf("\t*************************************************** Join Bolt: %s; Ticker: %s - %s\n", ti, ticker, price);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
  }
}

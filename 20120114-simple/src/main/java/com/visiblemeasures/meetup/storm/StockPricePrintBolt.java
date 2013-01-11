package com.visiblemeasures.meetup.storm;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class StockPricePrintBolt extends BaseBasicBolt {

  @Override
  public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
    String ticker = tuple.getString(0);
    Double price = tuple.getDouble(1);
    System.out.printf("\t------- Ticker: %s - %s -------", ticker, price);
    System.out.println();
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
  }
}

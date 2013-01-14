package com.visiblemeasures.meetup.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.util.Arrays;
import java.util.Map;

public class StockPricePrintBolt extends BaseRichBolt {

  private int ti;
  private OutputCollector collector;

  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
    ti = context.getThisTaskIndex();
    System.out.printf("Preparing bolt %s of topology %s", ti, context.getStormId());
    System.out.println();
  }

  @Override
  public void execute(Tuple tuple) {
    String ticker = tuple.getString(0);
    Double price = tuple.getDouble(1);
    System.out.printf("\t------- Simple Bolt: %s; Ticker: %s - %s\n", ti, ticker, price);
    collector.emit("stock_prices", Arrays.asList((Object) ticker, price));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declareStream("stock_prices", new Fields("ticker", "price"));
  }
}

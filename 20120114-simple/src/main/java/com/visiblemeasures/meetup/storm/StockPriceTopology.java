package com.visiblemeasures.meetup.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class StockPriceTopology {

  public static void main(String[] args) {
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("stocks_in", new YqlJsonSpout(), 1);
    builder.setSpout("stocks_names_in", new YqlTickerNameSpout(), 1);
    builder.setBolt("stocks_print", new StockPricePrintBolt(), 3).shuffleGrouping("stocks_in", "stock_prices");

    builder.setBolt("join_bolt", new JoinBolt(), 2)
        .fieldsGrouping("stocks_names_in", "stock_names", new Fields("ticker"))
        .fieldsGrouping("stocks_in", "stock_prices", new Fields("ticker"));

    Config conf = new Config();

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("TickerTopology", conf, builder.createTopology());
    try {
      Thread.sleep(30 * 60 * 1000); //30 min
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    cluster.killTopology("TickerTopology");
    cluster.shutdown();
  }
}

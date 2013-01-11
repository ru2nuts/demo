package com.visiblemeasures.meetup.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class StockPriceTopology {

  public static void main(String[] args) {
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("stocks_in", new YqlJsonSpout(), 1);
    builder.setBolt("stocks_print", new StockPricePrintBolt(), 1).shuffleGrouping("stocks_in");

    Config conf = new Config();
    conf.setDebug(false);
    conf.setNumAckers(2);
    conf.setNumWorkers(2);
    conf.setMaxTaskParallelism(4);
    conf.setMaxSpoutPending(100);
    conf.setMessageTimeoutSecs(30);

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

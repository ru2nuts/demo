package com.visiblemeasures.meetup.storm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.visiblemeasures.meetup.storm.util.YqlJsonQueryHelper;

import java.util.Map;

public class YqlJsonSpout extends BaseRichSpout {

  private static final int QUERY_INTERVAL = 3 * 1000;

  private transient SpoutOutputCollector spoutOutputCollector;
  private transient long minute = System.currentTimeMillis() / QUERY_INTERVAL;

  public YqlJsonSpout() {
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("ticker", "price"));
  }

  @Override
  public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
    this.spoutOutputCollector = spoutOutputCollector;
  }

  @Override
  public void nextTuple() {
    long currentMinute = System.currentTimeMillis() / QUERY_INTERVAL;
    if (minute != currentMinute) {
      minute = currentMinute;
      for (Map.Entry<String, Double> dataItem : YqlJsonQueryHelper.queryData().entrySet()) {
        spoutOutputCollector.emit(new Values(dataItem.getKey(), dataItem.getValue()));
      }
    } else {
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}




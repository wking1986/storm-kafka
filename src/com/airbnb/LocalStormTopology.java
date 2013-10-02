package com.airbnb;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.RawScheme;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.airbnb.storm.spout.KafkaSpout;

import java.io.UnsupportedEncodingException;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: wmoss
 * Date: 10/2/13
 * Time: 12:12 PM
 * To change this template use File | Settings | File Templates.
 */
public class LocalStormTopology {

  public static class PrinterBolt extends BaseRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
      try {
        System.out.println("Got Tuple: " + new String(tuple.getBinary(0), "utf-8"));
      } catch (UnsupportedEncodingException e) {
      } finally {
        collector.ack(tuple);
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) { }
  }

  public static void main(String [] args) {

    KafkaSpout spout = new KafkaSpout("localhost:2181", "localGroupId", "flog", new RawScheme());

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("kafka", spout, 10);
    builder.setBolt("printer", new PrinterBolt(), 5)
        .shuffleGrouping("kafka");

    Config conf = new Config();
    //conf.setDebug(true);
    conf.setNumWorkers(2);

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("log-processing", conf, builder.createTopology());


  }

}

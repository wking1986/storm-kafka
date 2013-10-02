package com.airbnb.storm.spout;


import backtype.storm.spout.Scheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaSpout implements IRichSpout {
  private final Properties props = new Properties();
  private final Scheme scheme;
  private final String topic;

  private SpoutOutputCollector collector;
  private ConsumerConnector consumer;
  private ConsumerIterator<byte[], byte[]> stream;

  public KafkaSpout(String zookeeper, String groupId, String topic, Scheme scheme) {
    props.put("zookeeper.connect", zookeeper);
    props.put("group.id", groupId);
    props.put("zookeeper.session.timeout.ms", "400");
    props.put("zookeeper.sync.time.ms", "200");
    props.put("auto.commit.interval.ms", "1000");
    props.put("consumer.timeout.ms", "0");

    this.topic = topic;
    this.scheme = scheme;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(scheme.getOutputFields());
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }

  @Override
  public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {
    this.collector = collector;

    ConsumerConfig config = new ConsumerConfig(props);
    consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);

    //TODO (wmoss): Consider using consumer.createMessageStreamsByFilter to support multiple topics
    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(topic, 1);
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
    this.stream = consumerMap.get(topic).get(0).iterator();
  }

  @Override
  public void close() {
    consumer.commitOffsets();
  }

  @Override
  public void activate() { }

  @Override
  public void deactivate() { }

  @Override
  public void nextTuple() {
    try {
      byte[] msg = stream.next().message();

      List<Object> tuple = scheme.deserialize(msg);
      collector.emit(tuple);
    } catch (ConsumerTimeoutException e) { }
  }

  @Override
  public void ack(Object o) {
    //We don't need to do anything here, Kafka doesn't require acks
  }

  @Override
  public void fail(Object o) {
    //TODO (wmoss): Deal with failure
  }
}
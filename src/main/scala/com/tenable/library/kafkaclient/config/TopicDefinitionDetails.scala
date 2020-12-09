package com.tenable.library.kafkaclient.config

case class TopicDefinitionDetails(
  name: String,
  partitions: Int,
  replicationFactor: Short,
  // Additional, optional config, for reference see: https://kafka.apache.org/documentation/#topicconfigs
  properties: Map[String, String] = Map.empty
)

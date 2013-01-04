package com.linkedin.batch.etl.kafka.common;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import kafka.api.PartitionFetchInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchRequest;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;

import com.linkedin.batch.etl.kafka.EtlJob;

/**
 * 
 * One stop client to access all the API's related to Kafka
 * 
 * 
 */

public class KafkaClient
{
  public static HashMap<String, List<EtlRequest>> partitionInfo;

  public KafkaClient()
  {
  }

  public static HashMap<String, List<EtlRequest>> loadKafkaMetadata(List<String> topics) throws IOException

  {
      //A different kafka client can be specified
    SimpleConsumer consumer =
        new SimpleConsumer("172.20.72.39", 10251, 30000, 1024 * 1024, null);

    // TODO : Hard coded values... modify to read from the configuration
    // Part of the retry logic

    int retries = 3;
    int retryInterval = 1000; // this is in milliseconds
    boolean success = false;
    TopicMetadataResponse response = null;
    while (retries != 0 && success == false)
    {
      try
      {
        response = consumer.send(new TopicMetadataRequest(topics));
        success = true;
      }
      catch (Exception E)
      {
        System.err.println("Error encountered while fetching metadata from the kafka broker. Retrying after "
            + retryInterval + " msec.");
        try
        {
          Thread.sleep(1000);
        }
        catch (InterruptedException e)
        {
          System.err.println("Not able to pause the program betweeen two calls for metadata.");
        }
        retries--;
      }
    }
    if (retries == 0 && success == false)
    {
      System.err.println("Fetching metadata from the Kafka broker failed after 3 attempts. Exiting the program.");
      // Is there a better way of doing this?
      System.exit(-1);
    }

    List<TopicMetadata> topicsMetadata = response.topicsMetadata();
    for (TopicMetadata topicMetadata : topicsMetadata)
    {
      List<PartitionMetadata> partitionsMetadata = topicMetadata.partitionsMetadata();
      List<EtlRequest> tempEtlRequests = new ArrayList<EtlRequest>();
      for (PartitionMetadata partitionMetadata : partitionsMetadata)
      {
        if (!(partitionMetadata.errorCode() == ErrorMapping.NoError()))
        {
          // Error encountered for this topic and partition
          // Bypass. To be taken care in the next run.
          continue;
        }
        else
        {
          int index = partitionMetadata.leader().getZkString().indexOf(":");
          // The replicas are not needed to be added as extra information
          EtlRequest etlRequest;
          try
          {
            etlRequest =
                new EtlRequest(topicMetadata.topic(),
                               Integer.toString(partitionMetadata.leader().id()),
                               partitionMetadata.partitionId(),
                               new URI(partitionMetadata.leader()
                                                        .getZkString()
                                                        .substring(index + 1)));
            tempEtlRequests.add(etlRequest);
          }
          catch (URISyntaxException e)
          {
            System.out.println("Error in generating the broker URI after loading the Kafka Metadata.");
            e.printStackTrace();
          }

        }
      }
      if (tempEtlRequests.size() != 0 || tempEtlRequests != null)
      {
        partitionInfo.put(topicMetadata.topic(), tempEtlRequests);
      }
    }
    return partitionInfo;
  }

  public FetchResponse getFetchRequests(SimpleConsumer simpleConsumer,
                                        String topicName,
                                        int partitionId,
                                        int nodeId,
                                        long offset,
                                        int fetchBufferSize)
  {
    PartitionFetchInfo partitionFetchInfo =
        new PartitionFetchInfo(offset, fetchBufferSize);
    HashMap<TopicAndPartition, PartitionFetchInfo> fetchInfo =
        new HashMap<TopicAndPartition, PartitionFetchInfo>();

    // Till the VIP is available, we instanitate the brokers with one of the brokers
    // information obtained from the zookeeper
    // Hardcoding
    TopicAndPartition topicAndPartition = new TopicAndPartition(topicName, partitionId);
    fetchInfo.put(topicAndPartition, partitionFetchInfo);
    // min Bytes/ Max wait ... These parameters need to be set
    // corrlelation, client, broker (node Id), maxwait, minbytes,fetchInfo

    // This needs confirmation from Joel as to if "0" is a perfectly valid option or not
    FetchRequest fetchRequest =
        new FetchRequest(-1, "hadoop-etl", nodeId, 0, 0, fetchInfo);
    FetchResponse fetchResponse = simpleConsumer.fetch(fetchRequest);
    return fetchResponse;
  }

  // public HashMap<String, List<EtlRequest>> generateETLRequests(List<String> topics)
  // {
  // HashMap<String, List<EtlRequest>> partitionInfo = null;
  // try
  // {
  // partitionInfo = loadKafkaMetadata(topics);
  // }
  // catch (IOException e)
  // {
  // // TODO Auto-generated catch block
  // e.printStackTrace();
  // }
  // catch (URISyntaxException e)
  // {
  // // TODO Auto-generated catch block
  // e.printStackTrace();
  // }
  // return partitionInfo;
  // }

//  public static String getKafkaHostURI()
//  {
//    return EtlJob.KAFKA_HOSTS;
//  }

}

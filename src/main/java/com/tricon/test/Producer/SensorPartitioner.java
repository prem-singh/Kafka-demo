/**
 * 
 */
package com.tricon.test.Producer;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.utils.Utils;

/**
 * @author premsingh
 * Custom partition sample. This will send all the specific sensor data to fixed 30% partition and rest sensor data will go to remaining 70%.
 */
public class SensorPartitioner implements Partitioner{
	
	private String speedSensorName;

	@Override
	public void configure(Map<String, ?> configs) {
	          this.speedSensorName = configs.get("speed.sensor.name").toString();
	}

	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
		List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
		int numPartitions = partitions.size();
		System.out.println("Partition size:::" +numPartitions);
		int sp = (int)Math.abs(numPartitions*0.3);
		int p=0;
		
		if ( (keyBytes == null) || (!(key instanceof String)) )
            throw new InvalidRecordException("All messages must have sensor name as key");
		
		if ( ((String)key).equals(speedSensorName) )
            p = Utils.toPositive(Utils.murmur2(valueBytes)) % sp;
       else
            p = Utils.toPositive(Utils.murmur2(keyBytes)) % (numPartitions-sp) + sp ;
		
		System.out.println("Key = " + (String)key + " Partition = " + p );
		
		
		return p;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

}

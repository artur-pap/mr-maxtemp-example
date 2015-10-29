package ru.apapikyan.learn.bigdata.mapreds;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	static Logger LOG = Logger.getLogger(MaxTemperatureMapper.class);
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
	        throws IOException, InterruptedException {
	    // TODO Auto-generated method stub
	    super.map(key, value, context);
	}
}
package ru.apapikyan.learn.bigdata;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import ru.apapikyan.learn.bigdata.mapreds.MaxTemperatureMapper;
import ru.apapikyan.learn.bigdata.mapreds.MaxTemperatureReducer;

public class MaxTemperatureMapperTest {
	@Test
	public void processesValidRecord() throws IOException, InterruptedException {
		Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
		// Year ^^^^
		        "99999V0203201N00261220001CN9999999N9-00111+99999999999");
		// Temperature ^^^^^
		new MapDriver<LongWritable, Text, Text, IntWritable>().withMapper(new MaxTemperatureMapper())
		        .withInput(new LongWritable(0), value).withOutput(new Text("1950"), new IntWritable(-11)).runTest();
	}

	public class MaxTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
		        InterruptedException {
			int maxValue = Integer.MIN_VALUE;
			for (IntWritable value : values) {
				maxValue = Math.max(maxValue, value.get());
			}
			context.write(key, new IntWritable(maxValue));
		}
	}
}
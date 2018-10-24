package p3;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class WordCountStreaming {
	public static void main(String[]args) 
	{
		//set up streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		//checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);
		
		//make parameter available in the web interface
		env.getConfig().setGlobalJobParameters(params);
		
		//read stream data
		DataStream<String> text = env.socketTextStream("localhost", 9999);
		
		DataStream<Tuple2<String,Integer>> counts = text.filter(new FilterFunction<String>()
				{
			public boolean filter(String value) {
				return value.startsWith("N");
			}
				}
				)
				.map(new Tokenizer()) //split up the lines in pairs(2-tuples) containing: tuple2{(name,1)}
				.keyBy(0).sum(1);
		
		counts.print();
		//exxecute program
		try {
			env.execute("Streaming WordCount");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	public static final class Tokenizer implements MapFunction<String, Tuple2<String,Integer>>
	{
		public Tuple2<String,Integer> map(String value)
		{
			return new Tuple2(value,Integer.valueOf(1));
		}
	}

}

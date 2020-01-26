package storm.cookbook;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class HelloWorldSpout extends BaseRichSpout {
	//Create the following member variables and construct the object:

	private SpoutOutputCollector collector;
	private int referenceRandom;
	private static final int MAX_RANDOM = 10;
	public HelloWorldSpout(){
		final Random rand = new Random();
	    referenceRandom = rand.nextInt(MAX_RANDOM);
	 }
	  
	//After construction, the Storm cluster will open the spout; provide the following implementation for the open method:
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}
	//The Storm cluster will repeatedly call the nextTuple method, which will do all the work of the spout. Provide the following implementation for the method:
	@Override
	public void nextTuple() {
        final Random rand = new Random();
        if (rand.nextInt(MAX_RANDOM) == referenceRandom) {
            collector.emit(new Values("Hello World!"));
        } else {
            collector.emit(new Values("Go Away!"));
        }
    }

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}

}

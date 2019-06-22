import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class SinleInput {

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        long startTime = System.currentTimeMillis();
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // get input data
        SingleOutputStreamOperator<Tuple4<Long, Integer, Double, Double>> reducedStream = env.readTextFile("G:\\stream1.txt")
                .assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor<String>(Time.milliseconds(10)) {
                            @Override
                            public long extractTimestamp(String s) {
                                String[] tokens = s.split(" ");
                                long time = Long.parseLong(tokens[0]) * 1000;
                                return time;
                            }
                        })
                .flatMap(new Tokenizer())
                .keyBy(1)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<Tuple4<Long, Integer, Double, Double>>() {
                    @Override
                    public Tuple4<Long, Integer, Double, Double> reduce(Tuple4<Long, Integer, Double, Double> t1, Tuple4<Long, Integer, Double, Double> t2) throws Exception {
                        if (t1.f0 > t2.f0) {
                            System.out.println(t1);
                            return t1;
                        } else {
                            return t2;
                        }
                    }
                });

//        reducedStream.print();
        // emit result
//        System.out.println("开始sink至文件");
//        counts.writeAsText("E:\\Desktop\\result1", FileSystem.WriteMode.OVERWRITE);
        reducedStream.writeAsText("E:\\Desktop\\result1", FileSystem.WriteMode.OVERWRITE);
//        counts.print();
        // execute program
        env.execute("Streaming WordCount");

        long endTime = System.currentTimeMillis();
        System.out.println("单文件" + env.getParallelism() + "线程无sink总耗时" + (endTime - startTime));
    }

    public static final class Tokenizer implements FlatMapFunction<String, Tuple4<Long, Integer, Double, Double>> {

        @Override
        public void flatMap(String value, Collector<Tuple4<Long, Integer, Double, Double>> out) {
            // normalize and split the line
//            System.out.println(value);
            String[] line = value.toLowerCase().split(" ");
            if (line.length > 3) {
                Long time = Long.parseLong(line[0].trim()) * 1000;
                Integer id = Integer.parseInt(line[1].trim());
                Double lon;
                try {
                    lon = Double.parseDouble(line[2].trim());
                } catch (NumberFormatException e) {
                    //错误处理
                    return;
                }
                Double lat = Double.parseDouble(line[3].trim());
                out.collect(new Tuple4<>(time, id, lon, lat));
            }
        }
    }

}
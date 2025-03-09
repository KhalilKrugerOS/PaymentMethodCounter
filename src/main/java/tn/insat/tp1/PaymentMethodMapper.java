package tn.insat.tp1;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PaymentMethodMapper
        extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text paymentMethod = new Text();
    public void map(Object key, Text value, Mapper.Context context
    ) throws IOException, InterruptedException {
        String[] tokens = value.toString().trim().split("\\s+");
        if (tokens.length > 0) {
            String method = tokens[tokens.length - 1 ];
            paymentMethod.set(method);
            context.write(paymentMethod, one);
        }
    }
}
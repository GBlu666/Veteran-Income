import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class UniqueRecsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final IntWritable one = new IntWritable(1);

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        // Indices of the columns to profile
        int[] columnsToProfile = {1, 2, 7};
        String[] columnLabels = {"Label (Grouping)", "Label", "United States!!Nonveterans!!Estimate"};

        for (int i = 0; i < columnsToProfile.length; i++) {
            int columnIndex = columnsToProfile[i];
            if (columnIndex < fields.length) {
                String columnValue = fields[columnIndex].trim();
                String outputKey = columnLabels[i] + ": " + columnValue;
                context.write(new Text(outputKey), one);
            }
        }
    }
}
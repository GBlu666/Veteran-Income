public class CleanMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(",");
        
        if (fields.length >= 2) {
            String industryName = fields[1].trim();
            context.write(NullWritable.get(), new Text(industryName + "\t"));
        }
    }
}
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class CleanMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private Pattern csvPattern = Pattern.compile("\"([^\"]+?)\",?|([^,]+),?|,");
    private boolean isFirstLine = true; // Flag to check if the current line is the first line

    private String[] splitCsv(String input) {
        Matcher matcher = csvPattern.matcher(input);
        String[] fields = new String[10]; // Adjust the size according to your dataset
        int index = 0;

        while (matcher.find()) {
            String match = matcher.group(1);
            if (match != null) {
                // Remove commas from numbers and unify missing data representations
                match = match.replaceAll(",", "").replaceAll("\\(X\\)", "NULL");
                fields[index++] = match.isEmpty() ? "NULL" : match;
            } else {
                String group2 = matcher.group(2);
                if (group2 == null) {
                    // Log the problematic input line and skip processing
                    System.err.println("Null group in input line: " + input);
                    return null;
                }
                fields[index++] = group2.isEmpty() ? "NULL" : group2;
            }
        }

        return fields;
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (isFirstLine) {
            context.write(value, NullWritable.get()); // Write the header as is
            isFirstLine = false;
            return;
        }

        String[] fields = splitCsv(value.toString());
        if (fields == null) {
            // Skip processing this line
            return;
        }

        StringBuilder cleanedRecord = new StringBuilder();
        for (String field : fields) {
            cleanedRecord.append(field);
            cleanedRecord.append(",");
        }

        // Remove the trailing comma
        if (cleanedRecord.length() > 0) {
            cleanedRecord.setLength(cleanedRecord.length() - 1);
        }

        context.write(new Text(cleanedRecord.toString()), NullWritable.get());
    }
}

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ProductAnalytics {

    // Regex to correctly split CSV lines containing quoted fields with commas
    private static final String CSV_REGEX = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";
    private static final Pattern RATING_PATTERN = Pattern.compile("Rated (\\d) out of 5 stars");

    // =========================================================================
    // JOB 1: AVERAGE RATING & TOTAL REVIEWS PER COUNTRY
    // =========================================================================
    public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            // Skip header row or empty lines
            if (line.isEmpty() || line.startsWith("Reviewer Name")) return;

            String[] parts = line.split(CSV_REGEX, -1);
            if (parts.length >= 6) {
                String country = parts[2].replace("\"", "").trim();
                String rawRating = parts[5].replace("\"", "").trim();

                Matcher matcher = RATING_PATTERN.matcher(rawRating);
                if (!country.isEmpty() && matcher.find()) {
                    String numericRating = matcher.group(1);
                    outputKey.set(country);
                    outputValue.set(numericRating);
                    context.write(outputKey, outputValue);
                }
            }
        }
    }

    public static class RatingReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double totalRating = 0;
            int count = 0;

            for (Text val : values) {
                try {
                    totalRating += Double.parseDouble(val.toString().trim());
                    count++;
                } catch (NumberFormatException e) {
                    // Skip unparseable rows
                }
            }

            if (count > 0) {
                double avgRating = totalRating / count;
                result.set(String.format("AvgRating: %.2f\tTotalReviews: %d", avgRating, count));
                context.write(key, result);
            }
        }
    }

    // =========================================================================
    // JOB 2: RATING STAR DISTRIBUTION PER COUNTRY
    // =========================================================================
    public static class DistributionMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text countryRatingPair = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty() || line.startsWith("Reviewer Name")) return;

            String[] parts = line.split(CSV_REGEX, -1);
            if (parts.length >= 6) {
                String country = parts[2].replace("\"", "").trim();
                String rawRating = parts[5].replace("\"", "").trim();

                Matcher matcher = RATING_PATTERN.matcher(rawRating);
                if (!country.isEmpty() && matcher.find()) {
                    String numericRating = matcher.group(1);
                    countryRatingPair.set("Country:" + country + "\tRating_" + numericRating + "Star");
                    context.write(countryRatingPair, one);
                }
            }
        }
    }

    public static class DistributionReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    // Driver Code
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: ProductAnalytics <rating|distribution> <inputPath> <outputPath>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        String jobType = args[0];
        Path inputPath = new Path(args[1]);
        Path outputPath = new Path(args[2]);

        Job job = Job.getInstance(conf, "Amazon Customer Review Analytics - " + jobType);
        job.setJarByClass(ProductAnalytics.class);

        if (jobType.equalsIgnoreCase("rating")) {
            job.setMapperClass(RatingMapper.class);
            job.setReducerClass(RatingReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
        } else if (jobType.equalsIgnoreCase("distribution")) {
            job.setMapperClass(DistributionMapper.class);
            job.setReducerClass(DistributionReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
        } else {
            System.err.println("Invalid job type. Use 'rating' or 'distribution'.");
            System.exit(-1);
        }

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

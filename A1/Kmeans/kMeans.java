import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class KMeans {
    public static final double CONVERGENCE_THRESHOLD = 0.1;

	//define the point class
	public static class Point {
        double x;
		double y;

		//constructor
		public Point(double x, double y) {
		    this.x = x;
		    this.y = y;
		}

		public Point(String str) {
			String[] point_str = str.split(",");
            this.x = Double.parseDouble(point_str[0]);
            this.y = Double.parseDouble(point_str[1]);
        }

        @Override
        public String toString() {
            return x + "," + y;
        }
    }

	public static double pointDistance(Point p1, Point p2) {

        return Math.sqrt((p1.x - p2.x) * (p1.x - p2.x) + (p1.y - p2.y) * (p1.y - p2.y));
    }

    public static Point findClosestCentroid(Point point, List<Point> centroidsList) {
		
        Point closestCentroid = null;
        double minDistance = Double.MAX_VALUE;

        for (int i = 0; i < centroidsList.size(); i++) {
            Point centroid = centroidsList.get(i);

			//calculate distance
            double distance = pointDistance(point, centroid);

            if (distance < minDistance) {
                minDistance = distance;
                closestCentroid = centroid;
            }
        }

        return closestCentroid;
    }
    
	public static Point findNewCentroid(List<Point> pointsList) {
	    if (pointsList.size() == 0) return new Point(0.0, 0.0); // avoid divide by zero
	
	    double sumX = 0.0, sumY = 0.0;
	    for (int i = 0; i < pointsList.size(); i++) {
	        Point p = pointsList.get(i);
	        sumX += p.x;
	        sumY += p.y;
	    }
	    return new Point(sumX / pointsList.size(), sumY / pointsList.size());
	}

	public static class PointsMapper extends Mapper<LongWritable, Text, Text, Text> {

		// centroids : Linked-list/arraylike
		public List<Point> centers = new ArrayList<Point>();

		@Override
		public void setup(Context context) throws IOException, InterruptedException {

			super.setup(context);
			Configuration conf = context.getConfiguration();

			// retrive file path
			Path centroids = new Path(conf.get("centroid.path"));

			// create a filesystem object
			FileSystem fs = FileSystem.get(conf);

			// create a file reader
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, centroids, conf);

			// read centroids from the file and store them in a centroids variable
			Text key = new Text();
			IntWritable value = new IntWritable();
			while (reader.next(key, value)) {
				centers.add(new Point(key.toString()));
			}
			reader.close();
		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// input -> key: charater offset, value -> a point (in Text)
			// write logic to assign a point to a centroid
			// emit key (centroid id/centroid) and value (point)
            Point parsedPoint = new Point(value.toString());

            Point closestCentroid = findClosestCentroid(parsedPoint, centers);
            
			context.write(new Text(closestCentroid.toString()), new Text(parsedPoint.toString()));			
		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
		}
	}

	public static class PointsReducer extends Reducer<Text, Text, Text, Text> {

		public static enum Counter {
			CONVERGED
		}
		// new_centroids (variable to store the new centroids
		public List<Point> new_centroids = new ArrayList<Point>();

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// Input: key -> centroid id/centroid , value -> list of points
			// calculate the new centroid
			// new_centroids.add() (store updated cetroid in a variable)

			List<Point> sameCentroidPointList = new ArrayList<Point>();

            for (Text val : values) {
				Point parsedPoint = new Point(val.toString());
				sameCentroidPointList.add(parsedPoint);
            }
			//get old centroid
			Point oldCentroid = new Point(key.toString());
			
			//compute new centroid
			Point newCentroid = findNewCentroid(sameCentroidPointList);
			new_centroids.add(newCentroid);
			
			// Use pointDistance to check if it is converging by comparing the previous centroid
			double centroidDistance = pointDistance(oldCentroid, newCentroid);
			if (centroidDistance < CONVERGENCE_THRESHOLD) {
				context.getCounter(Counter.CONVERGED).increment(1);
			}

			context.write(new Text(newCentroid.toString()), new Text(""));

		}

		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			// BufferedWriter
			// delete the old centroids
			// write the new centroids
			Configuration conf = new Configuration();

			Path center_path = new Path(context.getConfiguration().get("centroid.path"));
            FileSystem fs = FileSystem.get(conf);

            if (fs.exists(center_path)) {
                fs.delete(center_path, true);
            }

            SequenceFile.Writer writer = SequenceFile.createWriter(conf,
                    SequenceFile.Writer.file(center_path),
                    SequenceFile.Writer.keyClass(Text.class),
                    SequenceFile.Writer.valueClass(IntWritable.class));

            for (int i = 0; i < new_centroids.size(); i++) {
                writer.append(new Text(new_centroids.get(i).toString()), new IntWritable(0));
            }
            writer.close();
		}
	}
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Path center_path = new Path("centroid/cen.seq");
		conf.set("centroid.path", center_path.toString());

		FileSystem fs = FileSystem.get(conf);
	
		if (fs.exists(center_path)) {
			fs.delete(center_path, true);
		}

		final SequenceFile.Writer centerWriter = SequenceFile.createWriter(fs, conf, center_path, Text.class,
				IntWritable.class);
		final IntWritable value = new IntWritable(0);
		//modify based on k
		centerWriter.append(new Text("50.197031637442876,32.94048164287042"), value);
		centerWriter.append(new Text("43.407412339767056,6.541037020010927"), value);
		centerWriter.append(new Text("1.7885358732482017,19.666057053079573"), value);
		centerWriter.append(new Text("32.6358540480337,4.03843047564191"), value);
		centerWriter.append(new Text("48.41919054694239,31.23767287880673"), value);
		centerWriter.append(new Text("35.16296605521855,2.260927131938996"), value);
		centerWriter.append(new Text("49.04816562011978,34.599014498332885"), value);
		centerWriter.append(new Text("53.12345979800841,27.301827115259748"), value);
		centerWriter.append(new Text("2.6203753823708777,11.657150534894178"), value);
		centerWriter.close();

		//timer
		long startTime = System.currentTimeMillis();

        // store centroids after each iteration
        List<List<String>> centroidsHistory = new ArrayList<List<String>>();

		int itr = 0;
		while (itr < 20) {
			// config
			// job
			// set the job parameters
			// itr ++
            Job job = Job.getInstance(conf, "KMeans Iteration " + itr);
            job.setJarByClass(KMeans.class);

            job.setMapperClass(PointsMapper.class);
            job.setReducerClass(PointsReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path("input/data_points.txt"));
            FileOutputFormat.setOutputPath(job, new Path("output/iter" + itr));

            job.waitForCompletion(true);

            // Read current centroids - from PointsMapper setup()
            List<String> currentCentroids = new ArrayList<String>();
            SequenceFile.Reader currReader = new SequenceFile.Reader(fs, center_path, conf);
            Text key = new Text();
            IntWritable val = new IntWritable();
            while (currReader.next(key, val)) {
                currentCentroids.add(key.toString());
            }
            currReader.close();
            centroidsHistory.add(currentCentroids);
			
			long converged = job.getCounters().findCounter(PointsReducer.Counter.CONVERGED).getValue();

			//modify based on k
			if (converged == 9) {
				System.out.println("\nConverged after " + (itr + 1) + " iterations.");
				break;
			}

            itr++;
		}

		//end timer
		long duration = System.currentTimeMillis() - startTime;

        System.out.println("\n===== KMeans Clustering Summary =====");
        System.out.println("Iterations: " + itr);
		System.out.println("Computation time: " + duration + " ms");
		
		//print all centroids in the history 
        for (int i = 0; i < centroidsHistory.size(); i++) {

            List<String> centroids = centroidsHistory.get(i);
			System.out.println("\nCentroids after iteration " + i + ":");
            for (int j = 0; j < centroids.size(); j++) {
                System.out.println("Cluster " + (j + 1) + ": " + centroids.get(j));
            }
        }

		// read the centroid file from hdfs and print the centroids (final result)
    	SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(center_path));
    	Text key = new Text();
    	IntWritable val = new IntWritable();
		int k = 0;
    	while (reader.next(key, val)) {
    	    System.out.println("\nFinal centroid: "+ (k + 1) + ": " + key.toString());
			k++;
    	}
    	reader.close();
	}
}
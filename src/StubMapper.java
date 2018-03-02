import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StubMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    @Override
    public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
        String[] inpLine = value.toString().split("\t");
        Long inpUser = Long.parseLong(inpLine[0]);
        ArrayList<Long> outUsersList = new ArrayList<Long>();

        if(inpLine.length == 2){
	        	String[] subparts = inpLine[1].split(",");
	        	//adding delimiter between User and his direct friends
	        for(int i = 0; i < subparts.length; i++){
	        		Long myConsumer = Long.parseLong(subparts[i]);
	            outUsersList.add(myConsumer);
	            Text temp = new Text(myConsumer.toString() + "," + -1);
	            ctx.write(new LongWritable(inpUser), temp);
	        }
	        
	        //looping through direct friends of User and setting connection between them
	        for (int i = 0; i < outUsersList.size(); i++) {
	            	for (int j = i+1; j < outUsersList.size(); j++) {
	            		Text tempJ = new Text((outUsersList.get(j)).toString() + "," + inpUser.toString());
	            		ctx.write(new LongWritable(outUsersList.get(i)), tempJ);
	                
	                Text tempI = new Text((outUsersList.get(i)).toString() + "," + inpUser.toString());
	                ctx.write(new LongWritable(outUsersList.get(j)), tempI);
	            }
	        }
        }
    }
}

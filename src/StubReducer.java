import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StubReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
    @Override
    public void reduce(LongWritable key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {

        final HashMap<Long, List<Long>> commonFriendsMap = new HashMap<Long, List<Long>>();

        for (Text val : values) {
        	final String valInp = val.toString();
        	final long user = Long.parseLong(valInp.split(",")[0]);
        	final long commonFriend = Long.parseLong(valInp.split(",")[1]);
            final Boolean checkFriendFlag;
            if(commonFriend == -1){
            	checkFriendFlag = true;
            }
            else{
            	checkFriendFlag = false;
            }

            if (commonFriendsMap.containsKey(user)) {
                if (checkFriendFlag) {
                    commonFriendsMap.put(user, null);
                } 
                else if (commonFriendsMap.get(user) != null) {
                	List<Long> tempList = commonFriendsMap.get(user);
                	tempList.add(commonFriend);
                }
            } 
            else {
                if (!checkFriendFlag) {
                	ArrayList<Long> temp = new ArrayList<Long>();
                	temp.add(commonFriend);
                    commonFriendsMap.put(user, temp);
                } 
                else {
                    commonFriendsMap.put(user, null);
                }
            }
        }

        //https://stackoverflow.com/questions/12947088/java-treemap-comparator
        Comparator<Long> sortByPreferenceKey = new Comparator<Long>() {
            @Override
            public int compare(Long key1, Long key2) {
                Integer v1 = commonFriendsMap.get(key1).size();
                Integer v2 = commonFriendsMap.get(key2).size();
                if (v1 > v2) {
                    return -1;
                }
                //ordering if they are equal
                else if (v1.equals(v2) && key1 < key2) {
                    return -1;
                } 
                else {
                    return 1;
                }
            }
		};
        SortedMap<Long, List<Long>> sortedcommonFriendsMap = new TreeMap<Long, List<Long>>(sortByPreferenceKey);

        for (Map.Entry<Long, List<Long>> entry : commonFriendsMap.entrySet()) {
            if (entry.getValue() != null) {
                sortedcommonFriendsMap.put(entry.getKey(), entry.getValue());
            }
        }

        int counter = 0; String output = "";
        for (Map.Entry<Long, List<Long>> entry : sortedcommonFriendsMap.entrySet()) {
        		if(counter == 10)
        			break;
            output += entry.getKey().toString() + ",";
            counter++;
        }
        ctx.write(key, new Text(output));
    }
}

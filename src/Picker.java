public class Picker {

    public static String pick(String record, String target){
        String des = null;
        String[] strs = record.split(",");
        for(String str:strs){
            if(str.contains(target)) {
                str = str.split(":")[1];
                int start = str.indexOf("\"");
                int end = str.lastIndexOf("\"");
                des = str.substring(start+1, end);
            }
        }
        return des;
    }

}

import java.util.*;
import java.util.stream.Collectors;

public class Test {
    // 1 <= s.length <= 1000
    public void re(int[] candidates, int target, int index, List<List<Integer>> result, List<Integer> temp){
        if(index>candidates.length-1){
            return;
        }
        if(target==0){
            result.add(new ArrayList<>(temp));
            return;
        }
        if(target<0){
            return;
        }
        temp.add(candidates[index]);
        re(candidates, target-candidates[index], index, result,temp);
        re(candidates, target-candidates[index], index+1, result,temp);
        temp.remove(temp.size()-1);
    }

    public static void main(String[] args) {
        List<List<Integer>> result=new ArrayList<>();
        List<Integer> temp=new ArrayList<>();
        int[] nums={2,3,6,7};
        Test t=new Test();
        t.re(nums,7,0,result,temp);
    }

    public int trap(int[] height) {
        int sum=0;
        int i=0;
        Stack<Integer> stack=new Stack<>();
        while(i<height.length){
            if(stack.size()==0){
                stack.push(i);
                i++;
            }else {
                if(height[i]<stack.peek()){
                    stack.push(i);
                    i++;
                }else {
                    if(stack.size()>=2){
                        int index=stack.pop();
                        sum+=(Math.min(height[index],height[stack.peek()])-height[index])*(i-stack.peek()-1);
                    }else {
                        stack.pop();
                        stack.push(i);
                        i++;
                    }
                }
            }
        }
        return sum;
    }

//    public List<List<String>> groupAnagrams(String[] strs) {
//        return new ArrayList<>(
//                Arrays.stream(strs)
//                        .collect(
//                                Collectors.groupingBy(str -> {
//                                    // 返回 str 排序后的结果。
//                                    // 按排序后的结果来grouping by，算子类似于 sql 里的 group by。
//                                    char[] array = str.toCharArray();
//                                    Arrays.sort(array);
//                                    return new String(array);
//                                })
//                        ).values()
//        );
//    }


    public List<List<String>> groupAnagrams(String[] strs) {
        Map<String,List<String>> result=new HashMap<>();
        for(String s:strs){
            String key=genMast(s);
            List<String> list=result.getOrDefault(key,new ArrayList<>());
            list.add(s);
            result.put(key,list);
        }
        return new ArrayList<>(result.values());
    }
    public String genMast(String s){
        int[] array=new int[26];
        for(char c:s.toCharArray()){
            array[c-'a']++;
        }
        StringBuilder sb=new StringBuilder();
        for(int i=0; i<array.length; i++){
            if(array[i]!=0){
                sb.append((char)(i+'a'));
                sb.append(array[i]);
            }
        }
        return sb.toString();
    }

    public int[][] merge(int[][] intervals) {
        List<int[]> resultList=new ArrayList<>();
        Arrays.sort(intervals, Comparator.comparingInt(o -> o[0]));
        int[] temp=intervals[0];
        for(int i=1; i<intervals.length; i++){
            //无重叠
            if(temp[1]<intervals[i][0]){
                resultList.add(temp);
                temp=intervals[i];
            }else{
                temp[0]=Math.min(temp[0],intervals[i][0]);
                temp[1]=Math.max(temp[1],intervals[i][1]);
            }
        }
        resultList.add(temp);
        int[][]resutlArray=new int[resultList.size()][2];
        return resultList.toArray(resutlArray);
    }
}

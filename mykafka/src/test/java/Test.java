import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Test {
    //输入：s = "ADOBECODEBANC", t = "ABC"
    //输出："BANC"
    public List<List<Integer>> subsets(int[] nums) {
        List<List<Integer>> result=new ArrayList<>();
        List<Integer> temp=new ArrayList<>();
        back(result,temp,nums,0);
        return result;

    }

    public void back(List<List<Integer>> result, List<Integer> temp, int[] nums, int index){
        if(index>nums.length){
            return;
        }

        for(int i=index; i<nums.length; i++){
            temp.add(nums[i]);
            back(result,temp,nums,index+1);
            temp.remove(temp.size()-1);
        }

    }

    public static void main(String[] args) {
        Test test=new Test();
        int[] ar={1,2,3};
        test.subsets(ar);
    }
}

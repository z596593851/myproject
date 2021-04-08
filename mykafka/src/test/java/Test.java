import java.util.*;

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
}

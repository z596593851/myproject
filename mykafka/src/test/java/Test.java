import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Test {
    // 1 <= s.length <= 1000
    public String longestPalindrome(String s) {
        int len=s.length();
        int x=0, y=0;
        int max=1;
        boolean dp[][]=new boolean[len][len];
        for(int i=0; i<len-1; i++){
            if(s.charAt(i)==s.charAt(i+1)){
                dp[i][i+1]=true;
                max=2;
                x=i;
                y=i+1;
            }
        }
        for(int i=0; i<len; i++){
            dp[i][i]=true;
        }
        for(int gap=2; gap<len; gap++){
            for(int i=0; i+gap<len; i++){
                int j=i+gap;
                if(s.charAt(i)==s.charAt(j) && dp[i+1][j-1]){
                    dp[i][j]=true;
                    if(j-i+1>max){
                        max=j-i+1;
                        x=i;
                        y=j;
                    }
                }
            }
        }
        return s.substring(x,y+1);
    }

    public static void main(String[] args) {

    }
}

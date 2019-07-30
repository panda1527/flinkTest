import java.util.*;
public class Test {
    public static void main(String[] args){
        Scanner in=new Scanner(System.in);
        String str1=in.next();
        String str2=in.next();
        char[] c1=str1.toCharArray();
        char[] c2=str2.toCharArray();
        int l=c2.length;
        String res="";
        for(int i=0;i<l;i++){
            String temp=mul(c1,c2[i]-'0',l-i-1);
            res=add(res,temp);
            System.out.println(temp);
        }
        System.out.println(res);
        return;
    }
    public static String add(String s1,String s2){
        char[] c1=s1.toCharArray();
        char[] c2=s2.toCharArray();
        int len1=c1.length-1;
        int len2=c2.length-1;
        String res="";
        int pre=0;
        while(len1>=0&&len2>=0){
            res+=((c1[len1]-'0')+(c2[len2]-'0')+pre)%10;
            pre=((c1[len1]-'0')+(c2[len2]-'0')+pre)/10;
            len1--;
            len2--;
        }
        while(len1>=0){
            res+=((c1[len1]-'0')+pre)%10;
            pre=((c1[len1]-'0')+pre)/10;
            len1--;
        }
        while(len2>=0){
            res+=((c2[len2]-'0')+pre)%10;
            pre=((c2[len2]-'0')+pre)/10;
            len2--;
        }
        StringBuffer sb = new StringBuffer(res);
        return sb.reverse().toString();
    }
    public static String mul(char[] c,int i,int d){
        String res="";
        int len=c.length-1;
        int pre=0;
        System.out.println("乘法内： " +i);
        while(len>=0){
            res+=((c[len]-'0')*i+pre)%10;
            pre=((c[len]-'0')*i+pre)/10;
            len--;
            System.out.println(res);
        }
        if(pre!=0){
            res+=pre;
        }
        StringBuffer sb = new StringBuffer(res);
        res=sb.reverse().toString();
        while(d>0){
            res+="0";
            d--;
        }
        return res;
    }
}
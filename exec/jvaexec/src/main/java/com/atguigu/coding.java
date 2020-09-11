package com.atguigu;


import java.util.Arrays;

public class coding {
    public static void main(String[] args) {



/*  冒泡排序
    for(int i = 0 ; i < arr.length-1; i++){
            for(int j = 0 ;j <arr.length-1-i ;j ++){
                if(arr[j] >arr[j+1])	{
                    int tmp = arr[j];
                    arr[j] = arr[j+1];
                    arr[j+1] =  tmp; }
            }
        }*/

      /*  int[] n = new int[]{1,6,3,8,33,27,66,9,7,88};
        int temp,index = -1;
        for (int i = 0; i < n.length-1; i++) {
            index=i;
            //如果大于，暂存较小的数的下标
            for (int j = i+1; j <n.length; j++) {
                if(n[index]>n[j]){
                    index = j;
                }
            }
            ////将一趟下来求出的最小数，与这个数交换
            if(index>0){
                temp = n[i];
                n[i] = n[index];
                n[index] = temp;
            }
            System.out.println(Arrays.toString(n));
        }
        System.out.println(Arrays.toString(n));
*/


/*        //直接插入排序
        int[] n = new int[]{20,12,15,1,5,49,58,24,578,211,20,214,78,35,125,789,11};
        int temp =0,j;
        for (int i = 1; i < n.length; i++) {
            temp = n[i];
            for (j = i; j >0; j--) {
                //如果当前数前面的数大于当前数，则把前面的数向后移一个位置
                if(n[j-1]>temp){
                    n[j] = n[j-1];

                    //第一个数已经移到第二个数，将当前数放到第一个位置，这一趟结束
                    if(j==1){
                        n[j-1] = temp;
                        break;
                    }

                }else{//如果不大于，将当前数放到j的位置，这一趟结束

                    n[j] = temp;
                    break;
                }
            }
        }
        System.out.println(Arrays.toString(n));*/

        // 快排
        int[] arr = new int[]{10, 6, 3, 8, 33, 27, 66, 9, 7, 88};
        ff(arr,0,arr.length-1);
//        f(arr, 0, arr.length - 1);
        System.out.println(Arrays.toString(arr));
    }


   /* public static void f1(int[] arr, int start, int end){
        if (start< end){
            int left = start;
            int right = end;
            int temp = arr[start];
            while (left< right){
                while (left<right && arr[right]> temp){
                    right --;
                }
                arr[left]= arr[right];
                left++;
                while (left<right && arr[left] <temp){
                    left ++;
                }
                arr[right]=arr[left];
                right --;
            }
            arr[left] =temp;
            f1(arr,0,left);
            f1(arr,left+1,end);

            }



        }
*/
   public static void ff(int[] arr,int start,int end){
       //直到start=end时结束递归
       if(start<end){
           int left = start;
           int right = end;
           int temp = arr[start];

           while(left<right){

               //右面的数字大于标准数时，右边的数的位置不变，指针向左移一个位置
               while(left<right && arr[right]>temp){
                   right--;
               }

               //右边的数字小于或等于基本数，将右边的数放到左边
               arr[left] = arr[right];
               left++;

               //左边的数字小于或等于标准数时，左边的数的位置不变，指针向右移一个位置
               while(left<right && arr[left]<=temp){
                   left++;
               }

               //左边的数字大于基本数，将左边的数放到右边
               arr[right] = arr[left];
           }

           //一趟循环结束，此时left=right，将基数放到这个重合的位置，
           arr[left] = temp;
           System.out.println(Arrays.toString(arr));
           //将数组从left位置分为两半，继续递归下去进行排序
           ff(arr,start,left);
           ff(arr,left+1,end);
       }



   }




}

    class test3{
    public static void main(String args[])
        {for(int i=0;i<3;i++)
            {for( int j=3;j>=0;j--)
            {if(i==j)continue;
                System.out.println("i="+i+"j="+j);
            }}}}
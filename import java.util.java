import java.util.Scanner;
import java.util.Arrays;
import java.util.List; // Import List
import java.util.ArrayList; // Import ArrayList

public class Solution {

    /**
     * @brief Solves the Minimum Complexity Level problem.
     *
     * @param complexity A List of integers representing the complexity of each lecture.
     * @param d The number of days.
     * @return The minimum overall complexity level as a long.
     */
    // Changed parameter from int[] to List<Integer>
    public static long findMinComplexity(List<Integer> complexity, int d) {
        // Changed from .length to .size()
        int n = complexity.size(); 
        
        long[][] dp = new long[n][d + 1];
        
        for (int i = 0; i < n; i++) {
            Arrays.fill(dp[i], Long.MAX_VALUE);
        }

        long currentMax = 0;
        for (int i = 0; i < n; i++) {
            // Changed from complexity[i] to complexity.get(i)
            currentMax = Math.max(currentMax, (long)complexity.get(i));
            dp[i][1] = currentMax;
        }

        for (int j = 2; j <= d; j++) { // j = number of days
            for (int i = j - 1; i < n; i++) {
                
                long dayJMax = 0;
                
                for (int k = i; k >= j - 1; k--) {
                    
                    // Changed from complexity[k] to complexity.get(k)
                    dayJMax = Math.max(dayJMax, (long)complexity.get(k));

                    long costOfPrevDays = dp[k - 1][j - 1];

                    if (costOfPrevDays != Long.MAX_VALUE) {
                        dp[i][j] = Math.min(dp[i][j], costOfPrevDays + dayJMax);
                    }
                }
            }
        }

        return dp[n - 1][d];
    }

    // Main function to read input and run the solution
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);

        int n = sc.nextInt();

        // Read lecture complexities into a List<Integer>
        List<Integer> complexity = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            complexity.add(sc.nextInt());
        }

        int d = sc.nextInt();

        // Calculate and print the result
        long result = findMinComplexity(complexity, d);
        System.out.println(result);

        sc.close();
    }
}
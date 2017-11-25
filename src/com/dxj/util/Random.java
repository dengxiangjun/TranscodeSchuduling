package com.dxj.util;

public class Random {
    public static double nextDouble(double origin, double bound) {
        java.util.Random random = new java.util.Random();
        double r = random.nextDouble();
        if (origin < bound) {
            r = r * (bound - origin) + origin;
            if (r >= bound) // correct for rounding
                r = Double.longBitsToDouble(Double.doubleToLongBits(bound) - 1);
        }
        return r;
    }

    public static int nextInt(int origin, int bound) {
        java.util.Random random = new java.util.Random();
        if (origin < bound) {
            int n = bound - origin;
            if (n > 0) {
                return random.nextInt(n) + origin;
            }
            else {  // range not representable as int
                int r;
                do {
                    r = random.nextInt();
                } while (r < origin || r >= bound);
                return r;
            }
        }
        else {
            return random.nextInt();
        }
    }
}

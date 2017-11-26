
package edu.uci.jun.database;

import java.util.Arrays;

public class MergeInterval {
    public int find(Interval[] intervals, Interval target) {
        if (intervals == null || intervals.length == 0) {
            return -1;
        }
        Arrays.sort(intervals, (a, b) -> a.start - b.start);
        int res = 0;
        int i = 0;
        int start = target.start;
        while (i < intervals.length) {
            int cur = greedy(intervals, i, start);
            res++;
            if (intervals[cur].end >= target.end) return res;
            i = cur;
            start = intervals[cur].end;
        }
        return -1;
    }
//    find start < target.start 并且 end 最大的 index
    public int greedy(Interval[] intervals, int i, int tar) {
        int res = i;
        while (i < intervals.length) {
            if (intervals[i].start <= tar && intervals[i].end > intervals[res].end) {
                res = i;
            } else if (intervals[i].start > tar) return res;
            i++;
        }
        return res;
    }
    public static void main(String [] args){
        MergeInterval mergeInterval = new MergeInterval();
        Interval[] intervals = new Interval[7];
        intervals[0] = new Interval(-1, 9);
        intervals[1] = new Interval(0,  3);
        intervals[2] = new Interval(1, 10);
        intervals[3] = new Interval(2, 9);
        intervals[4] = new Interval(3, 14);
        intervals[5] = new Interval(9, 10);
        intervals[6] = new Interval(10, 16);
        int greedy = mergeInterval.find(intervals, new Interval(2, 15));
        System.out.println(greedy);

    }
}
class Interval {
    int start;
    int end;

    Interval(int s, int e) {
        start = s;
        end = e;
    }
    @Override
    public String toString() {
        return "Interval{" +
                "start=" + start +
                ", end=" + end +
                '}';
    }
}


# Writeup

## Describe any design decisions you made.

1. **Fixed-Width Buckets**  
   The histogram splits the range `[min, max]` into equally sized sections, or "buckets." This helps save memory and makes it easy to predict how long operations will take when adding values or estimating selectivity.

2. **Error Handling During Setup**  
   When setting up the histogram, the constructor checks that the input values for `min` and `max` are valid. If not, it raises an error to prevent problems during initialization.

3. **Fractional Bucket Contributions**  
   For range queries like `LT` or `LE`, the contribution of a value is calculated based on how far it is within a bucket. This improves the accuracy of selectivity estimation.
---
## Describe any missing or incomplete elements of your code.

1. Everything is completed

## Describe how long you spent on the assignment and whether there was anything you found particularly difficult or confusing.

I spent approximately 8 hours on this assignment

Personally, I had a difficult time passing the `ColumnStatsTest.less_than_or_equals` test. I later on discovered that the issue might of came from calculating how much of a bucket should be included when the value `v` was right at the edge of the bucket. When debugging and estimating values "less than or equal to `v`," i realized that the code needed to figure out how much of the last bucket to count without overestimating or underestimating.
So when debugging i realized my calculation was incorrect, after communication with team make and debugging we concluded with the formula:
`bucketFraction = (v - bucketRangeStart + 1) / bucket_width_;` which passed the test. 

---

## If you collaborated with someone else, describe how you split the workload.

I worked with Yu Liang on this assignment. We both first worked on our own and have our design decisions and code implementation. After working on most of the part and passing most of the test we communicated and collaborated our code together finalizing and passing all the tests. 
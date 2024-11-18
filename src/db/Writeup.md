# Writeup

## Describe any design decisions you made.

1. **Projection**  
   The `DbFile` processes the input and extracts only the specified fields, placing them in the output `DbFile`. By using `TupleDesc` and the `index_of` method, the field order and schema compatibility are preserved, ensuring that the output matches the required structure.

2. **Filters**  
   The filter operation evaluates each tuple against a list of predicates. Tuples that satisfy all conditions are written to the output. Each predicate is applied using the `PredicateOp` logic, allowing for flexibility in handling complex conditions.

3. **Aggregate**  
   For grouping and aggregation, hash maps are used to store results and counts. The use of `std::variant` ensures safe handling of numeric fields of varying types. This design enables efficient processing, particularly when working with large datasets.

4. **Join**  
   Equality joins are optimized by using a hash table to quickly match keys, while inequality joins compare tuples pair by pair. The hash-based approach is highly efficient for equality conditions in large datasets, while the nested loop approach ensures all conditions are checked for inequality joins.

5. **Errors**  
   To handle invalid inputs, the code throws `std::runtime_error` exceptions with clear error messages. This makes debugging straightforward by pinpointing the issue.

## Describe any missing or incomplete elements of your code.
We have successfully passed all provided tests, which confirms that no parts of the code are missing or incomplete.

## Describe how long you spent on the assignment and whether there was anything you found particularly difficult or confusing.
I spent about an hour each day working on this project. One of the hardest challenges was implementing the `min` and `sum` functions for the aggregate operation. The main challenge was managing numeric fields of varying types, such as `int` and `double`, to ensure accurate handling. To address this, I utilized `std::variant` to ensure type safety and leveraged `std::visit` to process and compare the fields reliably and consistently.


## If you collaborated with someone else, describe how you split the workload.
I worked on this project with Yu Liang. While I concentrated on the `join` functionality, improving the join operations. I addressed schema compatibility issues and optimized the join by using `std::unordered_multimap` for handling duplicates in equality joins. Yu worked more on aggreate input, we lat4er combined our efforts, which allowed us to successfully complete the assignment and pass all tests.
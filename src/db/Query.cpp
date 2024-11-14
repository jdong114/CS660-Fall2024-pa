#include <db/Query.hpp>
#include <db/HeapFile.hpp>
#include <db/BTreeFile.hpp>
#include <unordered_map>
#include <stdexcept>
#include <limits>
#include <variant>
#include <type_traits>
#include <vector>

using namespace db;

// Projection function: projects specified fields from input DbFile to output DbFile
void db::projection(const DbFile &in, DbFile &out, const std::vector<std::string> &field_names) {
    const TupleDesc &schema = in.getTupleDesc();  // Retrieve schema information

    // Iterate over each tuple in the DbFile
    for (auto it = in.begin(); it != in.end(); in.next(it)) {
        const Tuple &tuple = in.getTuple(it);
        std::vector<field_t> projectedFields;

        // Extract only the specified fields
        for (const auto &field_name : field_names) {
            size_t fieldIndex = schema.index_of(field_name);  // Get field index
            projectedFields.push_back(tuple.get_field(fieldIndex));  // Add specified field
        }

        out.insertTuple(Tuple(projectedFields));  // Insert into output file
    }
}

void db::filter(const DbFile &in, DbFile &out, const std::vector<FilterPredicate> &pred) {
    const TupleDesc &schema = in.getTupleDesc();

    // Iterate over each tuple in the DbFile
    for (auto it = in.begin(); it != in.end(); in.next(it)) {
        const Tuple &tuple = in.getTuple(it);
        bool matches = true;

        // Check each predicate condition
        for (const auto &p : pred) {
            size_t fieldIndex = schema.index_of(p.field_name);
            const field_t &field_value = tuple.get_field(fieldIndex);

            // Perform comparison based on PredicateOp
            switch (p.op) {
                case PredicateOp::EQ:  matches = (field_value == p.value); break;
                case PredicateOp::NE:  matches = (field_value != p.value); break;
                case PredicateOp::LT:  matches = (field_value < p.value); break;
                case PredicateOp::LE:  matches = (field_value <= p.value); break;
                case PredicateOp::GT:  matches = (field_value > p.value); break;
                case PredicateOp::GE:  matches = (field_value >= p.value); break;
                default: throw std::runtime_error("Unsupported comparison operator");
            }

            if (!matches) break;  // Exit if not matching
        }

        if (matches) {
            out.insertTuple(tuple);  // Insert tuple that meets criteria
        }
    }
}

void db::aggregate(const DbFile &in, DbFile &out, const Aggregate &agg) {
    const TupleDesc &schema = in.getTupleDesc();
    size_t fieldIndex = schema.index_of(agg.field);
    std::unordered_map<field_t, double> groupResults;
    std::unordered_map<field_t, int> groupCounts;

    bool hasGroup = agg.group.has_value();
    size_t groupIndex = hasGroup ? schema.index_of(agg.group.value()) : 0;

    double globalResult = 0;
    int globalCount = 0;
    double minResult = std::numeric_limits<double>::max();
    double maxResult = std::numeric_limits<double>::lowest();
    bool hasValues = false;  // Check if valid values were processed

    // Iterate over each tuple in the DbFile
    for (auto it = in.begin(); it != in.end(); in.next(it)) {
        const Tuple &tuple = in.getTuple(it);

        // Ensure only numeric types are processed
        double value = std::visit([](auto&& arg) -> double {
            if constexpr (std::is_arithmetic_v<std::decay_t<decltype(arg)>>) {
                return static_cast<double>(arg);
            } else {
                throw std::runtime_error("Non-numeric field encountered in aggregate operation");
            }
        }, tuple.get_field(fieldIndex));

        hasValues = true;  // Valid value processed
        field_t groupKey = hasGroup ? tuple.get_field(groupIndex) : field_t{};

        // Perform calculation based on aggregate operation type
        switch (agg.op) {
            case AggregateOp::SUM:
            case AggregateOp::AVG:
                if (hasGroup) {
                    groupResults[groupKey] += value;
                    groupCounts[groupKey]++;
                } else {
                    globalResult += value;
                    globalCount++;
                }
                break;

            case AggregateOp::MIN:
                if (hasGroup) {
                    if (groupResults.find(groupKey) == groupResults.end() || value < groupResults[groupKey]) {
                        groupResults[groupKey] = value;
                    }
                } else {
                    minResult = std::min(minResult, value);
                }
                break;

            case AggregateOp::MAX:
                if (hasGroup) {
                    if (groupResults.find(groupKey) == groupResults.end() || value > groupResults[groupKey]) {
                        groupResults[groupKey] = value;
                    }
                } else {
                    maxResult = std::max(maxResult, value);
                }
                break;

            case AggregateOp::COUNT:
                if (hasGroup) {
                    groupCounts[groupKey]++;
                } else {
                    globalCount++;
                }
                break;

            default:
                throw std::runtime_error("Unsupported aggregate operation");
        }
    }

    // Process and insert aggregate results
    if (hasGroup) {
        for (const auto &[groupKey, result] : groupResults) {
            double outputValue = result;
            if (agg.op == AggregateOp::AVG) {
                outputValue /= groupCounts[groupKey];
            } else if (agg.op == AggregateOp::COUNT) {
                outputValue = groupCounts[groupKey];
            }
            out.insertTuple(Tuple({groupKey, field_t(outputValue)}));
        }
    } else {
        field_t finalResult;
        switch (agg.op) {
            case AggregateOp::SUM:
                finalResult = field_t(static_cast<int>(globalResult));
                break;
            case AggregateOp::AVG:
                finalResult = field_t(globalCount > 0 ? globalResult / globalCount : 0);
                break;
            case AggregateOp::MIN:
                finalResult = field_t(hasValues ? static_cast<int>(minResult) : 0);
                break;
            case AggregateOp::MAX:
                finalResult = field_t(hasValues ? static_cast<int>(maxResult) : 0);
                break;
            case AggregateOp::COUNT:
                finalResult = field_t(globalCount);
                break;
            default:
                throw std::runtime_error("Unsupported aggregate operation");
        }
        out.insertTuple(Tuple({finalResult}));  // Insert non-grouped aggregate result
    }
}

void db::join(const DbFile &left, const DbFile &right, DbFile &out, const JoinPredicate &pred) {
    const TupleDesc &leftSchema = left.getTupleDesc();
    const TupleDesc &rightSchema = right.getTupleDesc();

    size_t leftFieldIndex = leftSchema.index_of(pred.left);
    size_t rightFieldIndex = rightSchema.index_of(pred.right);

    std::unordered_multimap<field_t, Tuple> hashTable;

    // Choose join logic based on predicate type
    if (pred.op == PredicateOp::EQ) {
        // Build hash table for equality join
        for (auto it = left.begin(); it != left.end(); left.next(it)) {
            const Tuple &tupleLeft = left.getTuple(it);
            field_t key = tupleLeft.get_field(leftFieldIndex);
            hashTable.emplace(key, tupleLeft);  // Use unordered_multimap for duplicate keys
        }

        // Iterate over right table for equality join
        for (auto it = right.begin(); it != right.end(); right.next(it)) {
            const Tuple &tupleRight = right.getTuple(it);
            field_t key = tupleRight.get_field(rightFieldIndex);

            // Find matching keys and join
            auto range = hashTable.equal_range(key);
            for (auto iter = range.first; iter != range.second; ++iter) {
                const Tuple &tupleLeft = iter->second;

                // Combine fields in TupleDesc order
                std::vector<field_t> combinedFields;
                combinedFields.reserve(tupleLeft.size() + tupleRight.size() - 1);

                // Add left table fields
                for (size_t j = 0; j < tupleLeft.size(); j++) {
                    combinedFields.push_back(tupleLeft.get_field(j));
                }

                // Add right table fields, excluding the join field
                for (size_t k = 0; k < tupleRight.size(); k++) {
                    if (k != rightFieldIndex) {  // Skip the joining field from the right table
                        combinedFields.push_back(tupleRight.get_field(k));
                    }
                }

                // Insert combined tuple into the output DbFile
                out.insertTuple(Tuple(combinedFields));
            }
        }
    } else if (pred.op == PredicateOp::NE) {
        // For inequality join, iterate over both tables and join non-equal tuples
        for (auto leftIt = left.begin(); leftIt != left.end(); left.next(leftIt)) {
            const Tuple &tupleLeft = left.getTuple(leftIt);
            field_t leftKey = tupleLeft.get_field(leftFieldIndex);

            for (auto rightIt = right.begin(); rightIt != right.end(); right.next(rightIt)) {
                const Tuple &tupleRight = right.getTuple(rightIt);
                field_t rightKey = tupleRight.get_field(rightFieldIndex);

                // Join only if keys are not equal
                if (leftKey != rightKey) {
                    std::vector<field_t> combinedFields;
                    combinedFields.reserve(tupleLeft.size() + tupleRight.size());

                    // Add fields from left and right tables
                    for (size_t j = 0; j < tupleLeft.size(); j++) {
                        combinedFields.push_back(tupleLeft.get_field(j));
                    }
                    for (size_t k = 0; k < tupleRight.size(); k++) {
                        combinedFields.push_back(tupleRight.get_field(k));
                    }

                    out.insertTuple(Tuple(combinedFields));
                }
            }
        }
    } else {
        throw std::runtime_error("Unsupported join predicate operation");
    }
}


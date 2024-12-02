#include <db/ColumnStats.hpp>
#include <stdexcept>
#include <cmath>

namespace db {

ColumnStats::ColumnStats(unsigned buckets, int min, int max)
    : buckets_(buckets), min_(min), max_(max), total_count_(0) {
  if (min >= max) {
    throw std::invalid_argument("Invalid range for ColumnStats: min must be less than max");
  }
  bucket_width_ = static_cast<double>((max + 1) - min) / buckets_;
  histogram_.resize(buckets_, 0);
}

void ColumnStats::addValue(int v) {
  if (v < min_ || v > max_) {
    return; // Ignore values outside the range
  }
  int bucketIndex = static_cast<int>((v - min_) / bucket_width_);
  if (bucketIndex >= static_cast<int>(buckets_)) {
    bucketIndex = buckets_ - 1; // Ensure max value lands in the last bucket
  }
  histogram_[bucketIndex]++;
  total_count_++;
}

size_t ColumnStats::estimateCardinality(PredicateOp op, int v) const {
  if (total_count_ == 0) {
    return 0; // No data in the histogram
  }

  if (v < min_) {
    if (op == PredicateOp::LT || op == PredicateOp::LE) {
      return 0;
    }
    return total_count_;
  }
  if (v > max_) {
    if (op == PredicateOp::GT || op == PredicateOp::GE) {
      return 0;
    }
    return total_count_;
  }

  int bucketIndex = static_cast<int>((v - min_) / bucket_width_);
  if (bucketIndex >= static_cast<int>(buckets_)) {
    bucketIndex = buckets_ - 1;
  }

  size_t count = 0;
  double bucketRangeStart = min_ + bucketIndex * bucket_width_;
  double bucketRangeEnd = bucketRangeStart + bucket_width_;
  double bucketFraction;

  switch (op) {
    case PredicateOp::EQ: {
      if (bucket_width_ < 1.0) {
        count = histogram_[bucketIndex];
      } else {
        bucketFraction = 1.0 / bucket_width_;
        count = static_cast<size_t>(histogram_[bucketIndex] * bucketFraction);
      }
      break;
    }

    case PredicateOp::NE: {
      if (bucket_width_ < 1.0) {
        size_t eqCount = histogram_[bucketIndex];
        count = total_count_ - eqCount;
      } else {
        bucketFraction = 1.0 / bucket_width_;
        size_t eqCount = static_cast<size_t>(histogram_[bucketIndex] * bucketFraction);
        count = total_count_ - eqCount;
      }
      break;
    }

    case PredicateOp::LT: {
      for (int i = 0; i < bucketIndex; ++i) {
        count += histogram_[i];
      }
      bucketFraction = (v - bucketRangeStart) / bucket_width_;
      count += static_cast<size_t>(histogram_[bucketIndex] * bucketFraction);
      break;
    }

    case PredicateOp::LE: {
      for (int i = 0; i < bucketIndex; ++i) {
        count += histogram_[i];
      }
      if (bucket_width_ < 1.0) {
        count += histogram_[bucketIndex];
      } else {
        bucketFraction = (v - bucketRangeStart + 1) / bucket_width_;
        count += static_cast<size_t>(histogram_[bucketIndex] * bucketFraction);
      }
      break;
    }

    case PredicateOp::GT: {
      if (bucket_width_ < 1.0) {
        for (int i = bucketIndex + 1; i < static_cast<int>(buckets_); ++i) {
          count += histogram_[i];
        }
      } else {
        bucketFraction = (bucketRangeEnd - v - 1) / bucket_width_;
        count += static_cast<size_t>(histogram_[bucketIndex] * bucketFraction);
        for (int i = bucketIndex + 1; i < static_cast<int>(buckets_); ++i) {
          count += histogram_[i];
        }
      }
      break;
    }

    case PredicateOp::GE: {
      if (bucket_width_ < 1.0) {
        count += histogram_[bucketIndex];
      } else {
        bucketFraction = (bucketRangeEnd - v) / bucket_width_;
        count += static_cast<size_t>(histogram_[bucketIndex] * bucketFraction);
      }
      for (int i = bucketIndex + 1; i < static_cast<int>(buckets_); ++i) {
        count += histogram_[i];
      }
      break;
    }

    default:
      throw std::runtime_error("Unsupported PredicateOp in estimateCardinality");
  }
  return count;
}

} // namespace db

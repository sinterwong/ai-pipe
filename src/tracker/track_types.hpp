/**
 * @file track_types.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-03-21
 *
 * @copyright Copyright (c) 2025
 *
 */
#ifndef __CORE_TRACKER_HPP_
#define __CORE_TRACKER_HPP_

#include <utility>
#include <vector>
namespace ai_pipe::tracker {

enum class TrackableState { NEW = 0, ACTIVE, INACTIVE, TERMINATED };

struct AssociationResult {
  std::vector<std::pair<int, int>> matches;

  std::vector<int> unmatchedTrackables;

  std::vector<int> unmatchedDetections;
};

struct TrackingConfig {
  float similarityThreshold = 0.5f;

  float activationThreshold = 0.7f;

  float inactivationThreshold = 0.3f;

  float terminationThreshold = 0.1f;

  float weakDetectionThreshold = 0.2f;

  float displayThreshold = 0.3f;

  int maxHistorySize = 30;

  int maxConsecutiveMisses = 5;

  int scoreAverageWindowSize = 5;
};

} // namespace ai_pipe::tracker
#endif
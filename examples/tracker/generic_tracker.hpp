/**
 * @file generic_tracker.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-03-26
 *
 * @copyright Copyright (c) 2025
 *
 */

#ifndef __AI_PIPE_GENERIC_TRACKER_HPP__
#define __AI_PIPE_GENERIC_TRACKER_HPP__

#include <atomic>

#include "track_core.hpp"

namespace ai_pipe {

template <typename DetectionType> class GenericTracker {
public:
  explicit GenericTracker(
      const tracker::TrackingConfig &config = tracker::TrackingConfig());

  void reset();

  void track(const std::vector<DetectionType> &detections, int frameIndex);

  std::vector<std::shared_ptr<tracker::ITrackable>> getTrackables() const;

  std::vector<std::shared_ptr<tracker::ITrackable>> getActiveTrackables() const;

  std::vector<std::shared_ptr<tracker::ITrackable>> getValidTrackables() const;

  size_t getTrackableCount() const;

  void configure(const tracker::TrackingConfig &config);

  const tracker::TrackingConfig &getConfiguration() const;

  void setAssociator(std::unique_ptr<tracker::IAssociator> associator);

protected:
  virtual std::shared_ptr<tracker::IDetection>
  createDetection(const DetectionType &detection, int frameIndex) = 0;

private:
  void removeDuplicatedTrackables();

  void cleanTerminatedTrackables();

private:
  tracker::TrackingConfig config_;

  std::vector<std::shared_ptr<tracker::ITrackable>> trackables_;

  std::unique_ptr<tracker::IAssociator> associator_;

  std::atomic<int> nextTrackableId_;

  mutable std::mutex mutex_;
};
} // namespace ai_pipe

#endif

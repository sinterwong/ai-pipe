/**
 * @file generic_tracker.cpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-03-26
 *
 * @copyright Copyright (c) 2025
 *
 */
#include <ostream>

#include "generic_tracker.hpp"
#include "tracker/iou_associator.hpp"
#include "tracker/track_default.hpp"

#include <logger.hpp>

namespace ai_pipe {
using namespace tracker;

template <typename DetectionType>
GenericTracker<DetectionType>::GenericTracker(const TrackingConfig &config)
    : config_(config), nextTrackableId_(0) {
  associator_ = std::make_unique<IoUAssociator>(config_.similarityThreshold);
}

template <typename DetectionType> void GenericTracker<DetectionType>::reset() {
  std::lock_guard<std::mutex> guard(mutex_);
  trackables_.clear();
  nextTrackableId_ = 0;
}

template <typename DetectionType>
void GenericTracker<DetectionType>::track(
    const std::vector<DetectionType> &detections, int frameIndex) {
  std::lock_guard<std::mutex> guard(mutex_);

  std::vector<std::shared_ptr<IDetection>> currentDetections;
  currentDetections.reserve(detections.size());

  for (const auto &detection : detections) {
    currentDetections.push_back(createDetection(detection, frameIndex));
  }

  std::vector<std::shared_ptr<ITrackable>> activateTrackablesForAssociation;
  std::vector<size_t> originalIndices;
  activateTrackablesForAssociation.reserve(trackables_.size());
  originalIndices.reserve(trackables_.size());

  for (size_t i = 0; i < trackables_.size(); ++i) {
    if (trackables_[i]->getState() != TrackableState::TERMINATED) {
      activateTrackablesForAssociation.push_back(trackables_[i]);
      originalIndices.push_back(i);
    }
  }

  if (!associator_) {
    LOG_ERRORS << "Associator is not set in GenericTracker!";
    throw std::runtime_error("Associator is not set in GenericTracker!");
  }

  AssociationResult associationResult = associator_->associate(
      activateTrackablesForAssociation, currentDetections);

  std::vector<bool> updateFlags(activateTrackablesForAssociation.size(), false);
  for (const auto &match : associationResult.matches) {
    int trackableAssocIdx = match.first;
    int detectionAssocIdx = match.second;

    if (trackableAssocIdx >= 0 &&
        trackableAssocIdx < activateTrackablesForAssociation.size() &&
        detectionAssocIdx >= 0 &&
        detectionAssocIdx < currentDetections.size()) {
      activateTrackablesForAssociation[trackableAssocIdx]->update(
          currentDetections[detectionAssocIdx]);

      updateFlags[trackableAssocIdx] = true;
    } else {
      LOG_WARNINGS << "Invalid association index: " << trackableAssocIdx << " "
                   << detectionAssocIdx;
    }
  }

  // create new trackables for unmatched detections
  for (int detectionIdx : associationResult.unmatchedDetections) {
    if (detectionIdx >= 0 && detectionIdx < currentDetections.size()) {
      const auto &detection = currentDetections[detectionIdx];

      if (detection->getScore() > config_.weakDetectionThreshold) {
        // create a new trackable
        auto newTrackable = std::make_shared<DefaultTrackable>(
            detection, nextTrackableId_++, config_);
        trackables_.push_back(newTrackable);
      }
    } else {
      LOG_WARNINGS << "Invalid detection index: " << detectionIdx;
    }
  }

  // clean up terminated trackables
  cleanTerminatedTrackables();

  // remove duplicates
  removeDuplicatedTrackables();
}
} // namespace ai_pipe
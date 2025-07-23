/**
 * @file iou_associator.cpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-03-21
 *
 * @copyright Copyright (c) 2025
 *
 */

#include <numeric>
#include <unordered_map>

#include "iou_associator.hpp"

#include <opencv2/opencv.hpp>

namespace ai_pipe::tracker {

IoUAssociator::IoUAssociator(float iouThreshold)
    : iouThreshold_(iouThreshold) {}

AssociationResult IoUAssociator::associate(
    const std::vector<std::shared_ptr<ITrackable>> &trackables,
    const std::vector<std::shared_ptr<IDetection>> &detections) {

  AssociationResult result;

  if (trackables.empty() || detections.empty()) {
    result.unmatchedTrackables.resize(trackables.size());
    std::iota(result.unmatchedTrackables.begin(),
              result.unmatchedTrackables.end(), 0);

    result.unmatchedDetections.resize(detections.size());
    std::iota(result.unmatchedDetections.begin(),
              result.unmatchedDetections.end(), 0);

    return result;
  }

  // matching T & D with the same label
  std::unordered_map<int,
                     std::vector<std::pair<int, std::shared_ptr<ITrackable>>>>
      trackablesByLabel;

  std::unordered_map<int,
                     std::vector<std::pair<int, std::shared_ptr<IDetection>>>>
      detectionsByLabel;

  for (size_t i = 0; i < trackables.size(); ++i) {
    const auto &trackable = trackables[i];
    // don't care terminated trackable
    if (trackable->getState() != TrackableState::TERMINATED) {
      int label = trackable->getLabel();
      trackablesByLabel[label].emplace_back(i, trackable);
    } else {
      result.unmatchedTrackables.push_back(i);
    }
  }

  for (size_t i = 0; i < detections.size(); ++i) {
    const auto &detection = detections[i];
    int label = detection->getLabel();
    detectionsByLabel[label].emplace_back(i, detection);
  }

  for (const auto &[label, labelTrackables] : trackablesByLabel) {
    const auto &labelDetections = detectionsByLabel[label];

    if (labelDetections.empty()) {
      for (const auto &[trackableIdx, _] : labelTrackables) {
        result.unmatchedTrackables.push_back(trackableIdx);
      }
      continue;
    }

    std::vector<std::vector<float>> iouMatrix(
        labelTrackables.size(),
        std::vector<float>(labelDetections.size(), 0.0f));

    for (size_t i = 0; i < labelTrackables.size(); ++i) {
      const auto &trackable = labelTrackables[i].second;
      const auto latestDetection = trackable->getLatestDetection();

      if (!latestDetection) {
        continue;
      }

      ai_core::BBox trackableBbox = latestDetection->getBoundingBox();

      for (size_t j = 0; j < labelDetections.size(); ++j) {
        const auto &detection = labelDetections[j].second;
        ai_core::BBox detectionBbox = detection->getBoundingBox();

        float iou = calculateIOU(trackableBbox, detectionBbox);
        iouMatrix[i][j] = iou;
      }
    }

    std::vector<bool> assignedTrackables(labelTrackables.size(), false);
    std::vector<bool> assignedDetections(labelDetections.size(), false);

    // greedy match
    bool foundMatch;
    do {
      foundMatch = false;
      float bestIoU = iouThreshold_;
      int bestTrackableIdx = -1;
      int bestDetectionIdx = -1;

      for (size_t i = 0; i < labelTrackables.size(); ++i) {
        if (assignedTrackables[i])
          continue;

        for (size_t j = 0; j < labelDetections.size(); ++j) {
          if (assignedDetections[j])
            continue;

          if (iouMatrix[i][j] > bestIoU) {
            bestIoU = iouMatrix[i][j];
            bestTrackableIdx = i;
            bestDetectionIdx = j;
            foundMatch = true;
          }
        }
      }

      if (foundMatch) {
        result.matches.emplace_back(labelTrackables[bestTrackableIdx].first,
                                    labelDetections[bestDetectionIdx].first);
        assignedTrackables[bestTrackableIdx] = true;
        assignedDetections[bestDetectionIdx] = true;
      }
    } while (foundMatch);

    for (size_t i = 0; i < labelTrackables.size(); ++i) {
      if (!assignedTrackables[i]) {
        result.unmatchedTrackables.push_back(labelTrackables[i].first);
      }
    }

    for (size_t j = 0; j < labelDetections.size(); ++j) {
      if (!assignedDetections[j]) {
        result.unmatchedDetections.push_back(labelDetections[j].first);
      }
    }
  }

  for (const auto &[label, labelDetections] : detectionsByLabel) {
    if (trackablesByLabel.find(label) == trackablesByLabel.end()) {
      for (const auto &[detectionIdx, _] : labelDetections) {
        result.unmatchedDetections.push_back(detectionIdx);
      }
    }
  }
  return result;
}

float IoUAssociator::calculateIOU(const ai_core::BBox &bbox1,
                                  const ai_core::BBox &bbox2) const {
  float x1 = std::max(bbox1.rect->x, bbox2.rect->x);
  float y1 = std::max(bbox1.rect->y, bbox2.rect->y);
  float x2 = std::min(bbox1.rect->x + bbox1.rect->width,
                      bbox2.rect->x + bbox2.rect->width);
  float y2 = std::min(bbox1.rect->y + bbox1.rect->height,
                      bbox2.rect->y + bbox2.rect->height);

  float intersectionWidth = std::max(0.0f, x2 - x1);
  float intersectionHeight = std::max(0.0f, y2 - y1);

  float intersectionArea = intersectionWidth * intersectionHeight;
  float bbox1Area = bbox1.rect->width * bbox1.rect->height;
  float bbox2Area = bbox2.rect->width * bbox2.rect->height;

  return intersectionArea / (bbox1Area + bbox2Area - intersectionArea);
}

} // namespace ai_pipe::tracker

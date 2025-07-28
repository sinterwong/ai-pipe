/**
 * @file track_core.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-03-21
 *
 * @copyright Copyright (c) 2025
 *
 */
#ifndef __CORE_TRACKER_CORE_HPP_
#define __CORE_TRACKER_CORE_HPP_

#include <memory>

#include "track_types.hpp"

#include <ai_core/algo_output_types.hpp>

namespace ai_pipe::tracker {
class IDetection {
public:
  virtual ~IDetection() = default;

  virtual ai_core::BBox getBoundingBox() const = 0;

  virtual int getLabel() const = 0;

  virtual float getScore() const = 0;

  virtual int getId() const = 0;

  virtual int getFrameIndex() const = 0;
};

class ITrackable {
public:
  virtual ~ITrackable() = default;

  virtual void update(std::shared_ptr<IDetection> detection) = 0;

  virtual void predict() = 0;

  virtual TrackableState getState() const = 0;

  virtual std::vector<std::shared_ptr<IDetection>> getHistory() const = 0;

  virtual std::shared_ptr<IDetection> getLatestDetection() const = 0;

  virtual int getId() const = 0;

  virtual int getLabel() const = 0;

  virtual float calculateScore() const = 0;

  virtual int getMissingCount() const = 0;
};

class IAssociator {
public:
  virtual ~IAssociator() = default;

  virtual AssociationResult
  associate(const std::vector<std::shared_ptr<ITrackable>> &trackables,
            const std::vector<std::shared_ptr<IDetection>> &detections) = 0;
};

} // namespace ai_pipe::tracker

#endif

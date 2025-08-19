#include "track_default.hpp"
#include <algorithm>
#include <vector>

namespace ai_pipe::tracker {

DefaultTrackable::DefaultTrackable(std::shared_ptr<IDetection> initDet,
                                   int trackId, const TrackingConfig &config)
    : id_(trackId), state_(TrackableState::NEW),
      config_(config) // Store config by value
{
  if (initDet) {
    update(initDet);
  }
}

void DefaultTrackable::updateState_nolock() {
  float score = calculateScore_nolock();

  if (consecutiveMisses_ >= config_.maxConsecutiveMisses) {
    state_ = TrackableState::TERMINATED;
  } else if (state_ != TrackableState::TERMINATED &&
             score < config_.terminationThreshold) {
    state_ = TrackableState::TERMINATED;
  } else if (state_ != TrackableState::TERMINATED &&
             score >= config_.activationThreshold) {
    state_ = TrackableState::ACTIVE;
  } else if (state_ != TrackableState::TERMINATED &&
             score >= config_.inactivationThreshold) {
    if (state_ != TrackableState::ACTIVE) {
      state_ = TrackableState::INACTIVE;
    }
  } else if (state_ != TrackableState::TERMINATED) {
    if (state_ == TrackableState::ACTIVE ||
        state_ == TrackableState::INACTIVE) {
      state_ = TrackableState::INACTIVE;
    } else {
      state_ = TrackableState::NEW;
    }
  }
}

float DefaultTrackable::calculateScore_nolock() const {
  if (history_.empty()) {
    return 0.0f;
  }

  const size_t scoreWindow = config_.maxHistorySize > 0
                                 ? static_cast<size_t>(config_.maxHistorySize)
                                 : config_.scoreAverageWindowSize;

  const size_t numSamples = std::min(history_.size(), scoreWindow);

  float sum = 0.0f;
  int count = 0;

  auto it = history_.rbegin();
  for (size_t i = 0; i < numSamples && it != history_.rend(); ++i, ++it) {
    if (*it) {
      sum += (*it)->getScore();
      count++;
    }
  }
  return count > 0 ? sum / count : 0.0f;
}

void DefaultTrackable::addHistory_nolock(
    std::shared_ptr<IDetection> detection) {
  history_.push_back(detection);

  // limit history size
  if (config_.maxHistorySize > 0 &&
      history_.size() > static_cast<size_t>(config_.maxHistorySize)) {
    history_.pop_front();
  }
}

void DefaultTrackable::update(std::shared_ptr<IDetection> detection) {
  std::lock_guard<std::mutex> guard(mutex_);

  if (detection) {
    consecutiveMisses_ = 0;
    addHistory_nolock(detection);
  } else {
    consecutiveMisses_++;
    addHistory_nolock(nullptr);
  }
  updateState_nolock();
}

void DefaultTrackable::predict() {
  std::lock_guard<std::mutex> guard(mutex_);
  consecutiveMisses_++;
  addHistory_nolock(nullptr);
  updateState_nolock();
}

float DefaultTrackable::calculateScore() const {
  std::lock_guard<std::mutex> guard(mutex_);
  return calculateScore_nolock();
}

TrackableState DefaultTrackable::getState() const {
  std::lock_guard<std::mutex> guard(mutex_);
  return state_;
}

std::vector<std::shared_ptr<IDetection>> DefaultTrackable::getHistory() const {
  std::lock_guard<std::mutex> guard(mutex_);
  // create copy to return
  return std::vector<std::shared_ptr<IDetection>>(history_.begin(),
                                                  history_.end());
}

std::shared_ptr<IDetection> DefaultTrackable::getLatestDetection() const {
  std::lock_guard<std::mutex> guard(mutex_);
  if (history_.empty()) {
    return nullptr;
  }
  // find the last non-nullptr entry
  for (auto it = history_.rbegin(); it != history_.rend(); ++it) {
    if (*it) {
      return *it;
    }
  }
  // no valid detection found in history
  return nullptr;
}

int DefaultTrackable::getId() const {
  std::lock_guard<std::mutex> guard(mutex_);
  return id_;
}

int DefaultTrackable::getLabel() const {
  std::lock_guard<std::mutex> guard(mutex_);
  std::shared_ptr<IDetection> latest = nullptr;
  for (auto it = history_.rbegin(); it != history_.rend(); ++it) {
    if (*it) {
      latest = *it;
      break;
    }
  }

  if (latest) {
    return latest->getLabel();
  }
  return -1;
}

int DefaultTrackable::getMissingCount() const {
  std::lock_guard<std::mutex> guard(mutex_);
  return consecutiveMisses_;
}

} // namespace ai_pipe::tracker
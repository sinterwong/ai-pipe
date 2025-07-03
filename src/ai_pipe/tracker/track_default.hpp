#ifndef __CORE_TRACKER_DEFAULT_HPP_
#define __CORE_TRACKER_DEFAULT_HPP_

#include "track_core.hpp"
#include "track_types.hpp"
#include <deque>
#include <memory>
#include <mutex>
#include <vector>

namespace ai_pipe::tracker {
class DefaultTrackable : public ITrackable {
public:
  DefaultTrackable(std::shared_ptr<IDetection> initDet, int trackId,
                   const TrackingConfig &config);

  virtual ~DefaultTrackable() = default;

  virtual void update(std::shared_ptr<IDetection> detection) override;
  virtual void predict() override;
  virtual TrackableState getState() const override;
  virtual std::vector<std::shared_ptr<IDetection>> getHistory() const override;
  virtual std::shared_ptr<IDetection> getLatestDetection() const override;
  virtual int getId() const override;
  virtual int getLabel() const override;
  virtual float calculateScore() const override;
  virtual int getMissingCount() const override;

private:
  void updateState_nolock();
  float calculateScore_nolock() const;
  void addHistory_nolock(std::shared_ptr<IDetection> detection);

private:
  int id_;
  TrackableState state_;
  std::deque<std::shared_ptr<IDetection>> history_;
  int consecutiveMisses_ = 0;
  TrackingConfig config_;

  mutable std::mutex mutex_;
};
} // namespace ai_pipe::tracker
#endif
/**
 * @file dummy_module.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-08-19
 *
 * @copyright Copyright (c) 2025
 *
 */
namespace ai_pipe {
class DummyModule {
public:
  static DummyModule &getInstance() {
    static DummyModule instance;
    return instance;
  }

  DummyModule(const DummyModule &) = delete;
  DummyModule &operator=(const DummyModule &) = delete;
  DummyModule(DummyModule &&) = delete;
  DummyModule &operator=(DummyModule &&) = delete;

private:
  DummyModule();
};

[[maybe_unused]] inline const static DummyModule &__tempDummyModule_ =
    DummyModule::getInstance();
} // namespace ai_pipe
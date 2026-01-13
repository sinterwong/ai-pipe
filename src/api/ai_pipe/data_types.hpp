/**
 * @file data_types.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-01-24
 *
 * @copyright Copyright (c) 2025
 *
 */

#ifndef AI_PIPE_DATA_TYPES_HPP
#define AI_PIPE_DATA_TYPES_HPP

#include "ai_pipe/data_packet.hpp"
#include <map>
#include <memory>
#include <string>

namespace ai_pipe {

using PortData = common_utils::DataPacket;

using PortDataPtr = std::shared_ptr<PortData>;

using PortDataMap = std::map<std::string, PortDataPtr>;

} // namespace ai_pipe

#endif

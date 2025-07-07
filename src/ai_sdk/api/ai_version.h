/**
 * @file version.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2022-04-23
 *
 * @copyright Copyright (c) 2022
 *
 */

#pragma once

// for cmake
#define AI_WORKFLOW_VER_MAJOR 0
#define AI_WORKFLOW_VER_MINOR 2
#define AI_WORKFLOW_VER_PATCH 1

#define AI_WORKFLOW_VERSION                                                    \
  (AI_WORKFLOW_VER_MAJOR * 10000 + AI_WORKFLOW_VER_MINOR * 100 +               \
   AI_WORKFLOW_VER_PATCH)

// for source code
#define _AI_WORKFLOW_STR(s) #s
#define AI_WORKFLOW_PROJECT_VERSION(major, minor, patch)                       \
  "v" _AI_WORKFLOW_STR(major.minor.patch)

#define AI_WORKFLOW_VERSION_STR                                                \
  AI_WORKFLOW_PROJECT_VERSION(AI_WORKFLOW_VER_MAJOR, AI_WORKFLOW_VER_MINOR,    \
                              AI_WORKFLOW_VER_PATCH)

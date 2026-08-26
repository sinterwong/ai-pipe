#pragma once

// for cmake
#define AI_PIPE_VER_MAJOR 0
#define AI_PIPE_VER_MINOR 5
#define AI_PIPE_VER_PATCH 0

#define AI_PIPE_VERSION                                                        \
  (AI_PIPE_VER_MAJOR * 10000 + AI_PIPE_VER_MINOR * 100 + AI_PIPE_VER_PATCH)

// for source code
#define AI_PIPE_STR(s) #s
// Stringification: the arguments must stay bare tokens, parentheses
// would end up inside the produced literal.
#define AI_PIPE_PROJECT_VERSION(major, minor, patch)                           \
  "v" AI_PIPE_STR(major.minor.patch) // NOLINT(bugprone-macro-parentheses)

#define AI_PIPE_VERSION_STR                                                    \
  AI_PIPE_PROJECT_VERSION(AI_PIPE_VER_MAJOR, AI_PIPE_VER_MINOR,                \
                          AI_PIPE_VER_PATCH)

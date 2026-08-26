#pragma once

/** Library semantic-version major component. */
#define AI_PIPE_VER_MAJOR 0
/** Library semantic-version minor component. */
#define AI_PIPE_VER_MINOR 5
/** Library semantic-version patch component. */
#define AI_PIPE_VER_PATCH 0

/** Numeric version encoded as `major * 10000 + minor * 100 + patch`. */
#define AI_PIPE_VERSION                                                        \
  (AI_PIPE_VER_MAJOR * 10000 + AI_PIPE_VER_MINOR * 100 + AI_PIPE_VER_PATCH)

#define AI_PIPE_STR(s) #s
// Stringification: the arguments must stay bare tokens, parentheses
// would end up inside the produced literal.
#define AI_PIPE_PROJECT_VERSION(major, minor, patch)                           \
  "v" AI_PIPE_STR(major.minor.patch) // NOLINT(bugprone-macro-parentheses)

#define AI_PIPE_VERSION_STR                                                    \
  AI_PIPE_PROJECT_VERSION(AI_PIPE_VER_MAJOR, AI_PIPE_VER_MINOR,                \
                          AI_PIPE_VER_PATCH)

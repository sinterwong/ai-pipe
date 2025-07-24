/**
 * @file node_param_types.cpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-06-22
 *
 * @copyright Copyright (c) 2025
 *
 */

#include "node_param_types.hpp"

namespace ai_core {
void from_json(const nlohmann::json &j, FramePreprocessArg &p) {
  if (j.contains("preproc_task_type")) {
    p.preprocTaskType = static_cast<FramePreprocessArg::FramePreprocType>(
        j.at("preproc_task_type").get<int>());
  } else {
    throw std::runtime_error(
        "Missing 'preproc_task_type' in FramePreprocessArg JSON");
  }
  if (j.contains("model_input_shape")) {
    auto shape_arr = j.at("model_input_shape");
    p.modelInputShape.w = j.at("model_input_shape").at("w").get<int>();
    p.modelInputShape.h = j.at("model_input_shape").at("h").get<int>();
    p.modelInputShape.c = j.at("model_input_shape").at("c").get<int>();
  } else {
    throw std::runtime_error(
        "Missing 'model_input_shape' in FramePreprocessArg JSON");
  }
  if (j.contains("need_resize")) {
    j.at("need_resize").get_to(p.needResize);
  } else {
    throw std::runtime_error(
        "Missing 'need_resize' in FramePreprocessArg JSON");
  }
  if (j.contains("is_equal_scale")) {
    j.at("is_equal_scale").get_to(p.isEqualScale);
  } else {
    throw std::runtime_error(
        "Missing 'is_equal_scale' in FramePreprocessArg JSON");
  }
  if (j.contains("hwc2chw")) {
    j.at("hwc2chw").get_to(p.hwc2chw);
  } else {
    throw std::runtime_error("Missing 'hwc2chw' in FramePreprocessArg JSON");
  }
  if (j.contains("data_type")) {
    p.dataType = static_cast<DataType>(j.at("data_type").get<int>());
  } else {
    throw std::runtime_error("Missing 'data_type' in FramePreprocessArg JSON");
  }
  if (j.contains("buffer_location")) {
    p.outputLocation =
        static_cast<BufferLocation>(j.at("buffer_location").get<int>());
  } else {
    throw std::runtime_error(
        "Missing 'output_location' in FramePreprocessArg JSON");
  }
  if (j.contains("input_names")) {
    j.at("input_names").get_to(p.inputNames);
  } else {
    p.inputNames = {};
  }
  if (j.contains("pad")) {
    j.at("pad").get_to(p.pad);
  } else {
    p.pad = {};
  }
  if (j.contains("mean_vals")) {
    j.at("mean_vals").get_to(p.meanVals);
  } else {
    p.meanVals = {};
  }
  if (j.contains("norm_vals")) {
    j.at("norm_vals").get_to(p.normVals);
  } else {
    p.normVals = {};
  }
}

void from_json(const nlohmann::json &j, AlgoInferParams &p) {
  if (j.contains("name")) {
    j.at("name").get_to(p.name);
  } else {
    throw std::runtime_error("Missing 'name' in AlgoInferParams JSON");
  }
  if (j.contains("model_path")) {
    j.at("model_path").get_to(p.modelPath);
  } else {
    throw std::runtime_error("Missing 'model_path' in AlgoInferParams JSON");
  }
  if (j.contains("need_decrypt")) {
    j.at("need_decrypt").get_to(p.needDecrypt);
  } else {
    throw std::runtime_error("Missing 'need_decrypt' in AlgoInferParams JSON");
  }
  if (j.contains("device_type")) {
    p.deviceType = static_cast<DeviceType>(j.at("device_type").get<int>());
  } else {
    throw std::runtime_error("Missing 'device_type' in AlgoInferParams JSON");
  }
  if (j.contains("data_type")) {
    p.dataType = static_cast<DataType>(j.at("data_type").get<int>());
  } else {
    throw std::runtime_error("Missing 'data_type' in AlgoInferParams JSON");
  }
}

void from_json(const nlohmann::json &j, AnchorDetParams &p) {
  if (j.contains("cond_thre")) {
    j.at("cond_thre").get_to(p.condThre);
  } else {
    throw std::runtime_error("Missing 'cond_thre' in AnchorDetParams JSON");
  }
  if (j.contains("nms_thre")) {
    j.at("nms_thre").get_to(p.nmsThre);
  } else {
    throw std::runtime_error("Missing 'nms_thre' in AnchorDetParams JSON");
  }
  if (j.contains("det_alog_type")) {
    p.detAlogType = static_cast<AnchorDetParams::AnchorDetAlogType>(
        j.at("det_alog_type").get<int>());
  } else {
    throw std::runtime_error("Missing 'det_alog_type' in AnchorDetParams JSON");
  }
  if (j.contains("output_names")) {
    j.at("output_names").get_to(p.outputNames);
  } else {
    throw std::runtime_error("Missing 'output_names' in AnchorDetParams JSON");
  }
}

void from_json(const nlohmann::json &j, GenericPostParams &p) {
  if (j.contains("postproc_type")) {
    p.postprocType = static_cast<GenericPostParams::GenericAlgoType>(
        j.at("postproc_type").get<int>());
  } else {
    throw std::runtime_error(
        "Missing 'postproc_type' in GenericPostParams JSON");
  }
  if (j.contains("output_names")) {
    j.at("output_names").get_to(p.outputNames);
  } else {
    throw std::runtime_error(
        "Missing 'output_names' in GenericPostParams JSON");
  }
}

} // namespace ai_core

namespace ai_pipe {
void from_json(const nlohmann::json &j, ImageReaderNodeParams &p) {
  if (j.contains("color_type")) {
    std::string color_type_str = j.at("color_type").get<std::string>();
    if (color_type_str == "RGB888") {
      p.colorType = ColorType::RGB888;
    } else if (color_type_str == "BGR888") {
      p.colorType = ColorType::BGR888;
    } else if (color_type_str == "GRAY") {
      p.colorType = ColorType::GRAY;
    } else if (color_type_str == "YUV") {
      p.colorType = ColorType::YUV;
    } else if (color_type_str == "YUV_I420") {
      p.colorType = ColorType::YUV_I420;
    } else if (color_type_str == "YUV_YV12") {
      p.colorType = ColorType::YUV_YV12;
    } else {
      throw std::runtime_error("Unknown color_type: " + color_type_str);
    }
  } else {
    LOG_WARNINGS << "Missing 'color_type' in ImageReaderNodeParams JSON. "
                    "Defaulting to BGR888.";
    p.colorType = ColorType::BGR888;
  }
}

void from_json(const nlohmann::json &j, GenFrameNodeParams &p) {
  // GenFrameNodeParams currently has no parameters, so this function is empty.
  // If parameters are added in the future, they should be parsed here.
  (void)j; // Suppress unused variable warning
  (void)p; // Suppress unused variable warning
}

void from_json(const nlohmann::json &j, VisionInferenceNodeParams &p) {
  if (j.contains("model_name")) {
    j.at("model_name").get_to(p.modelName);
  } else {
    throw std::runtime_error(
        "Missing 'model_name' in VisionInferenceNodeParams JSON");
  }
}

void from_json(const nlohmann::json &j, ResultSaverNodeParams &p) {
  if (j.contains("output_dir")) {
    j.at("output_dir").get_to(p.outputDir);
  } else {
    throw std::runtime_error(
        "Missing 'output_dir' in ResultSaverNodeParams JSON");
  }
}

void from_json(const nlohmann::json &j, VisualizationNodeParams &p) {
  if (j.contains("output_dir")) {
    j.at("output_dir").get_to(p.outputDir);
  } else {
    throw std::runtime_error(
        "Missing 'output_dir' in VisualizationNodeParams JSON");
  }
}

void from_json(const nlohmann::json &j, PreprocessorNodeParams &p) {
  if (j.contains("module_name")) {
    j.at("module_name").get_to(p.moduleName);
  } else {
    throw std::runtime_error(
        "Missing 'module_name' in PreprocessorNodeParams JSON");
  }

  if (p.moduleName == "FramePreprocess") {
    if (j.contains("preproc_params")) {
      ai_core::FramePreprocessArg arg;
      j.at("preproc_params").get_to(arg);
      p.preprocParams.setParams<ai_core::FramePreprocessArg>(arg);
    } else {
      throw std::runtime_error("Unsupport moduleName " + p.moduleName +
                               " has no 'preproc_params' block.");
    }
  } else {
    throw std::runtime_error("Unknown preprocessor module name: " +
                             p.moduleName);
  }
}

void from_json(const nlohmann::json &j, InferEngineNodeParams &p) {
  if (j.contains("module_name")) {
    j.at("module_name").get_to(p.moduleName);
  } else {
    throw std::runtime_error(
        "Missing 'module_name' in InferEngineNodeParams JSON");
  }

  if (j.contains("infer_params")) {
    j.at("infer_params").get_to(p.inferParams);
  } else {
    throw std::runtime_error(
        "Missing 'infer_params' in InferEngineNodeParams JSON");
  }
}

void from_json(const nlohmann::json &j, PostprocessorNodeParams &p) {
  if (j.contains("module_name")) {
    j.at("module_name").get_to(p.moduleName);
  } else {
    throw std::runtime_error(
        "Missing 'module_name' in PostprocessorNodeParams JSON");
  }

  if (p.moduleName == "GenericPostproc") {
    if (j.contains("postproc_params")) {
      ai_core::GenericPostParams arg;
      j.at("postproc_params").get_to(arg);
      p.postprocParams.setParams<ai_core::GenericPostParams>(arg);
    } else {
      throw std::runtime_error("Unsupport moduleName " + p.moduleName +
                               " has no 'postproc_params' block.");
    }
  } else if (p.moduleName == "AnchorDetPostproc") {
    if (j.contains("postproc_params")) {
      ai_core::AnchorDetParams arg;
      j.at("postproc_params").get_to(arg);
      p.postprocParams.setParams<ai_core::AnchorDetParams>(arg);
    } else {
      throw std::runtime_error("Unsupport moduleName " + p.moduleName +
                               " has no 'postproc_params' block.");
    }
  } else {
    throw std::runtime_error("Unknown postprocessor module name: " +
                             p.moduleName);
  }
}
} // namespace ai_pipe

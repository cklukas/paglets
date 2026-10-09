// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Contract of the `ai` system paglet (planning/cpp-web-ai.md): local AI
// inference on hosts with a backend (Ollama; Apple Foundation Models on
// macOS 27 later). Content given to `ai` never leaves the host for
// inference. Long requests are answered when done (the reply is deferred);
// the mesh policy decides per operation and model (rules and grants for
// the service `ai` with the model as the root), and every owner has a
// request quota per hour.
//
// `model` empty: the host's default model.

#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace paglets::services::ai {

struct CapabilitiesRequest {};

struct Model {
    std::string name;
    std::int64_t context = 0;  // tokens; 0: unknown
    bool vision = false;
};

struct Capabilities {
    std::string backend;  // "ollama", "test"
    bool available = false;
    std::vector<Model> models;
    std::vector<std::string> tasks;  // the operations it serves
    std::int64_t max_input_bytes = 0;
    std::int64_t queue = 0;  // requests waiting or running
};

struct TextReply {
    std::string text;
    std::string model;
    std::int64_t elapsed_ms = 0;
};

struct SummarizeRequest {
    std::string text;
    std::int64_t max_words = 150;
    std::string model;
};

struct ClassifyRequest {
    std::string text;
    std::vector<std::string> labels;
    std::string model;
};

struct ClassifyReply {
    std::string label;  // one of the labels
    std::string model;
};

struct Field {
    std::string name;
    std::string description;
};

struct ExtractRequest {
    std::string text;
    std::vector<Field> fields;
    std::string model;
};

struct FieldValue {
    std::string name;
    std::string value;  // empty: not found
};

struct ExtractReply {
    std::vector<FieldValue> fields;
    std::string model;
};

struct GenerateRequest {
    std::string prompt;
    std::string system;
    std::string model;
};

struct EmbedRequest {
    std::vector<std::string> texts;
    std::string model;
};

struct Vector {
    std::vector<double> values;
};

struct EmbedReply {
    std::vector<Vector> vectors;
    std::string model;
};

struct DescribeImageRequest {
    std::vector<std::uint8_t> image;  // PNG or JPEG
    std::string prompt;               // empty: a short description
    std::string model;
};

struct Contract {
    static constexpr std::string_view service = "ai";
    Capabilities capabilities(const CapabilitiesRequest&);
    TextReply summarize(const SummarizeRequest&);
    ClassifyReply classify(const ClassifyRequest&);
    ExtractReply extract(const ExtractRequest&);
    TextReply generate(const GenerateRequest&);
    EmbedReply embed(const EmbedRequest&);
    TextReply describe_image(const DescribeImageRequest&);
};

}  // namespace paglets::services::ai

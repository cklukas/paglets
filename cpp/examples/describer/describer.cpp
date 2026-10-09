// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Image Describer (planning/cpp-demo-paglets.md, demo 18): makes image
// collections searchable by content. The paglet visits the hosts with the
// images and reads them there (the mesh policy grants the directories),
// finds a host whose `ai` offer can see (the offer requirement vision=yes
// with the operation describe_image, through mesh-info), moves there for
// a caption and tags per image, and writes a JSON catalogue on the output
// host. Images read from a root that must stay on its host keep the paglet
// there (data residency).

#include "describer.schema.gen.hpp"

#include <paglets/patterns/files.hpp>
#include <paglets/patterns/services.hpp>
#include <paglets/services/ai.gen.hpp>

#include <algorithm>
#include <cctype>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace {

using namespace describer_msgs;
namespace svc = paglets::services;
namespace abi = paglets::abi;
namespace pt = paglets::patterns;
using paglets::Capability;
using paglets::Endpoint;
using paglets::Message;

bool is_image(const std::string& path) {
    std::string p = path;
    std::ranges::transform(p, p.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return p.ends_with(".png") || p.ends_with(".jpg") || p.ends_with(".jpeg");
}

std::string json_string(const std::string& s) {
    std::string out = "\"";
    for (unsigned char c : s) {
        switch (c) {
            case '"': out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default:
                if (c < 0x20) {
                    static constexpr char hex[] = "0123456789abcdef";
                    out += "\\u00";
                    out += hex[c >> 4];
                    out += hex[c & 15];
                } else {
                    out += static_cast<char>(c);
                }
        }
    }
    return out + "\"";
}

std::vector<std::string> tags_of(const std::string& text) {
    std::vector<std::string> tags;
    std::size_t pos = 0;
    while (pos <= text.size() && tags.size() < 8) {
        auto comma = text.find(',', pos);
        if (comma == std::string::npos) comma = text.size();
        std::string t = text.substr(pos, comma - pos);
        while (!t.empty() && std::isspace(static_cast<unsigned char>(t.front()))) t.erase(t.begin());
        while (!t.empty() && (std::isspace(static_cast<unsigned char>(t.back())) || t.back() == '.')) t.pop_back();
        if (!t.empty()) tags.push_back(std::move(t));
        pos = comma + 1;
    }
    return tags;
}

class Describer : public paglets::Paglet {
public:
    Describer() {
        router().on<Start>("start", [this](const Start& s, Message& m) {
            if (status_.state != "idle") {
                (void)m.reply(Started{false, "already started"});
                return;
            }
            if (s.sources.empty() || s.output.host.empty()) {
                (void)m.reply(Started{false, "no sources or no output"});
                return;
            }
            params_ = s;
            status_.state = "collecting";
            (void)m.reply(Started{true, {}});
            where([this] { next_source(); });
        });
        router().on("status", [this](Message& m) { (void)m.reply(status_); });
    }

    void on_arrived(const abi::ArrivedEvent&) override {
        where([this] {
            if (status_.state == "collecting") return collect();
            if (status_.state == "describing") return describe(0);
            if (status_.state == "writing") return write();
        });
    }

    void on_move_failed(const abi::MoveFailed& f) override {
        // Refused by data residency, or no host to go to: it stays here.
        status_.state = f.reason.find("data residency") != std::string::npos ? "refused" : "failed";
        status_.error = f.reason;
        paglets::log("describer: " + f.reason);
    }

private:
    // Learns which host this is, then goes on.
    void where(std::function<void()> next) {
        pt::here([this, next](paglets::Result<svc::mesh_info::Snapshot> s) {
            if (s) {
                here_ = s->host;
                here_name_ = s->host_name;
                status_.hosts.push_back(s->host_name.empty() ? s->host.substr(0, 8) : s->host_name);
            }
            next();
        });
    }

    bool is_here(const std::string& host) const {
        return host == here_name_ || host == here_ || (host.size() >= 8 && here_.starts_with(host));
    }

    void go(const std::string& destination) {
        if (auto moved = paglets::dispatch(destination); !moved) fail(pt::error_text("dispatch to " + destination, moved.error()));
    }

    void fail(std::string why) {
        status_.state = "failed";
        status_.error = std::move(why);
        paglets::log("describer: " + status_.error);
    }

    // -- collecting -------------------------------------------------------------------------

    void next_source() {
        if (source_ >= params_.sources.size()) return to_ai_host();
        const auto& s = params_.sources[source_];
        if (is_here(s.host)) return collect();
        go(s.host);
    }

    void collect() {
        const Source s = params_.sources[source_];
        pt::granted_dir(s.root, "", {"read"}, "image describer", [this, s](paglets::Result<Capability> d) {
            if (!d) return fail(pt::error_text("files of " + s.root, d.error()));
            dir_ = std::make_shared<Capability>(*d);
            pt::endpoint("files", [this, s](paglets::Result<Endpoint> files) {
                if (!files) return fail(pt::error_text("files", files.error()));
                svc::files::Client client{*files};
                svc::files::FindRequest q;
                q.pattern = s.pattern;
                q.limit = 10'000;
                auto sent = client.find(
                    q,
                    [this](paglets::Result<svc::files::FindReply> r, Message&) {
                        if (!r) return fail(pt::error_text("find", r.error()));
                        found_.clear();
                        for (const auto& e : r->entries) {
                            if (e.type == svc::files::EntryType::file && is_image(e.path)) found_.push_back(e);
                        }
                        read_next(0);
                    },
                    pt::lending(*dir_));
                if (!sent) fail(pt::error_text("find", sent.error()));
            });
        });
    }

    void read_next(std::size_t i) {
        const auto root = params_.sources[source_].root;
        while (i < found_.size() &&
               (found_[i].size > static_cast<std::uint64_t>(params_.max_image_bytes) ||
                total_ + static_cast<std::int64_t>(found_[i].size) > params_.max_total_bytes)) {
            status_.skipped.push_back(here_name_ + ": " + root + "/" + found_[i].path + " (too large)");
            ++i;
        }
        if (i >= found_.size() || static_cast<std::int64_t>(pixels_.size()) >= params_.max_images) {
            (void)dir_->drop();
            dir_.reset();
            ++source_;
            return next_source();
        }
        pt::read_file(*dir_, found_[i].path, static_cast<std::uint64_t>(params_.max_image_bytes),
                      [this, i, root](paglets::Result<pt::CarriedFile> f) {
                          if (!f) {
                              status_.skipped.push_back(here_name_ + ": " + root + "/" + found_[i].path + " (unreadable)");
                              return read_next(i + 1);
                          }
                          Image img;
                          img.host = here_name_;
                          img.path = root + "/" + f->name;
                          img.size = static_cast<std::int64_t>(f->data.size());
                          total_ += img.size;
                          status_.images.push_back(std::move(img));
                          pixels_.push_back(std::move(f->data));
                          read_next(i + 1);
                      });
    }

    // -- describing -------------------------------------------------------------------------

    // A host whose `ai` offer can see: the offer requirement vision=yes.
    void to_ai_host() {
        if (pixels_.empty()) {
            status_.state = "writing";
            if (is_here(params_.output.host)) return write();
            return go(params_.output.host);
        }
        auto mi = paglets::service("mesh-info");
        if (!mi) return fail("no mesh-info");
        svc::mesh_info::Client client{*mi};
        svc::mesh_info::OffersRequest q;
        q.service = "ai";
        q.op = "describe_image";
        q.require.push_back(svc::mesh_info::Requirement{"vision", "=", "yes", 0});
        q.limit = 1;
        auto sent = client.find_offers(q, [this](paglets::Result<svc::mesh_info::Offers> o, Message&) {
            if (!o) return fail(pt::error_text("find_offers", o.error()));
            if (o->matches.empty()) return fail("no host offers ai.describe_image with vision");
            const auto& m = o->matches.front();
            status_.ai_host = m.host_name;
            status_.state = "describing";
            if (is_here(m.host)) return describe(0);
            go(m.host);
        });
        if (!sent) fail(pt::error_text("find_offers", sent.error()));
    }

    void describe(std::size_t i) {
        if (i >= pixels_.size()) {
            pixels_.clear();  // described: only the text travels on
            status_.state = "writing";
            if (is_here(params_.output.host)) return write();
            return go(params_.output.host);
        }
        ask(pixels_[i], "Describe this image in one sentence.", [this, i](paglets::Result<svc::ai::TextReply> caption) {
            if (!caption) return fail(pt::error_text("describe_image", caption.error()));
            status_.images[i].caption = caption->text;
            status_.model = caption->model;
            ask(pixels_[i], "Give up to eight comma-separated tags for this image.",
                [this, i](paglets::Result<svc::ai::TextReply> tags) {
                    if (!tags) return fail(pt::error_text("describe_image", tags.error()));
                    status_.images[i].tags = tags_of(tags->text);
                    describe(i + 1);
                });
        });
    }

    void ask(const std::vector<std::uint8_t>& image, std::string prompt,
             std::function<void(paglets::Result<svc::ai::TextReply>)> next) {
        pt::lookup("ai", [this, &image, prompt = std::move(prompt), next](paglets::Result<Endpoint> ai) {
            if (!ai) return next(std::unexpected(ai.error()));
            svc::ai::Client client{*ai};
            auto sent = client.describe_image(svc::ai::DescribeImageRequest{image, prompt, params_.model},
                                              [next](paglets::Result<svc::ai::TextReply> r, Message&) { next(std::move(r)); });
            if (!sent) next(std::unexpected(sent.error()));
        });
    }

    // -- writing ------------------------------------------------------------------------------

    std::string catalogue() const {
        std::string out = "{\n  \"model\": " + json_string(status_.model) + ",\n  \"images\": [";
        for (std::size_t i = 0; i < status_.images.size(); ++i) {
            const auto& img = status_.images[i];
            out += i == 0 ? "\n" : ",\n";
            out += "    {\"host\": " + json_string(img.host) + ", \"path\": " + json_string(img.path) +
                   ", \"size\": " + std::to_string(img.size) + ", \"caption\": " + json_string(img.caption) +
                   ", \"tags\": [";
            for (std::size_t t = 0; t < img.tags.size(); ++t) out += (t == 0 ? "" : ", ") + json_string(img.tags[t]);
            out += "]}";
        }
        return out + "\n  ]\n}\n";
    }

    void write() {
        const Output o = params_.output;
        pt::granted_dir(o.root, "", {"write", "create"}, "image describer", [this, o](paglets::Result<Capability> d) {
            if (!d) return fail(pt::error_text("files of " + o.root, d.error()));
            auto dir = std::make_shared<Capability>(*d);
            const std::string text = catalogue();
            pt::write_file(*dir, o.path, paglets::Bytes(text.begin(), text.end()), [this, dir](paglets::Result<void> w) {
                (void)dir->drop();
                if (!w) return fail(pt::error_text("write", w.error()));
                status_.state = "written";
            });
        });
    }

    Start params_;
    Status status_{"idle"};
    std::string here_;
    std::string here_name_;
    std::size_t source_ = 0;
    std::int64_t total_ = 0;
    std::vector<svc::files::Entry> found_;
    std::vector<std::vector<std::uint8_t>> pixels_;  // the images read, in the order of status_.images
    std::shared_ptr<Capability> dir_;
};

}  // namespace

PAGLETS_PAGLET(Describer)

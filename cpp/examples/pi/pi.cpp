// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Pi compute example (planning/cpp-compute-jobs.md, section 4): hex digits
// of pi in chunks, computed as compute jobs on the hosts of the mesh.
//
// The paglet stays where it was started and never computes itself. Each
// chunk is a job (`digits`, a function of this module) that runs as long as
// it needs on a host with free slots (`hosts: any`); its result comes back
// as a message. The paglet puts the chunks together in order; a chunk whose
// job failed (its host went away) is submitted again.
//
// The paglet takes the chunks in order as they come in: it writes them to a
// file below a named root of its host (when asked to) and keeps only chunks
// that came early, so many digits need little memory. At the end one long
// job (`to_decimal`) converts all hex digits into decimal digits: it reads
// them from the file (an artifact), and its output is the decimal file.
// Every pass multiplies the fraction by 10^9 and takes the nine digits that
// move in front of the point; that needs every hex digit, so it cannot run
// per chunk.

#include "pi.schema.gen.hpp"

#include "bbp.hpp"

#include <paglets/compute.hpp>
#include <paglets/paglet.hpp>
#include <paglets/patterns/files.hpp>
#include <paglets/services/mesh_info.gen.hpp>

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

namespace {

using namespace pi_msgs;
namespace svc = paglets::services;
namespace abi = paglets::abi;
namespace compute = paglets::compute;
using paglets::Message;
namespace pt = paglets::patterns;

constexpr std::size_t shown_digits = 4096;          // in `status`
constexpr std::int64_t max_inline_hex = 800'000;    // a conversion without files: input and output are messages
constexpr std::int64_t max_decimal_hex = 400'000'000;  // the fraction must fit the conversion job's memory
constexpr std::int64_t max_attempts = 5;

std::int64_t now_ms() {
    return paglets::now_ns(abi::Clock::monotonic) / 1'000'000;
}

// -- the jobs ------------------------------------------------------------------------------

// --8<-- [start:digits_job]
// A chunk of hex digits: as long as it takes, no slicing. The paglet's
// handlers run meanwhile; this is a separate instance of the module.
void digits(compute::JobContext& job) {
    Task t;
    if (!job.input(t) || t.count <= 0) return job.fail(abi::malformed);
    const std::int64_t began = now_ms();
    std::string out;
    out.reserve(static_cast<std::size_t>(t.count));
    while (static_cast<std::int64_t>(out.size()) < t.count) {
        const std::int64_t at = t.start + static_cast<std::int64_t>(out.size());
        pi_bbp::Block block(at, static_cast<int>(std::min<std::int64_t>(6, t.count - static_cast<std::int64_t>(out.size()))));
        while (!block.step(20'000)) {
            if (job.cancelled()) return job.fail(abi::cancelled);
        }
        out += block.digits();
        job.progress(static_cast<std::int64_t>(out.size()), t.count);
    }
    // Demonstrations and tests: a chunk takes at least work_ms.
    while (now_ms() - began < t.work_ms) {
        if (job.cancelled()) return job.fail(abi::cancelled);
    }
    (void)job.output(out);
}
PAGLETS_JOB(digits)
// --8<-- [end:digits_job]

// --8<-- [start:convert_job]
// Hex digits ("3." and the digits after the point) to decimal digits, as
// text of the same form. The fraction, 8 hex digits per 32-bit limb, the
// most significant first; every pass multiplies it by 10^9, and the
// overflow above the point is the next nine decimal digits.
void to_decimal(compute::JobContext& job) {
    std::vector<std::uint32_t> limbs;
    limbs.reserve(static_cast<std::size_t>(job.input_size() / 8 + 1));
    std::uint32_t partial = 0;
    int partial_n = 0;
    std::int64_t hex = 0;
    std::vector<std::uint8_t> buf(abi::job_chunk);
    for (std::int64_t at = 0; at < job.input_size();) {
        auto n = job.read(at, buf);
        if (!n || *n == 0) return job.fail(n ? abi::malformed : n.error());
        for (std::size_t i = 0; i < *n; ++i) {
            const char c = static_cast<char>(buf[i]);
            if (at + static_cast<std::int64_t>(i) < 2) continue;  // "3."
            const std::uint32_t v = c <= '9' ? static_cast<std::uint32_t>(c - '0') : static_cast<std::uint32_t>(c - 'A' + 10);
            partial = (partial << 4) | v;
            ++hex;
            if (++partial_n == 8) {
                limbs.push_back(partial);
                partial = 0;
                partial_n = 0;
            }
        }
        at += static_cast<std::int64_t>(*n);
    }
    if (partial_n > 0) limbs.push_back(partial << (4 * (8 - partial_n)));
    // n hex digits fix n * log10(16) decimal digits; two fewer, so that a
    // run of nines or zeros at the end cannot hide a carry.
    const std::int64_t total = std::max<std::int64_t>(0, static_cast<std::int64_t>(static_cast<double>(hex) * std::log10(16.0)) - 2);
    std::string out = "3.";
    std::int64_t done = 0;
    while (done < total) {
        if ((done & 0x3ff) == 0) {
            if (job.cancelled()) return job.fail(abi::cancelled);
            job.progress(done, total);
        }
        // Only the precision the remaining digits need (and two limbs more).
        const std::int64_t remaining = total - done;
        const auto need = static_cast<std::size_t>(static_cast<double>(remaining) * 3.3219280949 / 32.0) + 2;
        if (limbs.size() > need) limbs.resize(need);
        std::uint64_t carry = 0;
        for (std::size_t i = limbs.size(); i-- > 0;) {
            const std::uint64_t t = static_cast<std::uint64_t>(limbs[i]) * 1'000'000'000ULL + carry;
            limbs[i] = static_cast<std::uint32_t>(t);
            carry = t >> 32;
        }
        char nine[16];
        std::snprintf(nine, sizeof nine, "%09u", static_cast<unsigned>(carry));
        const auto take = static_cast<std::size_t>(std::min<std::int64_t>(9, remaining));
        out.append(nine, take);
        done += static_cast<std::int64_t>(take);
        if (out.size() >= abi::job_chunk) {
            if (auto w = job.write(std::span(reinterpret_cast<const std::uint8_t*>(out.data()), out.size())); !w) {
                return job.fail(w.error());
            }
            out.clear();
        }
    }
    if (auto w = job.write(std::span(reinterpret_cast<const std::uint8_t*>(out.data()), out.size())); !w) {
        job.fail(w.error());
    }
}
PAGLETS_JOB(to_decimal)
// --8<-- [end:convert_job]

// -- the paglet ------------------------------------------------------------------------------

class Pi : public paglets::Paglet {
public:
    Pi() {
        router().on<Start>("start", [this](const Start& s, Message& m) { m.reply(start(s)); });
        router().on("status", [this](Message& m) { m.reply(status()); });
        router().on("tick", [this](Message&) { tick(); });
    }

private:
    struct Chunk {
        std::int64_t start = 0;
        std::int64_t count = 0;
        std::string digits;
        bool done = false;
        bool sent = false;
        std::int64_t attempt = 0;
    };

    Started start(const Start& s) {
        if (started_) return Started{false, "already started"};
        if (s.digits <= 0 || s.chunk <= 0 || s.max_in_flight <= 0) return Started{false, "invalid parameters"};
        if (s.decimal && s.output_root.empty() && s.digits > max_inline_hex) {
            return Started{false, "without files the decimal conversion takes at most " +
                                      std::to_string(max_inline_hex) + " hex digits (set output_root)"};
        }
        if (s.decimal && s.digits > max_decimal_hex) {
            return Started{false, "the decimal conversion holds every digit in memory: at most " +
                                      std::to_string(max_decimal_hex) + " hex digits"};
        }
        started_ = true;
        params_ = s;
        state_ = "computing";
        for (std::int64_t at = 0; at < s.digits; at += s.chunk) {
            chunks_.push_back(Chunk{at, std::min(s.chunk, s.digits - at)});
        }
        if (s.output_root.empty()) {
            if (s.decimal) all_hex_ = "3.";
            begin();
            return Started{true, {}};
        }
        // The files: a directory of the root, granted on this host.
        hex_file_.path = s.output_path + ".hex";
        if (s.decimal) dec_path_ = s.output_path + ".txt";
        hex_file_.pending = "3.";
        pt::granted_dir(s.output_root, "", {"read", "write", "create"}, "pi: digits",
                        [this](paglets::Result<paglets::Capability> d) {
                            if (!d) return fail(pt::error_text("files of " + params_.output_root, d.error()));
                            dir_ = std::make_shared<paglets::Capability>(*d);
                            const auto slash = params_.output_path.rfind('/');
                            pt::make_dirs(*dir_,
                                          slash == std::string::npos ? std::string() : params_.output_path.substr(0, slash),
                                          [this](paglets::Result<void> made) {
                                              if (!made) {
                                                  return fail(pt::error_text("directory for " + params_.output_path,
                                                                             made.error()));
                                              }
                                              begin();
                                          });
                        });
        return Started{true, {}};
    }

    void begin() {
        (void)paglets::after_raw(500, "tick", {});
        // The names of the hosts (jobs report key IDs), then the first chunks.
        auto info = paglets::service("mesh-info");
        if (!info) return send_more();
        svc::mesh_info::Client client{*info};
        auto sent = client.landscape({}, [this](paglets::Result<svc::mesh_info::Landscape> l, Message&) {
            if (l) {
                for (const auto& h : l->hosts) names_[h.host] = h.host_name.empty() ? h.host.substr(0, 8) : h.host_name;
            }
            send_more();
        });
        if (!sent) send_more();
    }

    void fail(std::string why) {
        state_ = "failed";
        error_ = std::move(why);
        paglets::log("pi: " + error_);
    }

    std::int64_t in_flight() const {
        return std::ranges::count_if(chunks_, [](const Chunk& c) { return c.sent && !c.done; });
    }

    // --8<-- [start:submit]
    // Chunks go out as jobs while fewer than max_in_flight run. The host
    // picks where (`hosts: any`: the one with the most free slots); on a
    // host without a mesh they run here.
    void send_more() {
        if (state_ != "computing") return;
        for (auto& c : chunks_) {
            if (in_flight() >= params_.max_in_flight) break;
            if (c.sent || c.done) continue;
            if (++c.attempt > max_attempts) return fail("chunk " + std::to_string(c.start) + " failed every time");
            compute::Options o;
            o.hosts = local_only_ ? std::vector<std::string>{} : params_.hosts;
            o.max_ms = params_.chunk_timeout_ms;
            const bool elsewhere = !o.hosts.empty();
            auto job = compute::submit("digits", Task{c.start, c.count, params_.work_ms},
                                       [this, start = c.start](paglets::Result<compute::Output> r, Message& m) {
                                           on_result(start, std::move(r), m);
                                       },
                                       std::move(o));
            if (!job && job.error() == abi::unsupported && elsewhere) {
                local_only_ = true;  // no mesh: this host only
                --c.attempt;
                return send_more();
            }
            if (!job) return fail("submitting a job: " + std::string(abi::error_name(job.error())));
            c.sent = true;
        }
    }
    // --8<-- [end:submit]

    // --8<-- [start:result]
    void on_result(std::int64_t start, paglets::Result<compute::Output> r, Message& m) {
        auto it = std::ranges::find_if(chunks_, [&](const Chunk& c) { return c.start == start; });
        if (it == chunks_.end() || it->done) return;
        Chunk& c = *it;
        std::string digits;
        if (!r || !r->decode(digits) || digits.size() != static_cast<std::size_t>(c.count)) {
            // Its host went away, or it ran too long: again, elsewhere.
            const auto p = m.payload();
            paglets::log("pi: chunk " + std::to_string(c.start) + " again: " +
                         (r ? std::string("malformed output") : std::string(abi::error_name(r.error())) + " " +
                                                                    std::string(p.begin(), p.end())));
            c.sent = false;
            ++resent_;
            (void)paglets::after_raw(200, "tick", {});
            return;
        }
        c.done = true;
        c.digits = std::move(digits);
        auto name = names_.find(r->info.host);
        hosts_.insert(name != names_.end() ? name->second : r->info.host.substr(0, 8));
        if (r->info.aot) ++chunks_aot_;
        compute_ms_ += r->info.elapsed_ms;
        take_in_order();
        send_more();
    }
    // --8<-- [end:result]

    // --8<-- [start:in_order]
    // Chunks that are next in order leave the paglet's memory: into the file
    // (or, without files, into the digits kept for the conversion).
    void take_in_order() {
        while (next_ < chunks_.size() && chunks_[next_].done) {
            Chunk& c = chunks_[next_++];
            done_ += c.count;
            if (shown_hex_.size() < shown_digits) shown_hex_ += c.digits.substr(0, shown_digits - shown_hex_.size());
            if (dir_) {
                hex_file_.pending += c.digits;
                flush(hex_file_);
            } else if (params_.decimal) {
                all_hex_ += c.digits;
            }
            std::string().swap(c.digits);
        }
        if (finished() && state_ == "computing") {
            state_ = "writing";
            maybe_converted();
        }
    }
    // --8<-- [end:in_order]

    // -- the decimal conversion --------------------------------------------------------------

    // --8<-- [start:convert]
    // Every hex digit is there (and in the file): one job converts them all,
    // here. Its input is the hex file (an artifact of this host) or, without
    // files, the digits themselves; its output is the decimal text.
    void start_conversion() {
        state_ = "converting";
        decimal_total_ = std::max<std::int64_t>(0, static_cast<std::int64_t>(static_cast<double>(done_) * std::log10(16.0)) - 2);
        compute::Options o;
        o.max_ms = 24LL * 3'600'000;                     // capped by the host
        o.memory_mb = done_ / 2 / (1024 * 1024) + 32;    // the fraction, half a byte per digit
        auto converted = [this](paglets::Result<compute::Output> r, Message& m) {
            if (!r) {
                const auto p = m.payload();
                return fail("conversion: " + std::string(abi::error_name(r.error())) + " " + std::string(p.begin(), p.end()));
            }
            convert_ms_ += r->info.elapsed_ms;
            if (r->artifact) return save_decimal(std::move(*r->artifact));
            std::string text(r->data.begin(), r->data.end());
            decimal_done_ = std::max<std::int64_t>(0, static_cast<std::int64_t>(text.size()) - 2);
            shown_dec_ = text.size() > 2 ? text.substr(2, shown_digits) : std::string();
            converted_ = true;
            if (dir_) {
                dec_file_.path = dec_path_;
                dec_file_.pending = std::move(text);
                flush(dec_file_);
            }
            maybe_done();
        };
        if (!dir_) {
            auto job = compute::submit_raw("to_decimal", paglets::Bytes(all_hex_.begin(), all_hex_.end()), converted,
                                           std::move(o));
            std::string().swap(all_hex_);
            if (!job) fail("conversion: " + std::string(abi::error_name(job.error())));
            return;
        }
        pt::share_file(*dir_, hex_file_.path, [this, o, converted](paglets::Result<pt::SharedFile> f) mutable {
            if (!f) return fail(pt::error_text("share " + hex_file_.path, f.error()));
            o.input_artifact = f->content;
            auto job = compute::submit_raw("to_decimal", {}, converted, std::move(o));
            (void)f->content.drop();
            if (!job) fail("conversion: " + std::string(abi::error_name(job.error())));
        });
    }

    // A large decimal text came as an artifact: into the file, and its
    // beginning into the status.
    void save_decimal(paglets::Capability content) {
        pt::save_file(*dir_, dec_path_, content, [this, content](paglets::Result<svc::files::Entry> e) mutable {
            (void)content.drop();
            if (!e) return fail(pt::error_text("save " + dec_path_, e.error()));
            decimal_done_ = static_cast<std::int64_t>(e->size) - 2;
            dec_file_.path = dec_path_;
            converted_ = true;
            pt::endpoint("files", [this](paglets::Result<paglets::Endpoint> files) {
                if (!files) return maybe_done();
                svc::files::Client client{*files};
                auto sent = client.read(
                    svc::files::ReadRequest{dec_path_, 2, shown_digits},
                    [this](paglets::Result<svc::files::ReadReply> r, Message&) {
                        if (r) shown_dec_.assign(r->data.begin(), r->data.end());
                        maybe_done();
                    },
                    pt::lending(*dir_));
                if (!sent) maybe_done();
            });
        });
    }
    // --8<-- [end:convert]

    // -- the files -----------------------------------------------------------------------------

    struct Sink {
        std::string path;
        std::string pending;  // not written yet
        bool busy = false;
        bool written = false;  // the first write replaces the file, later ones append
    };

    void flush(Sink& sink) {
        if (!dir_ || sink.busy || sink.pending.empty() || state_ == "failed") return;
        constexpr std::size_t block = 1024 * 1024;
        std::string part = sink.pending.substr(0, block);
        sink.pending.erase(0, part.size());
        sink.busy = true;
        Sink* target = &sink;
        pt::endpoint("files", [this, target, part = std::move(part)](paglets::Result<paglets::Endpoint> files) {
            if (!files) return fail(pt::error_text("files", files.error()));
            svc::files::Client client{*files};
            svc::files::WriteRequest q;
            q.path = target->path;
            q.data.assign(part.begin(), part.end());
            q.mode = target->written ? svc::files::WriteMode::append : svc::files::WriteMode::replace;
            auto sent = client.write(
                q,
                [this, target](paglets::Result<svc::files::Entry> w, Message&) {
                    target->busy = false;
                    if (!w) return fail(pt::error_text("write " + target->path, w.error()));
                    target->written = true;
                    flush(*target);
                    maybe_converted();
                    maybe_done();
                },
                pt::lending(*dir_));
            if (!sent) fail(pt::error_text("write " + target->path, sent.error()));
        });
    }

    bool written(const Sink& s) const { return !s.busy && s.pending.empty(); }

    // The hex digits are complete (and written): convert, or finish.
    void maybe_converted() {
        if (state_ != "writing" || !written(hex_file_)) return;
        if (params_.decimal) return start_conversion();
        maybe_done();
    }

    // Done when every file is written and, with `decimal`, the conversion
    // is there.
    void maybe_done() {
        if (state_ != "writing" && state_ != "converting") return;
        if (params_.decimal && !converted_) return;
        if (!written(hex_file_) || !written(dec_file_)) return;
        state_ = "done";
        if (dir_) (void)dir_->drop();
        dir_.reset();
    }

    // --8<-- [start:resend]
    // Chunks whose job failed go out again (a little later).
    void tick() {
        send_more();
        if (!finished() && state_ == "computing") (void)paglets::after_raw(500, "tick", {});
    }
    // --8<-- [end:resend]

    bool finished() const {
        return started_ && std::ranges::all_of(chunks_, [](const Chunk& c) { return c.done; });
    }

    Status status() const {
        Status s;
        s.state = state_;
        s.digits = params_.digits;
        s.done = done_;
        s.chunks = static_cast<std::int64_t>(chunks_.size());
        s.chunks_done = std::ranges::count_if(chunks_, [](const Chunk& c) { return c.done; });
        s.in_flight = in_flight();
        s.resent = resent_;
        s.finished = finished();
        s.hex = "3." + shown_hex_;
        s.decimal_digits = decimal_total_;
        s.decimal_done = decimal_done_;
        s.decimal = "3." + shown_dec_;
        s.shown = static_cast<std::int64_t>(shown_digits);
        if (!hex_file_.path.empty()) s.files.push_back(hex_file_.path);
        if (!dec_path_.empty()) s.files.push_back(dec_path_);
        s.hosts.assign(hosts_.begin(), hosts_.end());
        s.chunks_aot = chunks_aot_;
        s.compute_ms = compute_ms_;
        s.convert_ms = convert_ms_;
        s.error = error_;
        return s;
    }

    bool started_ = false;
    std::string state_ = "idle";
    std::string error_;
    Start params_;
    std::vector<Chunk> chunks_;
    std::size_t next_ = 0;   // the first chunk not yet taken in order
    std::int64_t done_ = 0;  // hex digits taken in order
    std::string shown_hex_;  // the first of them
    std::string all_hex_;    // without files: "3." and every digit, for the conversion
    std::int64_t decimal_total_ = 0;
    std::int64_t decimal_done_ = 0;
    std::string shown_dec_;
    std::shared_ptr<paglets::Capability> dir_;
    Sink hex_file_;
    Sink dec_file_;
    std::string dec_path_;
    std::map<std::string, std::string> names_;  // host key ID -> name
    std::set<std::string> hosts_;
    std::int64_t resent_ = 0;
    std::int64_t chunks_aot_ = 0;
    std::int64_t compute_ms_ = 0;
    std::int64_t convert_ms_ = 0;
    bool converted_ = false;
    bool local_only_ = false;
};

}  // namespace

PAGLETS_PAGLET(Pi)

// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

#include <paglets/wasm/snapshot.hpp>

#include <algorithm>
#include <cstring>

namespace paglets::wasm {

namespace {

constexpr char magic[8] = {'P', 'G', 'I', 'M', 'G', '0', '0', '1'};

bool is_zero_page(std::span<const std::uint8_t> page) {
    return std::ranges::all_of(page, [](std::uint8_t b) { return b == 0; });
}

class ByteWriter {
public:
    void raw(const void* p, std::size_t n) {
        const auto* b = static_cast<const std::uint8_t*>(p);
        out.insert(out.end(), b, b + n);
    }
    template <class U>
    void le(U v) {
        for (std::size_t i = 0; i < sizeof(U); ++i) out.push_back(static_cast<std::uint8_t>(v >> (8 * i)));
    }
    std::vector<std::uint8_t> out;
};

class ByteReader {
public:
    explicit ByteReader(std::span<const std::uint8_t> in) : in_(in) {}
    bool raw(void* p, std::size_t n) {
        if (pos_ + n > in_.size()) return false;
        std::memcpy(p, in_.data() + pos_, n);
        pos_ += n;
        return true;
    }
    template <class U>
    bool le(U& v) {
        if (pos_ + sizeof(U) > in_.size()) return false;
        v = 0;
        for (std::size_t i = 0; i < sizeof(U); ++i) v |= static_cast<U>(U{in_[pos_ + i]} << (8 * i));
        pos_ += sizeof(U);
        return true;
    }
    bool at_end() const { return pos_ == in_.size(); }

private:
    std::span<const std::uint8_t> in_;
    std::size_t pos_ = 0;
};

}  // namespace

std::expected<Snapshot, std::string> capture(Instance& instance) {
    const ModuleInfo& info = instance.module()->info();
    if (auto ready = check_snapshot_ready(info); !ready) {
        return std::unexpected(ready.error());
    }

    Snapshot snap;
    snap.module_hash = instance.module()->hash();
    for (const Global& g : info.globals) {
        if (!g.is_mutable || g.imported) continue;
        auto bits = instance.global_bits(*g.export_name);
        if (!bits) return std::unexpected(bits.error());
        snap.globals.push_back({*g.export_name, static_cast<std::uint8_t>(g.type), *bits});
    }

    const std::span<std::uint8_t> mem = instance.memory();
    snap.page_count = static_cast<std::uint32_t>(mem.size() / page_size);
    snap.pages.resize(snap.page_count);
    for (std::uint32_t p = 0; p < snap.page_count; ++p) {
        const auto page = mem.subspan(std::size_t{p} * page_size, page_size);
        if (is_zero_page(page)) continue;
        snap.pages[p].zero = false;
        snap.pages[p].hash = sha256(page);
        snap.data.insert(snap.data.end(), page.begin(), page.end());
    }
    return snap;
}

std::expected<std::unique_ptr<Instance>, std::string> restore(std::shared_ptr<Module> module, const Snapshot& snapshot,
                                                              const Limits& limits) {
    if (module->hash() != snapshot.module_hash) {
        return std::unexpected("image belongs to module " + to_hex(snapshot.module_hash) + ", not " +
                               module->hash_hex());
    }
    if (snapshot.page_count > limits.max_memory_pages) {
        return std::unexpected("image needs " + std::to_string(snapshot.page_count) + " pages, limit is " +
                               std::to_string(limits.max_memory_pages));
    }
    auto instance = Instance::create(module, limits, /*run_initializer=*/false);
    if (!instance) return std::unexpected(instance.error());
    Instance& inst = **instance;

    if (inst.page_count() > snapshot.page_count) {
        return std::unexpected("image has fewer pages than the module's initial memory");
    }
    if (!inst.grow_to(snapshot.page_count)) {
        return std::unexpected("cannot grow memory to " + std::to_string(snapshot.page_count) + " pages");
    }

    const std::span<std::uint8_t> mem = inst.memory();
    std::size_t data_offset = 0;
    for (std::uint32_t p = 0; p < snapshot.page_count; ++p) {
        std::uint8_t* dst = mem.data() + std::size_t{p} * page_size;
        if (snapshot.pages[p].zero) {
            std::memset(dst, 0, page_size);
            continue;
        }
        if (data_offset + page_size > snapshot.data.size()) {
            return std::unexpected("image data truncated");
        }
        std::memcpy(dst, snapshot.data.data() + data_offset, page_size);
        data_offset += page_size;
    }
    for (const GlobalValue& g : snapshot.globals) {
        if (auto r = inst.set_global_bits(g.name, g.bits); !r) return std::unexpected(r.error());
    }
    return instance;
}

std::vector<std::uint8_t> serialize(const Snapshot& snap) {
    ByteWriter w;
    w.raw(magic, sizeof magic);
    w.raw(snap.module_hash.data(), snap.module_hash.size());
    w.le<std::uint32_t>(snap.page_count);
    w.le<std::uint32_t>(static_cast<std::uint32_t>(snap.globals.size()));
    for (const GlobalValue& g : snap.globals) {
        w.le<std::uint16_t>(static_cast<std::uint16_t>(g.name.size()));
        w.raw(g.name.data(), g.name.size());
        w.le<std::uint8_t>(g.type);
        w.le<std::uint64_t>(g.bits);
    }
    for (const PageEntry& p : snap.pages) {
        w.le<std::uint8_t>(p.zero ? 0 : 1);
        if (!p.zero) w.raw(p.hash.data(), p.hash.size());
    }
    w.raw(snap.data.data(), snap.data.size());
    return std::move(w.out);
}

std::expected<Snapshot, std::string> deserialize(std::span<const std::uint8_t> bytes) {
    ByteReader r(bytes);
    char m[8];
    if (!r.raw(m, sizeof m) || std::memcmp(m, magic, sizeof magic) != 0) {
        return std::unexpected("not a paglets memory image");
    }
    Snapshot snap;
    std::uint32_t global_count = 0;
    if (!r.raw(snap.module_hash.data(), snap.module_hash.size()) || !r.le(snap.page_count) || !r.le(global_count)) {
        return std::unexpected("truncated image header");
    }
    for (std::uint32_t i = 0; i < global_count; ++i) {
        GlobalValue g;
        std::uint16_t len = 0;
        if (!r.le(len)) return std::unexpected("truncated global");
        g.name.resize(len);
        if (!r.raw(g.name.data(), len) || !r.le(g.type) || !r.le(g.bits)) return std::unexpected("truncated global");
        snap.globals.push_back(std::move(g));
    }
    snap.pages.resize(snap.page_count);
    std::size_t data_pages = 0;
    for (PageEntry& p : snap.pages) {
        std::uint8_t flag = 0;
        if (!r.le(flag)) return std::unexpected("truncated page table");
        p.zero = flag == 0;
        if (!p.zero) {
            if (!r.raw(p.hash.data(), p.hash.size())) return std::unexpected("truncated page table");
            ++data_pages;
        }
    }
    snap.data.resize(data_pages * page_size);
    if (!r.raw(snap.data.data(), snap.data.size()) || !r.at_end()) {
        return std::unexpected("image data size mismatch");
    }
    for (std::size_t i = 0, d = 0; i < snap.pages.size(); ++i) {
        if (snap.pages[i].zero) continue;
        const auto page = std::span<const std::uint8_t>(snap.data).subspan(d * page_size, page_size);
        if (sha256(page) != snap.pages[i].hash) {
            return std::unexpected("page " + std::to_string(i) + " fails its hash check");
        }
        ++d;
    }
    return snap;
}

}  // namespace paglets::wasm

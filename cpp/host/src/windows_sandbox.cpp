// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.
#include "windows_sandbox.hpp"

#include <aclapi.h>
#include <bcrypt.h>
#include <sddl.h>
#include <tlhelp32.h>
#include <userenv.h>
#include <map>
#include <mutex>
#include <stdexcept>
#include <cwctype>

namespace paglets::ipc {
namespace {
std::string error(const char* op, DWORD code = GetLastError()) {
    return std::string(op) + " failed (error " + std::to_string(code) + ")";
}
std::wstring lower(std::wstring s) {
    for (auto& c : s) c = std::towlower(c);
    return s;
}
std::wstring unique_name() {
    unsigned char random[16];
    if (BCryptGenRandom(nullptr, random, sizeof random, BCRYPT_USE_SYSTEM_PREFERRED_RNG) != 0)
        throw std::runtime_error("AppContainer identity entropy failed");
    std::wstring s = L"paglets.worker.";
    for (auto b : random) { s += L"0123456789abcdef"[b >> 4]; s += L"0123456789abcdef"[b & 15]; }
    return s;
}
struct Files {
    std::filesystem::path directory;
    std::vector<HANDLE> sources;
    ~Files() {
        for (HANDLE h : sources) CloseHandle(h);
        std::error_code ec;
        if (!directory.empty()) std::filesystem::remove_all(directory, ec);
    }
};
std::shared_ptr<Files> files_for(const std::filesystem::path& exe) {
    static std::mutex mu;
    static std::map<std::filesystem::path, std::shared_ptr<Files>> cache;
    std::lock_guard lock(mu);
    const auto source = std::filesystem::canonical(exe);
    if (auto i = cache.find(source); i != cache.end()) return i->second;
    auto files = std::make_shared<Files>();
    std::map<std::wstring, std::filesystem::path> inputs;
    inputs.emplace(lower(source.filename().wstring()), source);
    // Adjacent DLLs take precedence, matching the executable's DLL search.
    for (const auto& entry : std::filesystem::directory_iterator(source.parent_path())) {
        if (entry.is_regular_file() && lower(entry.path().extension().wstring()) == L".dll")
            inputs.emplace(lower(entry.path().filename().wstring()), entry.path());
    }
    wchar_t windows[MAX_PATH];
    if (!GetWindowsDirectoryW(windows, MAX_PATH)) throw std::runtime_error(error("GetWindowsDirectory"));
    const auto system_prefix = lower(std::wstring(windows)) + L"\\";
    HANDLE snapshot = CreateToolhelp32Snapshot(TH32CS_SNAPMODULE, GetCurrentProcessId());
    if (snapshot == INVALID_HANDLE_VALUE) throw std::runtime_error(error("module snapshot"));
    MODULEENTRY32W module{}; module.dwSize = sizeof module;
    if (!Module32FirstW(snapshot, &module)) { CloseHandle(snapshot); throw std::runtime_error(error("module list")); }
    do {
        std::wstring name(32768, L'\0');
        const DWORD length = GetModuleFileNameW(module.hModule, name.data(), static_cast<DWORD>(name.size()));
        if (!length || length >= name.size()) { CloseHandle(snapshot); throw std::runtime_error(error("runtime module path")); }
        name.resize(length);
        const std::filesystem::path path(name);
        if (lower(path.extension().wstring()) == L".dll" && !lower(path.wstring()).starts_with(system_prefix))
            inputs.emplace(lower(path.filename().wstring()), path);
    } while (Module32NextW(snapshot, &module));
    CloseHandle(snapshot);

    HANDLE token = nullptr;
    if (!OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &token)) throw std::runtime_error(error("OpenProcessToken"));
    DWORD size = 0; GetTokenInformation(token, TokenUser, nullptr, 0, &size);
    std::vector<unsigned char> user(size);
    const bool got = GetTokenInformation(token, TokenUser, user.data(), size, &size);
    CloseHandle(token);
    if (!got) throw std::runtime_error(error("TokenUser"));
    LPWSTR sid = nullptr;
    if (!ConvertSidToStringSidW(reinterpret_cast<TOKEN_USER*>(user.data())->User.Sid, &sid))
        throw std::runtime_error(error("user SID"));
    // Only this user and SYSTEM may modify the bundle. LPACs may read code.
    const std::wstring acl = L"D:P(A;OICI;FA;;;" + std::wstring(sid) + L")(A;OICI;FA;;;SY)(A;OICI;GRGX;;;S-1-15-2-2)";
    LocalFree(sid);
    PSECURITY_DESCRIPTOR sd = nullptr;
    if (!ConvertStringSecurityDescriptorToSecurityDescriptorW(acl.c_str(), SDDL_REVISION_1, &sd, nullptr))
        throw std::runtime_error(error("bundle DACL"));
    std::unique_ptr<void, decltype(&LocalFree)> descriptor_owner(sd, LocalFree);
    BOOL present = FALSE, defaulted = FALSE; PACL bundle_acl = nullptr;
    if (!GetSecurityDescriptorDacl(sd, &present, &bundle_acl, &defaulted) || !present)
        throw std::runtime_error(error("bundle file DACL"));
    SECURITY_ATTRIBUTES attributes{sizeof(attributes), sd, FALSE};
    const auto directory = std::filesystem::temp_directory_path() / unique_name();
    const bool created = CreateDirectoryW(directory.c_str(), &attributes);
    const DWORD create_error = GetLastError();
    if (!created) throw std::runtime_error(error("create worker bundle", create_error));
    files->directory = directory;
    for (const auto& [name, input] : inputs) {
        // Pin source bytes for this host's lifetime: a rebuild must first stop it.
        HANDLE h = CreateFileW(input.c_str(), GENERIC_READ, FILE_SHARE_READ, nullptr, OPEN_EXISTING, 0, nullptr);
        if (h == INVALID_HANDLE_VALUE) throw std::runtime_error(error("pin worker runtime file"));
        files->sources.push_back(h);
        const auto output = directory / input.filename();
        if (!CopyFileW(input.c_str(), output.c_str(), TRUE)) throw std::runtime_error(error("copy worker runtime file"));
        if (!SetFileAttributesW(output.c_str(), FILE_ATTRIBUTE_NORMAL)) throw std::runtime_error(error("bundle attributes"));
        const DWORD security = SetNamedSecurityInfoW(const_cast<wchar_t*>(output.c_str()), SE_FILE_OBJECT,
            DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION, nullptr, nullptr, bundle_acl, nullptr);
        if (security) throw std::runtime_error(error("protect worker runtime file", security));
    }
    cache.emplace(source, files);
    return files;
}
std::vector<unsigned char> registry_capability() {
    using Derive = BOOL(WINAPI*)(PCWSTR, PSID**, DWORD*, PSID**, DWORD*);
    auto derive = reinterpret_cast<Derive>(GetProcAddress(GetModuleHandleW(L"kernelbase.dll"), "DeriveCapabilitySidsFromName"));
    PSID *groups = nullptr, *caps = nullptr; DWORD ng = 0, nc = 0;
    if (!derive || !derive(L"registryRead", &groups, &ng, &caps, &nc))
        throw std::runtime_error(error("derive registryRead capability"));
    std::vector<unsigned char> result;
    if (nc == 1) {
        result.resize(GetLengthSid(caps[0]));
        CopySid(static_cast<DWORD>(result.size()), result.data(), caps[0]);
    }
    for (DWORD i = 0; i < ng; ++i) LocalFree(groups[i]);
    for (DWORD i = 0; i < nc; ++i) LocalFree(caps[i]);
    LocalFree(groups); LocalFree(caps);
    if (result.empty()) throw std::runtime_error("registryRead capability was not unique");
    return result;
}
}  // namespace

std::expected<void, std::string> verify_windows_identity(HANDLE token) {
    DWORD container = 0, size = 0;
    if (!GetTokenInformation(token, TokenIsAppContainer, &container, sizeof container, &size) || !container)
        return std::unexpected(std::string("the worker was not launched in an AppContainer"));
    GetTokenInformation(token, TokenCapabilities, nullptr, 0, &size);
    std::vector<unsigned char> bytes(size);
    if (!size || !GetTokenInformation(token, TokenCapabilities, bytes.data(), size, &size))
        return std::unexpected(error("worker token capabilities"));
    try {
        const auto expected = registry_capability();
        const auto* capabilities = reinterpret_cast<const TOKEN_GROUPS*>(bytes.data());
        if (capabilities->GroupCount != 1 || !EqualSid(capabilities->Groups[0].Sid, const_cast<unsigned char*>(expected.data())))
            return std::unexpected(std::string("worker token has capabilities beyond registryRead"));
    } catch (const std::exception& e) { return std::unexpected(e.what()); }
    return {};
}

WindowsSandbox::~WindowsSandbox() {
    // Called only after the process ends; no profiles of other tasks are reused.
    if (!profile.empty()) DeleteAppContainerProfile(profile.c_str());
}
std::expected<std::shared_ptr<WindowsSandbox>, std::string>
prepare_windows_sandbox(const std::filesystem::path& exe, std::intptr_t job) {
    try {
        auto result = std::make_shared<WindowsSandbox>();
        auto files = files_for(exe);
        result->files = files;
        result->executable = files->directory / exe.filename();
        result->registry = registry_capability();  // Winsock initialization; no network capabilities.
        const auto name = unique_name();
        PSID sid = nullptr;
        HRESULT hr = CreateAppContainerProfile(name.c_str(), L"paglets worker", L"Temporary isolated paglets worker", nullptr, 0, &sid);
        if (FAILED(hr)) return std::unexpected(error("create AppContainer profile", static_cast<DWORD>(hr)));
        result->profile = name;
        result->sid.resize(GetLengthSid(sid));
        CopySid(static_cast<DWORD>(result->sid.size()), result->sid.data(), sid);
        FreeSid(sid);
        if (job >= 0) {
            // The child may inspect the limits already installed by its host.
            PACL old_acl = nullptr, next = nullptr; PSECURITY_DESCRIPTOR sd = nullptr;
            DWORD rc = GetSecurityInfo(reinterpret_cast<HANDLE>(job), SE_KERNEL_OBJECT, DACL_SECURITY_INFORMATION,
                                       nullptr, nullptr, &old_acl, nullptr, &sd);
            if (rc) return std::unexpected(error("read worker job DACL", rc));
            EXPLICIT_ACCESSW access{};
            access.grfAccessPermissions = JOB_OBJECT_QUERY;
            access.grfAccessMode = GRANT_ACCESS;
            access.Trustee.TrusteeForm = TRUSTEE_IS_SID;
            access.Trustee.TrusteeType = TRUSTEE_IS_USER;
            access.Trustee.ptstrName = reinterpret_cast<LPWSTR>(result->sid.data());
            rc = SetEntriesInAclW(1, &access, old_acl, &next);
            if (!rc) rc = SetSecurityInfo(reinterpret_cast<HANDLE>(job), SE_KERNEL_OBJECT, DACL_SECURITY_INFORMATION,
                                          nullptr, nullptr, next, nullptr);
            LocalFree(next); LocalFree(sd);
            if (rc) return std::unexpected(error("grant worker job query", rc));
        }
        return result;
    } catch (const std::exception& e) { return std::unexpected(e.what()); }
}
}  // namespace paglets::ipc

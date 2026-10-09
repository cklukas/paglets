// Copyright (c) 2026 by C. Klukas.
// Licensed under the MIT License. See LICENSE for details.

// Test certificates: CAs and server certificates for 127.0.0.1, made with
// OpenSSL in memory (PEM).

#pragma once

#include <openssl/bio.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/rand.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>

#include <array>
#include <filesystem>
#include <fstream>
#include <string>

namespace paglets::test {

struct TestCert {
    std::string cert;  // PEM
    std::string key;   // PEM
};

// A certificate for `cn`, signed by `issuer` (self-signed without one): a CA,
// or a server certificate for the IP address 127.0.0.1. Valid from
// `from_s` to `to_s` seconds from now.
inline TestCert make_test_cert(const std::string& cn, const TestCert* issuer, bool ca, long from_s = -3600,
                               long to_s = 24L * 3600) {
    auto pem = [](auto write) {
        BIO* bio = BIO_new(BIO_s_mem());
        write(bio);
        char* data = nullptr;
        const long n = BIO_get_mem_data(bio, &data);
        std::string out(data, static_cast<std::size_t>(n));
        BIO_free(bio);
        return out;
    };
    auto read_cert = [](const std::string& text) {
        BIO* bio = BIO_new_mem_buf(text.data(), static_cast<int>(text.size()));
        X509* x = PEM_read_bio_X509(bio, nullptr, nullptr, nullptr);
        BIO_free(bio);
        return x;
    };
    auto read_key = [](const std::string& text) {
        BIO* bio = BIO_new_mem_buf(text.data(), static_cast<int>(text.size()));
        EVP_PKEY* k = PEM_read_bio_PrivateKey(bio, nullptr, nullptr, nullptr);
        BIO_free(bio);
        return k;
    };

    EVP_PKEY* key = EVP_EC_gen("P-256");
    X509* x = X509_new();
    X509_set_version(x, 2);
    std::array<unsigned char, 16> serial{};
    RAND_bytes(serial.data(), static_cast<int>(serial.size()));
    serial[0] &= 0x7f;
    BIGNUM* bn = BN_bin2bn(serial.data(), static_cast<int>(serial.size()), nullptr);
    BN_to_ASN1_INTEGER(bn, X509_get_serialNumber(x));
    BN_free(bn);
    X509_gmtime_adj(X509_getm_notBefore(x), from_s);
    X509_gmtime_adj(X509_getm_notAfter(x), to_s);
    X509_set_pubkey(x, key);
    X509_NAME_add_entry_by_txt(X509_get_subject_name(x), "CN", MBSTRING_UTF8,
                               reinterpret_cast<const unsigned char*>(cn.c_str()), -1, -1, 0);
    X509* issuer_cert = issuer != nullptr ? read_cert(issuer->cert) : x;
    EVP_PKEY* issuer_key = issuer != nullptr ? read_key(issuer->key) : key;
    X509_set_issuer_name(x, X509_get_subject_name(issuer_cert));
    X509V3_CTX ctx;
    X509V3_set_ctx_nodb(&ctx);
    X509V3_set_ctx(&ctx, issuer_cert, x, nullptr, nullptr, 0);
    auto add = [&](int nid, const char* value) {
        X509_EXTENSION* e = X509V3_EXT_conf_nid(nullptr, &ctx, nid, value);
        X509_add_ext(x, e, -1);
        X509_EXTENSION_free(e);
    };
    if (ca) {
        add(NID_basic_constraints, "critical,CA:TRUE");
        add(NID_key_usage, "critical,keyCertSign,cRLSign");
    } else {
        add(NID_basic_constraints, "CA:FALSE");
        add(NID_subject_alt_name, "IP:127.0.0.1");
        add(NID_ext_key_usage, "serverAuth");
    }
    X509_sign(x, issuer_key, EVP_sha256());
    TestCert out;
    out.cert = pem([&](BIO* b) { PEM_write_bio_X509(b, x); });
    out.key = pem([&](BIO* b) { PEM_write_bio_PrivateKey(b, key, nullptr, nullptr, 0, nullptr, nullptr); });
    if (issuer != nullptr) {
        X509_free(issuer_cert);
        EVP_PKEY_free(issuer_key);
    }
    X509_free(x);
    EVP_PKEY_free(key);
    return out;
}

inline std::filesystem::path write_test_file(const std::filesystem::path& path, const std::string& text) {
    std::ofstream(path, std::ios::binary) << text;
    return path;
}

}  // namespace paglets::test

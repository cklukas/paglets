# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
#
# Homebrew formula for paglets/cpp, published in the tap
# https://github.com/cklukas/homebrew-paglets:
#   brew install cklukas/paglets/paglets
# This file is the source of the tap's Formula/paglets.rb. The release
# workflow (.github/workflows/cpp-release.yml) fills in url and sha256 for
# every cpp-v* release (packaging/homebrew/update-formula.sh) and pushes the
# result to the tap.
class Paglets < Formula
  desc "Mobile agents as WebAssembly modules: hosts, mesh tools and a web UI"
  homepage "https://cklukas.github.io/paglets/cpp/"
  url "https://github.com/cklukas/paglets/releases/download/cpp-v0.2.0/paglets-cpp-0.2.0-source.tar.gz"
  sha256 "0000000000000000000000000000000000000000000000000000000000000000"
  license "MIT"
  head "https://github.com/cklukas/paglets.git", branch: "cpp"

  depends_on "cmake" => :build
  depends_on "ninja" => :build
  depends_on "gcc" # C++26 reflection (GCC 16); its libstdc++ at run time
  depends_on "libsodium"
  depends_on "openssl@3"
  depends_on "zstd"

  # The C++26 compiler: clang has no reflection yet.
  fails_with :clang

  # Fetched by the CMake build otherwise (cmake/PagletsWamr.cmake,
  # cmake/PagletsHttplib.cmake); the tags must match those files.
  resource "wamr" do
    url "https://github.com/bytecodealliance/wasm-micro-runtime/archive/refs/tags/WAMR-2.4.5.tar.gz"
    sha256 "1ab09d51099f276ca4a1d6629f6b589aab2bd0caa01445e05031a4bed22c199b"
  end

  resource "httplib" do
    url "https://github.com/yhirose/cpp-httplib/archive/refs/tags/v0.58.0.tar.gz"
    sha256 "31932ce8b33f2905472a987dc1984b9d8a7d338083a066021349587cc5f1cfea"
  end

  def install
    resource("wamr").stage buildpath/"deps/wamr"
    resource("httplib").stage buildpath/"deps/httplib"
    # Guest paglets need a WASI clang (see caveats); the formula installs the
    # host programs, the guest SDK and the CMake package.
    system "cmake", "-S", "cpp", "-B", "build", "-G", "Ninja",
           "-DPAGLETS_BUILD_TESTS=OFF",
           "-DPAGLETS_BUILD_GUESTS=OFF",
           "-DFETCHCONTENT_FULLY_DISCONNECTED=ON",
           "-DFETCHCONTENT_SOURCE_DIR_WAMR=#{buildpath}/deps/wamr",
           "-DFETCHCONTENT_SOURCE_DIR_HTTPLIB=#{buildpath}/deps/httplib",
           *std_cmake_args
    system "cmake", "--build", "build"
    system "cmake", "--install", "build"
  end

  def caveats
    <<~EOS
      paglets-host runs hosts and talks to meshes; paglets-web shows a mesh in
      the browser. To build your own paglets (Wasm modules) against this
      package with find_package(paglets), install a WASI toolchain too:
        brew install llvm lld wasi-libc wasi-runtimes
      Documentation: https://cklukas.github.io/paglets/cpp/
    EOS
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/paglets-host --version")
    assert_match version.to_s, shell_output("#{bin}/paglets-web --version")
    assert_match "reflection: yes", shell_output("#{bin}/paglets-host --info")
    system bin/"paglets-host", "keys", "init", "--role", "host", "--name", "test", "--out", testpath/"host.key"
    assert_path_exists testpath/"host.key"
  end
end

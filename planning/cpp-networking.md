# paglets/cpp: networking and movement (WP12)

Status: in progress. Companion to the plan (sections 3.1, 3.2 and 3.7) and to
[cpp-security-and-communication.md](cpp-security-and-communication.md)
(sections 3.1, 4.6 and 5.1), which state the goals; this document records the
concrete design.

## 1. Layers

1. **Transport**: HTTPS (TLS 1.3). It keeps reverse proxies and firewalls
   working; it is not what authenticates hosts.
2. **Channels**: end-to-end encrypted, mutually authenticated Noise channels
   between identity keys of a mesh (section 2), carried over the transport.
3. **Frames**: canonical values (the ledger's encoding) inside channels:
   gossip, module transfers and launches (WP11), deliveries and moves.

## 2. Channels

The Noise protocol framework (revision 34) with the pattern XX:
`Noise_XX_25519_ChaChaPoly_SHA256`, built on libsodium (X25519,
ChaCha20-Poly1305 IETF, SHA-256 and HMAC-SHA256). Code:
`cpp/host/src/net/noise.cpp` and `channel.cpp`; tests in
`cpp/tests/test_noise.cpp` include the test vector of the cacophony suite.

```text
-> e
<- e, ee, s, es
-> s, se
```

- **Static keys** are the X25519 forms of the Ed25519 identity keys (host
  keys between hosts; admin or owner keys for CLI sessions). Both sides prove
  possession of them in the handshake; ephemeral keys give forward secrecy.
- **Identities**: the responder's second message and the initiator's third
  message carry a payload `{v: 1, key: <Ed25519 public key>, role: host |
  admin | owner}`, encrypted by the handshake. The receiver converts the key
  to X25519 and requires it to equal the static key the handshake proved, so
  the peer of a channel is an identity key; the caller then checks it
  against the ledger (an enrolled host, an admin, an enrolled owner).
- **Prologue**: `"paglets channel v1" 0x00 <mesh ID>`. Peers of different
  meshes derive different keys and fail at the second message.
- **Frames**: a frame is split into Noise messages of at most 65 535 bytes;
  each plaintext starts with a flag byte (1: last part). Receivers refuse
  frames over their limit (64 MB by default). A message that fails
  authentication, arrives twice or out of order breaks the channel; the
  sender then opens a new one.
- The handshake hash is kept as the channel binding.

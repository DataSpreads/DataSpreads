# Security design WIP - the content obsoletes hours after writing, we are at research stage

Key points:

Libsodium is great, but
* AES-GCM ony hardware, but supported in WebCrypto. We could polyfill that with netcoreapp 3.0 support and even fallback to Bouncy when in .NET without hardware support.
* WebCrypto does not support Chacha/Poly.
* WebCrypto does not support Ed25519, only NIST (P-384 as our default).
* See https://github.com/jedisct1/wasm-crypto and Exonum js client, my crypto bookmarks as well.
* We have reserved 1 byte in StreamBlock, use it for crypto params. Decide later on layout. When the entire byte is zero it's Ed25519+Chacha20/Poly1305

Libsodium.js works in browsers. We will start with Ed25519+Chacha20/Poly1305. But need to reserve slots for version info, we will need NIST and other 
official algos for certain use cases.

We could use WebCrypto to store non-exportable RSAES-OAEP and with it encrypt Ed25519 key. However this is still vulnerable to xss.

## DataStream Hashing

Blake2b keyed chained hash. It's fast with .NET intrinsics and supported in browser.

Signature does not depend on this algo. Ed25519 uses SHA512 over arbitrary message size, we cannot change that (there is a Rust tweak impl, but not portable).

Sha512 block size is 128 bit. We could use digest size of the same length. For our purposes truncating Blake2B is probably fine, 
even to 64 is maybe fine (https://security.stackexchange.com/questions/47445/how-secure-is-a-partial-64bit-hash-of-a-sha1-160bit-hash).
But 16 bytes size is OK.

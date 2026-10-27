# snarkos-node-rest

[![Crates.io](https://img.shields.io/crates/v/snarkos-node-rest.svg?color=neon)](https://crates.io/crates/snarkos-node-rest)
[![Authors](https://img.shields.io/badge/authors-Aleo-orange.svg)](https://aleo.org)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](./LICENSE.md)

The `snarkos-node-rest` crate provides a REST API for the `snarkos` node.

REST responses support gzip compression when the client sends `Accept-Encoding: gzip`.
Responses smaller than 32 bytes are not compressed.

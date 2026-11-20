# snarkos-node-bft-ledger-service

[![Crates.io](https://img.shields.io/crates/v/snarkos-node-bft-ledger-service.svg?color=neon)](https://crates.io/crates/snarkos-node-bft-ledger-service)
[![Authors](https://img.shields.io/badge/authors-Aleo-orange.svg)](https://aleo.org)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](./LICENSE.md)

The `snarkos-node-bft-ledger-service` crate provides a ledger service implementation for snarkOS's memroy pool. This can, for example, be a wrapper around snarkVM's ledger.

## Atomic Updates
This crate provides a simple abstraction to perform atomic updates.
Users first invoke `LedgerService::begin_ledger_update`, which returns a `LedgerUpdate` object.

Then, they can use the object check if a specific block can be added to the ledger and to advance the Ledger. The implementation ensures only one `LedgerUpdate` can be active at any time, preventing concurrent updates.
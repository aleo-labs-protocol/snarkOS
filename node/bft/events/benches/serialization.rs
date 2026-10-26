// Copyright (c) 2019-2026 Provable Inc.
// This file is part of the snarkOS library.

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:

// http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Benchmarks for `EventCodec`.
//!
//! Encoding an event has two distinct costs, and the benchmarks below separate them:
//!
//! - Serializing the payload. This is paid only when the payload is held as a `Data::Object`,
//!   and it dominates everything else when it applies.
//! - Copying the encoded bytes into the connection's write buffer. This is paid either way, and
//!   it is all that remains once the payload is already a `Data::Buffer`.
//!
//! Run with `cargo bench -p snarkos-node-bft-events`.

use snarkos_node_bft_events::{BatchCertified, Event, EventCodec, TransmissionResponse};
use snarkvm::{
    console::types::Field,
    ledger::narwhal::{
        BatchCertificate,
        Data,
        Transmission,
        TransmissionID,
        batch_certificate::test_helpers::sample_batch_certificate_for_round_with_committee,
    },
    prelude::{FromBytes, MainnetV0, Network, PrivateKey, Rng, TestRng, ToBytes, Uniform},
};

use bytes::{Bytes, BytesMut};
use criterion::{Criterion, criterion_group, criterion_main};
use indexmap::IndexSet;
use std::hint::black_box;
use tokio_util::codec::Encoder;

type CurrentNetwork = MainnetV0;

/// The mainnet committee size, i.e. the number of signatures a certificate from a full committee
/// carries, and the number of peers a broadcast fans out to.
const COMMITTEE_SIZE: usize = 40;

/// The maximum number of previous certificates a batch header may reference.
const PREVIOUS_CERTIFICATES: usize = 40;

/// Round 2 is the earliest round permitted to reference previous certificates.
const ROUND: u64 = 2;

/// Payload sizes for the transmission benchmarks: a typical transaction, and the maximum one.
const TRANSMISSION_SIZES: [(&str, usize); 2] = [("16KiB", 16 * 1024), ("768KiB", 768 * 1024)];

/// Builds a batch certificate representative of a full mainnet committee: signed by every
/// validator, and referencing a full round of previous certificates.
fn sample_certificate(rng: &mut TestRng) -> BatchCertificate<CurrentNetwork> {
    let previous_certificate_ids: IndexSet<_> =
        (0..PREVIOUS_CERTIFICATES).map(|_| Field::<CurrentNetwork>::rand(rng)).collect();
    let committee: Vec<_> = (0..COMMITTEE_SIZE).map(|_| PrivateKey::<CurrentNetwork>::new(rng).unwrap()).collect();

    sample_batch_certificate_for_round_with_committee(
        ROUND,
        previous_certificate_ids,
        &committee[0],
        &committee[1..],
        rng,
    )
}

/// Round-trips a certificate through its serialized form.
///
/// This matters for more than convenience. `Group` stores a projective point, and serializing one
/// calls `to_affine`, which needs a field inversion unless the point is already normalized. A
/// locally produced signature is not normalized, but one parsed from bytes is. A primary assembles
/// a certificate from peer `BatchSignature` events, so benchmarking a freshly signed certificate
/// would overstate the cost of encoding a real one by two orders of magnitude.
fn from_wire(certificate: &BatchCertificate<CurrentNetwork>) -> BatchCertificate<CurrentNetwork> {
    BatchCertificate::from_bytes_le(&certificate.to_bytes_le().unwrap()).unwrap()
}

/// Builds a transmission response carrying an opaque payload of the given size.
fn sample_transmission_response(size: usize, rng: &mut TestRng) -> Event<CurrentNetwork> {
    let transmission_id = TransmissionID::Transaction(
        Field::<CurrentNetwork>::rand(rng).into(),
        rng.random::<<CurrentNetwork as Network>::TransmissionChecksum>(),
    );
    let transmission = Transmission::Transaction(Data::Buffer(Bytes::from(vec![0xAB; size])));
    Event::TransmissionResponse(TransmissionResponse::new(transmission_id, transmission))
}

/// Encodes the event exactly as a connection's writer task would.
fn encode(event: Event<CurrentNetwork>, codec: &mut EventCodec<CurrentNetwork>, dst: &mut BytesMut) {
    codec.encode(event, dst).unwrap();
    dst.clear();
}

fn encoded_len(event: &Event<CurrentNetwork>) -> usize {
    let mut dst = BytesMut::new();
    EventCodec::<CurrentNetwork>::default().encode(event.clone(), &mut dst).unwrap();
    dst.len()
}

/// Encoding a certificate, with the payload held either as an object or as bytes.
///
/// The `object` cases are dominated by serializing the certificate; the `buffer` case is what is
/// left once serialization is out of the way, and is dominated by copying into the write buffer.
fn bench_encode_certificate(c: &mut Criterion) {
    let rng = &mut TestRng::default();
    let certificate = sample_certificate(rng);

    let signed_locally = Event::BatchCertified(BatchCertified::new(Data::Object(certificate.clone())));
    let parsed_from_wire = Event::BatchCertified(BatchCertified::new(Data::Object(from_wire(&certificate))));
    let serialized =
        Event::BatchCertified(BatchCertified::new(Data::Buffer(certificate.to_bytes_le().unwrap().into())));

    let len = encoded_len(&serialized);
    println!("BatchCertified with {COMMITTEE_SIZE} signatures encodes to {len} bytes");

    let mut group = c.benchmark_group("encode_batch_certified");
    for (name, event) in [
        ("object_signed_locally", &signed_locally),
        ("object_parsed_from_wire", &parsed_from_wire),
        ("buffer", &serialized),
    ] {
        group.bench_function(name, |b| {
            let mut codec = EventCodec::<CurrentNetwork>::default();
            let mut dst = BytesMut::with_capacity(len * 2);
            b.iter(|| encode(black_box(event.clone()), &mut codec, &mut dst))
        });
    }
    group.finish();
}

/// Encoding an already-serialized transmission of a realistic size.
///
/// Nothing here is serialized, so this measures only what the codec itself does with the bytes.
fn bench_encode_transmission(c: &mut Criterion) {
    let rng = &mut TestRng::default();

    let mut group = c.benchmark_group("encode_transmission_response");
    for (name, size) in TRANSMISSION_SIZES {
        let event = sample_transmission_response(size, rng);
        let len = encoded_len(&event);
        group.throughput(criterion::Throughput::Bytes(len as u64));
        group.bench_function(name, |b| {
            let mut codec = EventCodec::<CurrentNetwork>::default();
            let mut dst = BytesMut::with_capacity(len * 2);
            b.iter(|| encode(black_box(event.clone()), &mut codec, &mut dst))
        });
    }
    group.finish();
}

criterion_group!(benches, bench_encode_certificate, bench_encode_transmission);
criterion_main!(benches);

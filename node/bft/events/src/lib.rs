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

#![forbid(unsafe_code)]

mod batch_certified;
pub use batch_certified::BatchCertified;

mod batch_propose;
pub use batch_propose::BatchPropose;

mod batch_signature;
pub use batch_signature::BatchSignature;

mod block_request;
pub use block_request::BlockRequest;

mod block_response;
pub use block_response::{BlockResponse, DataBlocks};

mod certificate_request;
pub use certificate_request::CertificateRequest;

mod certificate_response;
pub use certificate_response::CertificateResponse;

mod challenge_request;
pub use challenge_request::ChallengeRequest;

mod challenge_response;
pub use challenge_response::ChallengeResponse;

mod disconnect;
pub use disconnect::{Disconnect, DisconnectReason};

mod helpers;
pub use helpers::*;

mod primary_ping;
pub use primary_ping::PrimaryPing;

mod transmission_request;
pub use transmission_request::TransmissionRequest;

mod transmission_response;
pub use transmission_response::TransmissionResponse;

mod validators_request;
pub use validators_request::ValidatorsRequest;

mod validators_response;
pub use validators_response::ValidatorsResponse;

mod worker_ping;
pub use worker_ping::WorkerPing;

#[cfg(any(test, feature = "test-helpers"))]
pub mod committee_prop_tests;

use snarkos_node_sync_locators::BlockLocators;
use snarkvm::{
    console::prelude::{FromBytes, Network, Read, ToBytes, Write, error, io_error},
    ledger::{
        block::Block,
        narwhal::{BatchCertificate, BatchHeader, Data, Transmission, TransmissionID},
    },
    prelude::{Address, Field, Signature},
};

use anyhow::{Result, bail, ensure};
use indexmap::{IndexMap, IndexSet};
use serde::{Deserialize, Serialize};
pub use std::io::{self, Result as IoResult};
use std::{borrow::Cow, net::SocketAddr};

pub trait EventTrait: ToBytes + FromBytes {
    /// Returns the event name.
    fn name(&self) -> Cow<'static, str>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
// TODO (howardwu): For mainnet - Remove this clippy lint. The CertificateResponse should not
//  be a large enum variant, after removing the versioning.
#[allow(clippy::large_enum_variant)]
pub enum Event<N: Network> {
    BatchPropose(BatchPropose<N>),
    BatchSignature(BatchSignature<N>),
    BatchCertified(BatchCertified<N>),
    BlockRequest(BlockRequest),
    BlockResponse(BlockResponse<N>),
    CertificateRequest(CertificateRequest<N>),
    CertificateResponse(CertificateResponse<N>),
    ChallengeRequest(ChallengeRequest<N>),
    ChallengeResponse(ChallengeResponse<N>),
    Disconnect(Disconnect),
    PrimaryPing(PrimaryPing<N>),
    TransmissionRequest(TransmissionRequest<N>),
    TransmissionResponse(TransmissionResponse<N>),
    ValidatorsRequest(ValidatorsRequest),
    ValidatorsResponse(ValidatorsResponse<N>),
    WorkerPing(WorkerPing<N>),
}

impl<N: Network> From<DisconnectReason> for Event<N> {
    fn from(reason: DisconnectReason) -> Self {
        Self::Disconnect(Disconnect { reason })
    }
}

/// Replaces the payload with its serialized form, if it is not already serialized.
fn serialize_payload<T: FromBytes + ToBytes + Send + 'static>(payload: &mut Data<T>) -> Result<()> {
    if let Data::Object(object) = payload {
        *payload = Data::Buffer(object.to_bytes_le()?.into());
    }
    Ok(())
}

/// Returns `true` if the payload still holds an object that would have to be serialized.
fn is_payload_unserialized<T: FromBytes + ToBytes + Send + 'static>(payload: &mut Data<T>) -> bool {
    matches!(payload, Data::Object(_))
}

/// Applies `$f` to the event's [`Data`] payload, evaluating to `$default` if it carries none.
///
/// Both [`Event::serialize_payload`] and [`Event::has_unserialized_payload`] need to know which
/// variants carry a payload. Routing both through this macro keeps that list in exactly one place,
/// so a newly added variant cannot be handled by one and silently forgotten by the other.
macro_rules! with_data_payload {
    ($event:expr, $f:ident, $default:expr) => {
        match $event {
            Self::BatchPropose(event) => $f(&mut event.batch_header),
            Self::BatchCertified(event) => $f(&mut event.certificate),
            Self::PrimaryPing(event) => $f(&mut event.primary_certificate),
            Self::BlockResponse(event) => $f(&mut event.blocks),
            Self::ChallengeResponse(event) => $f(&mut event.signature),
            Self::TransmissionResponse(event) => match &mut event.transmission {
                Transmission::Solution(solution) => $f(solution),
                Transmission::Transaction(transaction) => $f(transaction),
                Transmission::Ratification => $default,
            },
            // The remaining events carry no `Data` payload.
            //
            // Note that `CertificateResponse` is in this list only because its certificate is held
            // directly rather than behind a `Data`, unlike every other certificate-bearing event.
            // It is consequently serialized on the writer task and deserialized on the reading
            // task, and cannot benefit from either of the methods below until that is changed.
            Self::BatchSignature(_)
            | Self::BlockRequest(_)
            | Self::CertificateRequest(_)
            | Self::CertificateResponse(_)
            | Self::ChallengeRequest(_)
            | Self::Disconnect(_)
            | Self::TransmissionRequest(_)
            | Self::ValidatorsRequest(_)
            | Self::ValidatorsResponse(_)
            | Self::WorkerPing(_) => $default,
        }
    };
}

impl<N: Network> Event<N> {
    /// The oldest event protocol version this node accepts from a peer; it can be incremented in
    /// order to force users to update.
    pub const MINIMUM_VERSION: u32 = 10;
    /// The version of the event protocol this node speaks.
    pub const VERSION: u32 = 11;

    /// Serializes the event's [`Data`] payload, if it holds one that is not already serialized.
    ///
    /// [`Data`] defers serialization until the event is written to a stream, so an event that is
    /// cloned for several peers is serialized once per peer, separately, inside each connection's
    /// writer task. Calling this beforehand performs the serialization once; every clone then
    /// shares the resulting buffer, and each writer only has to copy it to the wire.
    ///
    /// Before a broadcast, this avoids serializing the same payload once per recipient. Before a
    /// single send it saves no total work, but it still matters: it moves the serialization off
    /// the connection's writer task, which is a Tokio worker and should not be running compute.
    /// A large payload serialized there stalls the reactor, and it does so inside the write
    /// timeout, which then has to be sized to accommodate it.
    ///
    /// Accordingly, `Transport::send` performs this on a blocking thread for every outbound event,
    /// and `Transport::broadcast` performs it once up front so that the fan-out only clones the
    /// resulting buffer.
    ///
    /// It is a no-op for events that carry no payload, or whose payload is already serialized, so
    /// applying it twice is harmless.
    pub fn serialize_payload(&mut self) -> Result<()> {
        with_data_payload!(self, serialize_payload, Ok(()))
    }

    /// Returns `true` if this event carries a payload that has not been serialized yet.
    ///
    /// Callers use this to decide whether [`Self::serialize_payload`] is worth the cost of moving
    /// the event to a blocking thread. Most events carry no payload at all, and re-sending an
    /// already-serialized one is common, so the check keeps that hop off the common path.
    ///
    /// Note this takes `&mut self` only because it shares its variant list with
    /// [`Self::serialize_payload`]; it does not modify the event.
    pub fn has_unserialized_payload(&mut self) -> bool {
        with_data_payload!(self, is_payload_unserialized, false)
    }

    /// Returns the event type. Instance fields, such as a block height range, are omitted.
    #[inline]
    pub fn variant_name(&self) -> &'static str {
        match self {
            Self::BatchPropose(..) => "BatchPropose",
            Self::BatchSignature(..) => "BatchSignature",
            Self::BatchCertified(..) => "BatchCertified",
            Self::BlockRequest(..) => "BlockRequest",
            Self::BlockResponse(..) => "BlockResponse",
            Self::CertificateRequest(..) => "CertificateRequest",
            Self::CertificateResponse(..) => "CertificateResponse",
            Self::ChallengeRequest(..) => "ChallengeRequest",
            Self::ChallengeResponse(..) => "ChallengeResponse",
            Self::Disconnect(..) => "Disconnect",
            Self::PrimaryPing(..) => "PrimaryPing",
            Self::TransmissionRequest(..) => "TransmissionRequest",
            Self::TransmissionResponse(..) => "TransmissionResponse",
            Self::ValidatorsRequest(..) => "ValidatorsRequest",
            Self::ValidatorsResponse(..) => "ValidatorsResponse",
            Self::WorkerPing(..) => "WorkerPing",
        }
    }

    /// Returns the event name.
    #[inline]
    pub fn name(&self) -> Cow<'static, str> {
        match self {
            Self::BatchPropose(event) => event.name(),
            Self::BatchSignature(event) => event.name(),
            Self::BatchCertified(event) => event.name(),
            Self::BlockRequest(event) => event.name(),
            Self::BlockResponse(event) => event.name(),
            Self::CertificateRequest(event) => event.name(),
            Self::CertificateResponse(event) => event.name(),
            Self::ChallengeRequest(event) => event.name(),
            Self::ChallengeResponse(event) => event.name(),
            Self::Disconnect(event) => event.name(),
            Self::PrimaryPing(event) => event.name(),
            Self::TransmissionRequest(event) => event.name(),
            Self::TransmissionResponse(event) => event.name(),
            Self::ValidatorsRequest(event) => event.name(),
            Self::ValidatorsResponse(event) => event.name(),
            Self::WorkerPing(event) => event.name(),
        }
    }

    /// Returns the event ID.
    #[inline]
    pub fn id(&self) -> u16 {
        match self {
            Self::BatchPropose(..) => 0,
            Self::BatchSignature(..) => 1,
            Self::BatchCertified(..) => 2,
            Self::BlockRequest(..) => 3,
            Self::BlockResponse(..) => 4,
            Self::CertificateRequest(..) => 5,
            Self::CertificateResponse(..) => 6,
            Self::ChallengeRequest(..) => 7,
            Self::ChallengeResponse(..) => 8,
            Self::Disconnect(..) => 9,
            Self::PrimaryPing(..) => 10,
            Self::TransmissionRequest(..) => 11,
            Self::TransmissionResponse(..) => 12,
            Self::ValidatorsRequest(..) => 13,
            Self::ValidatorsResponse(..) => 14,
            Self::WorkerPing(..) => 15,
        }
    }
}

impl<N: Network> ToBytes for Event<N> {
    fn write_le<W: io::Write>(&self, mut writer: W) -> IoResult<()> {
        self.id().write_le(&mut writer)?;

        match self {
            Self::BatchPropose(event) => event.write_le(writer),
            Self::BatchSignature(event) => event.write_le(writer),
            Self::BatchCertified(event) => event.write_le(writer),
            Self::BlockRequest(event) => event.write_le(writer),
            Self::BlockResponse(event) => event.write_le(writer),
            Self::CertificateRequest(event) => event.write_le(writer),
            Self::CertificateResponse(event) => event.write_le(writer),
            Self::ChallengeRequest(event) => event.write_le(writer),
            Self::ChallengeResponse(event) => event.write_le(writer),
            Self::Disconnect(event) => event.write_le(writer),
            Self::PrimaryPing(event) => event.write_le(writer),
            Self::TransmissionRequest(event) => event.write_le(writer),
            Self::TransmissionResponse(event) => event.write_le(writer),
            Self::ValidatorsRequest(event) => event.write_le(writer),
            Self::ValidatorsResponse(event) => event.write_le(writer),
            Self::WorkerPing(event) => event.write_le(writer),
        }
    }
}

impl<N: Network> FromBytes for Event<N> {
    fn read_le<R: io::Read>(mut reader: R) -> io::Result<Self> {
        // Read the event ID.
        let id = u16::read_le(&mut reader).map_err(|_| error("Unknown event ID"))?;

        // Deserialize the data field.
        let event = match id {
            0 => Self::BatchPropose(BatchPropose::read_le(&mut reader)?),
            1 => Self::BatchSignature(BatchSignature::read_le(&mut reader)?),
            2 => Self::BatchCertified(BatchCertified::read_le(&mut reader)?),
            3 => Self::BlockRequest(BlockRequest::read_le(&mut reader)?),
            4 => Self::BlockResponse(BlockResponse::read_le(&mut reader)?),
            5 => Self::CertificateRequest(CertificateRequest::read_le(&mut reader)?),
            6 => Self::CertificateResponse(CertificateResponse::read_le(&mut reader)?),
            7 => Self::ChallengeRequest(ChallengeRequest::read_le(&mut reader)?),
            8 => Self::ChallengeResponse(ChallengeResponse::read_le(&mut reader)?),
            9 => Self::Disconnect(Disconnect::read_le(&mut reader)?),
            10 => Self::PrimaryPing(PrimaryPing::read_le(&mut reader)?),
            11 => Self::TransmissionRequest(TransmissionRequest::read_le(&mut reader)?),
            12 => Self::TransmissionResponse(TransmissionResponse::read_le(&mut reader)?),
            13 => Self::ValidatorsRequest(ValidatorsRequest::read_le(&mut reader)?),
            14 => Self::ValidatorsResponse(ValidatorsResponse::read_le(&mut reader)?),
            15 => Self::WorkerPing(WorkerPing::read_le(&mut reader)?),
            16.. => return Err(error(format!("Unknown event ID {id}"))),
        };

        // Ensure that there are no "dangling" bytes.
        // `bytes()` is inefficient, but we read at most one byte here.
        #[allow(clippy::unbuffered_bytes)]
        if reader.bytes().next().is_some() {
            return Err(error("Leftover bytes in an Event"));
        }

        Ok(event)
    }
}

#[cfg(test)]
mod tests {
    use crate::Event;
    use bytes::{Buf, BufMut, BytesMut};
    use snarkvm::console::prelude::{FromBytes, ToBytes};
    type CurrentNetwork = snarkvm::prelude::MainnetV0;

    #[test]
    fn deserializing_invalid_data_panics() {
        let buf = BytesMut::default();
        let invalid_id = u16::MAX;
        invalid_id.write_le(&mut buf.clone().writer()).unwrap();
        assert_eq!(
            Event::<CurrentNetwork>::read_le(buf.reader()).unwrap_err().to_string(),
            format!("Unknown event ID")
        );
    }
}

#[cfg(test)]
pub mod prop_tests {
    use crate::{
        Disconnect,
        DisconnectReason,
        Event,
        ValidatorsRequest,
        batch_certified::prop_tests::any_batch_certified,
        batch_propose::prop_tests::any_batch_propose,
        batch_signature::prop_tests::any_batch_signature,
        block_request::prop_tests::any_block_request,
        block_response::prop_tests::any_block_response,
        certificate_request::prop_tests::any_certificate_request,
        certificate_response::prop_tests::any_certificate_response,
        challenge_request::prop_tests::any_challenge_request,
        challenge_response::prop_tests::any_challenge_response,
        primary_ping::prop_tests::any_primary_ping,
        transmission_request::prop_tests::any_transmission_request,
        transmission_response::prop_tests::any_transmission_response,
        validators_response::prop_tests::any_validators_response,
        worker_ping::prop_tests::any_worker_ping,
    };
    use snarkvm::{
        console::{network::Network, types::Field},
        ledger::{narwhal::TransmissionID, puzzle::SolutionID},
        prelude::{FromBytes, ToBytes, Uniform},
    };

    use proptest::{
        prelude::{BoxedStrategy, Just, ProptestConfig, Strategy, any},
        prop_oneof,
        sample::Selector,
    };
    use rand::SeedableRng;
    use rand_chacha::ChaChaRng;
    use test_strategy::proptest;

    type CurrentNetwork = snarkvm::prelude::MainnetV0;

    /// Returns the current UTC epoch timestamp.
    pub fn now() -> i64 {
        time::OffsetDateTime::now_utc().unix_timestamp()
    }

    pub fn any_solution_id() -> BoxedStrategy<SolutionID<CurrentNetwork>> {
        any::<u64>().prop_map(|x| x.into()).boxed()
    }

    pub fn any_transaction_id() -> BoxedStrategy<<CurrentNetwork as Network>::TransactionID> {
        any::<u64>()
            .prop_map(|seed| {
                let rng = &mut ChaChaRng::seed_from_u64(seed);
                <CurrentNetwork as Network>::TransactionID::from(Field::rand(rng))
            })
            .boxed()
    }

    pub fn any_transmission_checksum() -> BoxedStrategy<<CurrentNetwork as Network>::TransmissionChecksum> {
        any::<<CurrentNetwork as Network>::TransmissionChecksum>().boxed()
    }

    pub fn any_transmission_id() -> BoxedStrategy<TransmissionID<CurrentNetwork>> {
        prop_oneof![
            (any_transaction_id(), any_transmission_checksum())
                .prop_map(|(id, cs)| TransmissionID::Transaction(id, cs)),
            (any_solution_id(), any_transmission_checksum()).prop_map(|(id, cs)| TransmissionID::Solution(id, cs)),
        ]
        .boxed()
    }

    /// A strategy covering every [`Event`] variant.
    ///
    /// Keep this exhaustive. Several properties are asserted over "any event", and a variant that
    /// is missing here is silently untested rather than failing -- which is how `BlockResponse` and
    /// `PrimaryPing`, the two largest `Data`-carrying events, went uncovered.
    pub fn any_event() -> BoxedStrategy<Event<CurrentNetwork>> {
        prop_oneof![
            any_batch_certified().prop_map(Event::BatchCertified),
            any_batch_propose().prop_map(Event::BatchPropose),
            any_batch_signature().prop_map(Event::BatchSignature),
            any_block_request().prop_map(Event::BlockRequest),
            any_block_response().prop_map(Event::BlockResponse),
            any_certificate_request().prop_map(Event::CertificateRequest),
            any_certificate_response().prop_map(Event::CertificateResponse),
            any_challenge_request().prop_map(Event::ChallengeRequest),
            any_challenge_response().prop_map(Event::ChallengeResponse),
            (
                Just(vec![
                    DisconnectReason::ProtocolViolation,
                    DisconnectReason::NoReasonGiven,
                    DisconnectReason::InvalidChallengeResponse,
                    DisconnectReason::OutdatedClientVersion,
                    DisconnectReason::SelfConnect,
                    DisconnectReason::NoExternalPeersAllowed,
                    DisconnectReason::AlreadyConnecting,
                    DisconnectReason::AlreadyConnected,
                    DisconnectReason::AlreadyConnectedToAleoAddress,
                    DisconnectReason::InvalidChallengeRequest,
                    DisconnectReason::UnauthorizedValidator,
                ]),
                any::<Selector>()
            )
                .prop_map(|(reasons, selector)| Event::Disconnect(Disconnect::from(selector.select(reasons)))),
            any_primary_ping().prop_map(Event::PrimaryPing),
            any_transmission_request().prop_map(Event::TransmissionRequest),
            any_transmission_response().prop_map(Event::TransmissionResponse),
            Just(ValidatorsRequest).prop_map(Event::ValidatorsRequest),
            any_validators_response().prop_map(Event::ValidatorsResponse),
            any_worker_ping().prop_map(Event::WorkerPing)
        ]
        .boxed()
    }

    #[proptest(ProptestConfig { cases: 128, ..ProptestConfig::default() })]
    fn serialize_deserialize(#[strategy(any_event())] original: Event<CurrentNetwork>) {
        let mut buf = Vec::new();
        Event::write_le(&original, &mut buf).unwrap();

        let deserialized: Event<CurrentNetwork> = Event::read_le(&*buf).unwrap();
        assert_eq!(original.id(), deserialized.id());
        assert_eq!(original.name(), deserialized.name());
    }

    /// Serializing the payload ahead of time must be invisible on the wire, otherwise doing it
    /// before a broadcast would change what peers receive.
    #[proptest(ProptestConfig { cases: 128, ..ProptestConfig::default() })]
    fn serialize_payload_preserves_the_encoding(#[strategy(any_event())] original: Event<CurrentNetwork>) {
        let mut expected = Vec::new();
        Event::write_le(&original, &mut expected).unwrap();

        let mut event = original.clone();
        event.serialize_payload().unwrap();

        let mut actual = Vec::new();
        Event::write_le(&event, &mut actual).unwrap();

        assert_eq!(expected, actual, "{} encoded differently once its payload was serialized", original.name());
    }

    /// Serializing the payload must be idempotent, so that broadcasting an event that has already
    /// been serialized does not deserialize and re-serialize it.
    #[proptest(ProptestConfig { cases: 128, ..ProptestConfig::default() })]
    fn serialize_payload_is_idempotent(#[strategy(any_event())] original: Event<CurrentNetwork>) {
        let mut once = original.clone();
        once.serialize_payload().unwrap();

        let mut twice = once.clone();
        twice.serialize_payload().unwrap();

        assert_eq!(once, twice);
    }

    /// `has_unserialized_payload` is what decides whether an event is worth handing to a blocking
    /// thread, so it must agree with `serialize_payload` about which events have work to do. If the
    /// two ever disagreed, a payload-carrying event could be serialized on a writer task after all.
    #[proptest(ProptestConfig { cases: 128, ..ProptestConfig::default() })]
    fn has_unserialized_payload_agrees_with_serialize_payload(
        #[strategy(any_event())] original: Event<CurrentNetwork>,
    ) {
        let mut event = original.clone();

        // Serializing must clear the flag, whether or not it was set to begin with.
        event.serialize_payload().unwrap();
        assert!(!event.has_unserialized_payload(), "{} still reports an unserialized payload", original.name());

        // And an event that reports no work to do must be unchanged by doing the work.
        let mut reported_no_work = original.clone();
        if !reported_no_work.has_unserialized_payload() {
            let before = reported_no_work.clone();
            reported_no_work.serialize_payload().unwrap();
            assert_eq!(before, reported_no_work, "{} changed despite reporting no work", original.name());
        }
    }
}

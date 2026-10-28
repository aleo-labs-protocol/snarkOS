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

use snarkos_node_bft_events::ConsensusSchedule;
use snarkvm::{
    console::network::Network,
    ledger::committee::Committee,
    prelude::{Address, FromBytes, IoResult, Read, Result, ToBytes, Write},
};

#[cfg(feature = "locktick")]
use locktick::parking_lot::Mutex;
#[cfg(not(feature = "locktick"))]
use parking_lot::Mutex;
use std::{
    collections::{HashMap, HashSet},
    fmt,
    fs,
    io,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
use tokio::sync::watch;

/// How often the monitor asks for a scheduled upgrade to be logged.
const WARNING_INTERVAL: Duration = Duration::from_secs(600);

/// How many blocks before the activation height of a required upgrade this build stops building
/// blocks, so that an error of a few blocks in the stop cannot reach the activation height.
pub const REQUIRED_UPGRADE_SAFETY_MARGIN: u32 = 100;

/// A consensus version that validators holding the availability threshold of stake activate at a
/// height where this build schedules an older one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RequiredUpgrade {
    pub height: u32,
    pub consensus_version: u16,
}

impl ToBytes for RequiredUpgrade {
    fn write_le<W: Write>(&self, mut writer: W) -> IoResult<()> {
        self.height.write_le(&mut writer)?;
        self.consensus_version.write_le(&mut writer)
    }
}

impl RequiredUpgrade {
    /// Returns the height of the first block that this build does not build,
    /// [`REQUIRED_UPGRADE_SAFETY_MARGIN`] blocks before the activation height.
    pub fn stop_height(&self) -> u32 {
        self.height.saturating_sub(REQUIRED_UPGRADE_SAFETY_MARGIN)
    }

    /// Writes the record of this required upgrade to `path`.
    ///
    /// The record is written to a temporary file next to `path` and then renamed, so that a crash
    /// leaves either the previous record or this one, never a truncated record. The rename is
    /// atomic on the file systems a node runs on.
    pub fn store(&self, path: &Path) -> Result<()> {
        let temporary = path.with_extension("tmp");
        let mut file = fs::File::create(&temporary)?;
        file.write_all(&self.to_bytes_le()?)?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        Ok(())
    }
}

/// The error for a block that this build may not build, as validators may run a newer consensus
/// version at its height. The block is retried with the next commit.
#[derive(Debug)]
pub struct BlockHeldBack(String);

impl fmt::Display for BlockHeldBack {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for BlockHeldBack {}

impl fmt::Display for RequiredUpgrade {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Validators holding at least a third of the stake run ConsensusVersion::V{} from height {}",
            self.consensus_version, self.height
        )
    }
}

impl FromBytes for RequiredUpgrade {
    fn read_le<R: Read>(mut reader: R) -> IoResult<Self> {
        let height = u32::read_le(&mut reader)?;
        let consensus_version = u16::read_le(&mut reader)?;
        Ok(Self { height, consensus_version })
    }
}

/// A consensus schedule as a step function of the height.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Steps(Vec<(u32, u16)>);

impl Steps {
    /// Returns the steps of `schedule`, in ascending order of height and of version.
    ///
    /// Peers send their schedules unchecked, so an entry that does not raise the version is dropped,
    /// which makes [`Self::version_at`] the highest version activated at or below a height.
    fn new(schedule: &ConsensusSchedule) -> Self {
        let mut entries: Vec<(u32, u16)> = schedule
            .heights()
            .iter()
            .zip(1u16..)
            .filter(|(height, _)| **height != u32::MAX)
            .map(|(height, version)| (*height, version))
            .collect();
        entries.sort_unstable();

        let mut steps: Vec<(u32, u16)> = Vec::with_capacity(entries.len());
        for (height, version) in entries {
            match steps.last_mut() {
                Some((_, last_version)) if version <= *last_version => {}
                Some((last_height, last_version)) if height == *last_height => *last_version = version,
                _ => steps.push((height, version)),
            }
        }
        Self(steps)
    }

    /// Returns the consensus version at `height`, or `0` if none is active.
    fn version_at(&self, height: u32) -> u16 {
        match self.0.partition_point(|(step_height, _)| *step_height <= height) {
            0 => 0,
            index => self.0[index - 1].1,
        }
    }

    /// Returns the lowest height at which this schedule runs a newer version than `own`, if any.
    ///
    /// The schedule can only overtake `own` where one of its own versions activates.
    fn first_ahead_of(&self, own: &Steps) -> Option<u32> {
        self.0.iter().find(|(height, version)| *version > own.version_at(*height)).map(|(height, _)| *height)
    }

    /// Returns the height at which `version`, or a later version, activates, if any.
    fn height_of(&self, version: u16) -> Option<u32> {
        self.0.iter().find(|(_, step_version)| *step_version >= version).map(|(height, _)| *height)
    }
}

/// Whether validators run a consensus version this build lacks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UpgradeStatus {
    /// No upgrade is required.
    Clear,
    /// An upgrade is required, and its stop height is beyond the next block to build.
    Scheduled(RequiredUpgrade),
    /// An upgrade is required, and the next block to build is at or past its stop height. Once set,
    /// it never changes.
    Required(RequiredUpgrade),
}

impl UpgradeStatus {
    /// Returns the required upgrade, if the status is `Required`.
    pub fn required(&self) -> Option<RequiredUpgrade> {
        match self {
            Self::Required(required) => Some(*required),
            Self::Clear | Self::Scheduled(_) => None,
        }
    }
}

/// The values of the `needs_upgrade`, `required_upgrade_height` and `required_upgrade_version`
/// metrics. An upgrade counts as soon as it is scheduled, so that operators can act before this
/// build stops, and a record counts while it holds back blocks.
fn metric_values(status: UpgradeStatus, recorded: Option<RequiredUpgrade>) -> (u8, u32, u16) {
    match (status, recorded) {
        (UpgradeStatus::Scheduled(required) | UpgradeStatus::Required(required), _)
        | (UpgradeStatus::Clear, Some(required)) => (1, required.height, required.consensus_version),
        (UpgradeStatus::Clear, None) => (0, 0, 0),
    }
}

/// The record that stands in for one that cannot be read. It holds back every block until a quorum
/// that is not ahead of this build at any height refutes it.
const UNREADABLE_RECORD: RequiredUpgrade = RequiredUpgrade { height: 0, consensus_version: u16::MAX };

/// The evidence the monitor keeps.
struct State<N: Network> {
    /// The steps of the schedule each validator disclosed in its latest handshake.
    ///
    /// The steps of a validator that disconnects are kept: its schedule is fixed by its build, and
    /// dropping it could hide a required upgrade at the moment its height is reached.
    schedules: HashMap<Address<N>, Steps>,
    /// The required upgrade that a previous run recorded, until connected validators refute it.
    recorded: Option<RequiredUpgrade>,
    /// When the monitor last asked for the scheduled upgrade to be logged.
    warned_at: Option<Instant>,
    /// The metric values last published, which start at those of `Clear` without a record.
    metrics: (u8, u32, u16),
}

/// Tracks the consensus schedules of validators, and decides whether this build may build a block.
///
/// Once the status is `Required`, the owner of the monitor stores the upgrade at
/// [`Self::record_path`] before it stops, so that the requirement outlives the process.
pub struct UpgradeMonitor<N: Network> {
    /// The address of this validator.
    address: Address<N>,
    /// The steps of this build's schedule.
    own: Steps,
    /// The file that records a required upgrade.
    record_path: PathBuf,
    /// The evidence; status transitions happen while this lock is held.
    state: Mutex<State<N>>,
    /// The current status.
    status: watch::Sender<UpgradeStatus>,
}

impl<N: Network> UpgradeMonitor<N> {
    /// Initializes the monitor of the validator at `address` from the record at `record_path`.
    ///
    /// A record of an upgrade that this build schedules is removed. Any other record holds back the
    /// blocks from its stop height until connected validators refute it, and one that cannot be
    /// read holds back every block.
    pub fn new(address: Address<N>, record_path: PathBuf) -> Self {
        let own = Steps::new(&ConsensusSchedule::of::<N>());
        let recorded = match read_record(&record_path) {
            Ok(None) => None,
            Ok(Some(recorded)) if own.version_at(recorded.height) >= recorded.consensus_version => {
                info!(
                    "This build schedules ConsensusVersion::V{} at height {}, as previously required",
                    recorded.consensus_version, recorded.height
                );
                remove_record(&record_path);
                None
            }
            Ok(Some(recorded)) => {
                warn!(
                    "A previous run found that validators run ConsensusVersion::V{} at height {}, which this build \
                     does not schedule. No block at or above height {} is built until connected validators \
                     confirm or refute it.",
                    recorded.consensus_version,
                    recorded.height,
                    recorded.stop_height()
                );
                Some(recorded)
            }
            Err(error) => {
                warn!("Unable to read the required upgrade at {} - {error}", record_path.display());
                Some(UNREADABLE_RECORD)
            }
        };
        let state = State { schedules: Default::default(), recorded, warned_at: None, metrics: (0, 0, 0) };
        Self { address, own, record_path, state: Mutex::new(state), status: watch::Sender::new(UpgradeStatus::Clear) }
    }

    /// Records the schedule that the validator at `address` disclosed in a completed handshake, or
    /// forgets its previous one if it disclosed none.
    pub fn record_schedule(&self, address: Address<N>, schedule: Option<&ConsensusSchedule>) {
        let mut state = self.state.lock();
        match schedule {
            Some(schedule) => state.schedules.insert(address, Steps::new(schedule)),
            None => state.schedules.remove(&address),
        };
    }

    /// Re-evaluates the status for the next block to build, at `next_height`.
    ///
    /// `connected` holds the validators that are connected now. A record is removed once this
    /// validator and the connected validators that are not ahead of it at the recorded height hold
    /// the quorum threshold of stake.
    pub fn update(&self, committee: &Committee<N>, connected: &HashSet<Address<N>>, next_height: u32) -> UpgradeStatus {
        self.update_locked(&mut self.state.lock(), committee, connected, next_height)
    }

    /// Re-evaluates the status like [`Self::update`], with the state already locked.
    fn update_locked(
        &self,
        state: &mut State<N>,
        committee: &Committee<N>,
        connected: &HashSet<Address<N>>,
        next_height: u32,
    ) -> UpgradeStatus {
        if let UpgradeStatus::Required(required) = self.status() {
            return UpgradeStatus::Required(required);
        }

        if let Some(recorded) = state.recorded {
            let refuted_height = (recorded != UNREADABLE_RECORD).then_some(recorded.height);
            if self.is_refuted(state, committee, connected, refuted_height) {
                match refuted_height {
                    Some(height) => info!(
                        "Connected validators holding a quorum of stake do not run ConsensusVersion::V{} at height {height}",
                        recorded.consensus_version
                    ),
                    None => info!(
                        "Connected validators holding a quorum of stake are not ahead of this build at any height, so \
                         the unreadable record of a required upgrade is removed"
                    ),
                }
                remove_record(&self.record_path);
                state.recorded = None;
            }
        }

        let status = match self.required_upgrade(state, committee) {
            Some(required) if required.stop_height() <= next_height => UpgradeStatus::Required(required),
            Some(required) => {
                self.warn_of(state, required);
                UpgradeStatus::Scheduled(required)
            }
            None => UpgradeStatus::Clear,
        };
        self.status.send_if_modified(|current| std::mem::replace(current, status) != status);
        let metrics = metric_values(status, state.recorded);
        if std::mem::replace(&mut state.metrics, metrics) != metrics {
            #[cfg(feature = "metrics")]
            Self::publish_metrics(metrics);
        }
        status
    }

    /// Sets the metrics that let operators alert on an upgrade that validators require of this build.
    #[cfg(feature = "metrics")]
    fn publish_metrics((needs_upgrade, height, consensus_version): (u8, u32, u16)) {
        metrics::gauge(metrics::consensus::NEEDS_UPGRADE, needs_upgrade);
        metrics::gauge(metrics::consensus::REQUIRED_UPGRADE_HEIGHT, height);
        metrics::gauge(metrics::consensus::REQUIRED_UPGRADE_VERSION, consensus_version);
    }

    /// Returns an error unless this build may build the block at `height`.
    ///
    /// Beyond a required upgrade, a block is held back:
    /// - from a record's stop height, until this validator and the connected validators that are
    ///   not ahead of it at the recorded height hold the quorum threshold of stake, and
    /// - at the first height at which a committee member's schedule runs a newer version than this
    ///   build's, until this validator and the connected validators that are not ahead of it at
    ///   that height hold the quorum threshold of stake.
    ///
    /// Connected validators that disclosed no schedule count as not ahead. A member's schedule
    /// counts only at the first height where it is ahead of this build; any other height would let
    /// it list many activations and hold back as many blocks. So a faulty member can hold back only
    /// that height, and only while this validator is connected to less than a quorum of stake that
    /// is not ahead there. Block sync still advances the ledger past it.
    ///
    /// The error is a [`BlockHeldBack`].
    pub fn ensure_block_may_be_built(
        &self,
        committee: &Committee<N>,
        connected: &HashSet<Address<N>>,
        height: u32,
    ) -> Result<()> {
        let mut state = self.state.lock();
        if let Some(required) = self.update_locked(&mut state, committee, connected, height).required() {
            return Err(BlockHeldBack(format!("{required}, which this build lacks")).into());
        }

        // A record that survived the update is not refuted at its activation height.
        if let Some(recorded) = state.recorded
            && recorded.stop_height() <= height
        {
            return Err(BlockHeldBack(format!(
                "A previous run found that validators may run ConsensusVersion::V{} from height {}, and connected \
                 validators holding a quorum of stake have not refuted it",
                recorded.consensus_version, recorded.height
            ))
            .into());
        }

        let is_claimed = state.schedules.iter().any(|(address, steps)| {
            committee.get_stake(*address) > 0 && steps.first_ahead_of(&self.own) == Some(height)
        });
        if is_claimed && !self.is_refuted(&state, committee, connected, Some(height)) {
            return Err(BlockHeldBack(format!(
                "Validators may run a newer consensus version than this build at height {height}, and connected \
                 validators holding a quorum of stake have not refuted it"
            ))
            .into());
        }
        Ok(())
    }

    /// Returns `true` if this validator and the connected validators that are not ahead of this
    /// build at `height`, or at any height if `height` is `None`, hold the quorum threshold of
    /// stake, which leaves the remaining validators short of the availability threshold.
    fn is_refuted(
        &self,
        state: &State<N>,
        committee: &Committee<N>,
        connected: &HashSet<Address<N>>,
        height: Option<u32>,
    ) -> bool {
        let is_ahead = |steps: &Steps| match height {
            Some(height) => steps.version_at(height) > self.own.version_at(height),
            None => steps.first_ahead_of(&self.own).is_some(),
        };
        let stake = connected
            .iter()
            .filter(|address| **address != self.address)
            .filter(|address| state.schedules.get(*address).is_none_or(|steps| !is_ahead(steps)))
            .map(|address| committee.get_stake(*address))
            .fold(committee.get_stake(self.address), u64::saturating_add);
        stake >= committee.quorum_threshold()
    }

    /// Returns the lowest height at which committee members holding the availability threshold of
    /// stake are ahead of this build, with the highest version they share there.
    ///
    /// The threshold `(f + 1)` guarantees that at least one honest validator runs a build that
    /// schedules that version at that height, so up to `f` faulty validators can neither produce a
    /// result on their own nor move it to an earlier height.
    fn required_upgrade(&self, state: &State<N>, committee: &Committee<N>) -> Option<RequiredUpgrade> {
        let members: Vec<_> =
            state.schedules.iter().filter(|(address, _)| committee.get_stake(**address) > 0).collect();

        // The set of validators ahead of this build only grows at the height of one of their steps.
        let mut heights: Vec<u32> =
            members.iter().flat_map(|(_, steps)| steps.0.iter().map(|(height, _)| *height)).collect();
        heights.sort_unstable();
        heights.dedup();

        for height in heights {
            let own_version = self.own.version_at(height);
            let mut ahead: Vec<_> = members
                .iter()
                .map(|(address, steps)| (steps.version_at(height), **address))
                .filter(|(version, _)| *version > own_version)
                .collect();
            ahead.sort_unstable_by_key(|(version, _)| std::cmp::Reverse(*version));

            // A validator ahead with a higher version also vouches for every lower one that is ahead.
            // `schedules` holds one entry per address, so no validator's stake counts twice.
            let mut stake = 0u64;
            for (version, address) in ahead {
                stake = stake.saturating_add(committee.get_stake(address));
                if stake >= committee.availability_threshold() {
                    return Some(RequiredUpgrade { height, consensus_version: version });
                }
            }
        }
        None
    }

    /// Logs a scheduled upgrade, at most once per [`WARNING_INTERVAL`].
    fn warn_of(&self, state: &mut State<N>, required: RequiredUpgrade) {
        if state.warned_at.is_some_and(|warned_at| warned_at.elapsed() < WARNING_INTERVAL) {
            return;
        }
        state.warned_at = Some(Instant::now());
        let this_build = match self.own.height_of(required.consensus_version) {
            Some(height) => format!("this build schedules it at height {height}"),
            None => "this build does not schedule it".to_string(),
        };
        error!(
            "{required}, and {this_build}. This node stops before building block {}, {REQUIRED_UPGRADE_SAFETY_MARGIN} blocks before that \
             height; upgrade snarkOS before then.",
            required.stop_height()
        );
    }

    /// Holds the lock on the evidence until the returned guard is dropped, which pauses a handshake
    /// where it records the peer's schedule.
    #[cfg(feature = "test")]
    pub fn lock_for_test(&self) -> impl Sized + '_ {
        self.state.lock()
    }

    /// Returns the path of the record of a required upgrade.
    pub fn record_path(&self) -> &Path {
        &self.record_path
    }

    /// Returns the current status.
    pub fn status(&self) -> UpgradeStatus {
        *self.status.borrow()
    }

    /// Returns a receiver that observes the status.
    pub fn subscribe(&self) -> watch::Receiver<UpgradeStatus> {
        self.status.subscribe()
    }
}

/// Reads the record of a required upgrade, or returns `None` if there is none.
fn read_record(path: &Path) -> Result<Option<RequiredUpgrade>> {
    match fs::read(path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        bytes => Ok(Some(RequiredUpgrade::from_bytes_le(&bytes?)?)),
    }
}

/// Removes the record of a required upgrade.
fn remove_record(path: &Path) {
    if let Err(error) = fs::remove_file(path) {
        warn!("Unable to remove the required upgrade at {} - {error}", path.display());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use snarkvm::{
        ledger::committee::test_helpers::sample_committee_for_round_and_members,
        prelude::{MainnetV0, TestRng},
    };

    use rand::RngExt;
    use std::sync::atomic::{AtomicUsize, Ordering};

    type CurrentNetwork = MainnetV0;

    /// A height past every activation this build schedules.
    const H: u32 = u32::MAX - 1_000;

    /// Returns a committee of four validators with equal stake, so that one validator is below the
    /// availability threshold, two reach it, and three reach the quorum threshold. The monitor runs
    /// as the last one.
    fn sample_committee() -> (Committee<CurrentNetwork>, Vec<Address<CurrentNetwork>>) {
        let rng = &mut TestRng::default();
        let addresses: Vec<_> = (0..4).map(|_| Address::new(rng.random())).collect();
        (sample_committee_for_round_and_members(1, addresses.clone(), rng), addresses)
    }

    /// A directory for the record of a required upgrade, removed on drop.
    struct RecordDir(PathBuf);

    impl RecordDir {
        fn new() -> Self {
            static COUNT: AtomicUsize = AtomicUsize::new(0);
            let count = COUNT.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!("snarkos-upgrade-monitor-{}-{count}", std::process::id()));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        fn record_path(&self) -> PathBuf {
            self.0.join("required-consensus-upgrade")
        }

        fn write(&self, bytes: &[u8]) {
            fs::write(self.record_path(), bytes).unwrap();
        }

        fn read(&self) -> Option<RequiredUpgrade> {
            fs::read(self.record_path()).ok().map(|bytes| RequiredUpgrade::from_bytes_le(&bytes).unwrap())
        }
    }

    impl Drop for RecordDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn monitor(addresses: &[Address<CurrentNetwork>], dir: &RecordDir) -> UpgradeMonitor<CurrentNetwork> {
        UpgradeMonitor::new(*addresses.last().unwrap(), dir.record_path())
    }

    /// A monitor running as the last of the validators of [`sample_committee`].
    struct Fixture {
        committee: Committee<CurrentNetwork>,
        addresses: Vec<Address<CurrentNetwork>>,
        /// Removes the record when dropped, so it must outlive `monitor`.
        dir: RecordDir,
        monitor: UpgradeMonitor<CurrentNetwork>,
    }

    fn setup() -> Fixture {
        let (committee, addresses) = sample_committee();
        let dir = RecordDir::new();
        let monitor = monitor(&addresses, &dir);
        Fixture { committee, addresses, dir, monitor }
    }

    /// Has validators 0 and 1, which reach the availability threshold, disclose a version beyond this
    /// build's at `height`.
    fn two_ahead_at(monitor: &UpgradeMonitor<CurrentNetwork>, addresses: &[Address<CurrentNetwork>], height: u32) {
        monitor.record_schedule(addresses[0], Some(&ahead_at(height)));
        monitor.record_schedule(addresses[1], Some(&ahead_at(height)));
    }

    fn own_schedule() -> ConsensusSchedule {
        ConsensusSchedule::of::<CurrentNetwork>()
    }

    /// The version that a peer schedules beyond every version of this build.
    fn next_version() -> u16 {
        own_schedule().heights().len() as u16 + 1
    }

    /// Returns this build's schedule, plus a version beyond it at `height`.
    fn ahead_at(height: u32) -> ConsensusSchedule {
        let mut heights = own_schedule().heights().to_vec();
        heights.push(height);
        ConsensusSchedule::new(heights).unwrap()
    }

    fn connected(addresses: &[Address<CurrentNetwork>]) -> HashSet<Address<CurrentNetwork>> {
        addresses.iter().copied().collect()
    }

    fn required(height: u32) -> RequiredUpgrade {
        RequiredUpgrade { height, consensus_version: next_version() }
    }

    #[test]
    fn steps_are_independent_of_the_order_of_the_table() {
        let steps = Steps::new(&ConsensusSchedule::new(vec![0, 30, 20, u32::MAX, 10, 30]).unwrap());
        // Versions 2 and 3 activate after version 5, so they never raise the version.
        assert_eq!(steps.0, vec![(0, 1), (10, 5), (30, 6)]);
        assert_eq!(steps.version_at(0), 1);
        assert_eq!(steps.version_at(9), 1);
        assert_eq!(steps.version_at(10), 5);
        assert_eq!(steps.version_at(29), 5);
        assert_eq!(steps.version_at(u32::MAX), 6);
        assert_eq!(steps.height_of(2), Some(10));
        assert_eq!(steps.height_of(7), None);
    }

    #[test]
    fn an_empty_or_unscheduled_table_has_no_version() {
        assert_eq!(Steps::new(&ConsensusSchedule::new(vec![]).unwrap()).version_at(u32::MAX), 0);
        assert_eq!(Steps::new(&ConsensusSchedule::new(vec![u32::MAX; 3]).unwrap()).version_at(u32::MAX), 0);
    }

    #[test]
    fn a_faulty_minority_cannot_require_an_upgrade() {
        let Fixture { committee, addresses, dir: _dir, monitor } = setup();
        monitor.record_schedule(addresses[0], Some(&ahead_at(0)));
        assert_eq!(monitor.update(&committee, &connected(&addresses), H), UpgradeStatus::Clear);
    }

    #[test]
    fn the_availability_threshold_schedules_an_upgrade() {
        let Fixture { committee, addresses, dir, monitor } = setup();
        two_ahead_at(&monitor, &addresses, H);
        let before_stop = required(H).stop_height() - 1;
        assert_eq!(
            monitor.update(&committee, &connected(&addresses), before_stop),
            UpgradeStatus::Scheduled(required(H))
        );
        assert_eq!(dir.read(), None);
    }

    #[test]
    fn faulty_validators_cannot_move_the_upgrade_earlier() {
        let Fixture { committee, addresses, dir: _dir, monitor } = setup();
        monitor.record_schedule(addresses[0], Some(&ahead_at(H - 500)));
        monitor.record_schedule(addresses[1], Some(&ahead_at(H)));
        assert_eq!(monitor.update(&committee, &connected(&addresses), 0), UpgradeStatus::Scheduled(required(H)));
    }

    #[test]
    fn a_postponed_version_schedules_an_upgrade() {
        // The last version this build schedules, moved earlier.
        let own = own_schedule();
        let (index, own_height) = own.heights().iter().enumerate().rfind(|(_, height)| **height != u32::MAX).unwrap();
        assert!(*own_height >= 5);
        let mut heights = own.heights().to_vec();
        heights[index] = own_height - 5;
        let earlier = ConsensusSchedule::new(heights).unwrap();

        let Fixture { committee, addresses, dir: _dir, monitor } = setup();
        monitor.record_schedule(addresses[0], Some(&earlier));
        monitor.record_schedule(addresses[1], Some(&earlier));
        let expected = RequiredUpgrade { height: own_height - 5, consensus_version: index as u16 + 1 };
        assert_eq!(monitor.update(&committee, &connected(&addresses), 0), UpgradeStatus::Scheduled(expected));
        assert_eq!(monitor.own.height_of(expected.consensus_version), Some(*own_height));
    }

    #[test]
    fn non_members_carry_no_stake() {
        let (committee, addresses) = sample_committee();
        let rng = &mut TestRng::default();
        let dir = RecordDir::new();
        let monitor = monitor(&addresses, &dir);
        for _ in 0..4 {
            monitor.record_schedule(Address::new(rng.random()), Some(&ahead_at(H)));
        }
        monitor.record_schedule(addresses[0], Some(&ahead_at(H)));
        assert_eq!(monitor.update(&committee, &connected(&addresses), H), UpgradeStatus::Clear);
    }

    #[test]
    fn a_handshake_without_a_schedule_replaces_the_previous_one() {
        let Fixture { committee, addresses, dir: _dir, monitor } = setup();
        two_ahead_at(&monitor, &addresses, H);
        monitor.record_schedule(addresses[1], None);
        assert_eq!(monitor.update(&committee, &connected(&addresses), H), UpgradeStatus::Clear);
    }

    #[test]
    fn the_upgrade_is_required_from_its_stop_height() {
        let Fixture { committee, addresses, dir: _dir, monitor } = setup();
        let receiver = monitor.subscribe();
        two_ahead_at(&monitor, &addresses, H);
        let stop = required(H).stop_height();
        assert_eq!(H - stop, REQUIRED_UPGRADE_SAFETY_MARGIN);

        assert!(monitor.ensure_block_may_be_built(&committee, &connected(&addresses), stop - 1).is_ok());
        let error = monitor.ensure_block_may_be_built(&committee, &connected(&addresses), stop).unwrap_err();
        // The BFT recognizes a held-back block through the context it adds.
        assert!(error.context("context").downcast_ref::<BlockHeldBack>().is_some());
        assert_eq!(*receiver.borrow(), UpgradeStatus::Required(required(H)));

        // Once required, the upgrade stays required.
        monitor.record_schedule(addresses[0], None);
        monitor.record_schedule(addresses[1], None);
        assert_eq!(monitor.update(&committee, &connected(&addresses), 0), UpgradeStatus::Required(required(H)));
    }

    #[test]
    fn a_stored_upgrade_is_read_back_and_leaves_no_temporary_file() {
        let Fixture { addresses, dir, monitor, .. } = setup();
        required(H).store(monitor.record_path()).unwrap();
        assert_eq!(dir.read(), Some(required(H)));
        assert!(!monitor.record_path().with_extension("tmp").exists());

        // A later run reads the record back, and holds back blocks from its height.
        let monitor = super::UpgradeMonitor::<CurrentNetwork>::new(*addresses.last().unwrap(), dir.record_path());
        assert_eq!(monitor.state.lock().recorded, Some(required(H)));
    }

    #[test]
    fn schedules_of_disconnected_validators_still_count() {
        let Fixture { committee, addresses, dir: _dir, monitor } = setup();
        two_ahead_at(&monitor, &addresses, H);
        assert_eq!(monitor.update(&committee, &HashSet::new(), H), UpgradeStatus::Required(required(H)));
    }

    #[test]
    fn a_block_is_held_back_without_a_quorum_where_a_member_is_ahead() {
        // Validator 0 is ahead and connected, validator 1 discloses no schedule, and validator 2 is
        // ahead but never connected, so too little stake is known to be ahead to require an upgrade.
        let Fixture { committee, addresses, dir: _dir, monitor } = setup();
        monitor.record_schedule(addresses[0], Some(&ahead_at(H)));
        let connected = connected(&addresses[..2]);

        assert!(monitor.ensure_block_may_be_built(&committee, &connected, H - 1).is_ok());
        assert!(monitor.ensure_block_may_be_built(&committee, &connected, H).is_err());
        assert_eq!(monitor.status(), UpgradeStatus::Clear);
    }

    #[test]
    fn a_member_ahead_holds_back_only_the_block_at_its_activation() {
        let Fixture { committee, addresses, dir: _dir, monitor } = setup();
        monitor.record_schedule(addresses[0], Some(&ahead_at(H)));
        let connected = connected(&addresses[..2]);

        assert!(monitor.ensure_block_may_be_built(&committee, &connected, H).is_err());
        assert!(monitor.ensure_block_may_be_built(&committee, &connected, H + 1).is_ok());
    }

    #[test]
    fn a_member_with_many_activations_ahead_holds_back_only_the_first() {
        let Fixture { committee, addresses, dir: _dir, monitor } = setup();
        let mut heights = own_schedule().heights().to_vec();
        heights.extend((0..150).map(|offset| H + offset));
        monitor.record_schedule(addresses[0], Some(&ConsensusSchedule::new(heights).unwrap()));
        let connected = connected(&addresses[..2]);

        assert!(monitor.ensure_block_may_be_built(&committee, &connected, H).is_err());
        for height in H + 1..H + 150 {
            assert!(monitor.ensure_block_may_be_built(&committee, &connected, height).is_ok());
        }
    }

    #[test]
    fn a_member_that_activates_a_version_at_genesis_holds_back_no_block() {
        // Without enough connected stake to refute it, this would hold back every block.
        let Fixture { committee, addresses, dir: _dir, monitor } = setup();
        monitor.record_schedule(addresses[0], Some(&ahead_at(0)));
        let connected = connected(&addresses[..2]);

        for height in [1, 1_000, H] {
            assert!(monitor.ensure_block_may_be_built(&committee, &connected, height).is_ok());
        }
        assert_eq!(monitor.status(), UpgradeStatus::Clear);
    }

    #[test]
    fn the_metrics_report_an_upgrade_from_when_it_is_scheduled() {
        let Fixture { committee, addresses, dir: _dir, monitor } = setup();
        let connected = connected(&addresses);
        let stop = required(H).stop_height();
        monitor.update(&committee, &connected, stop - 1);
        assert_eq!(monitor.state.lock().metrics, (0, 0, 0));

        two_ahead_at(&monitor, &addresses, H);
        let expected = (1, H, next_version());
        assert_eq!(monitor.update(&committee, &connected, stop - 1), UpgradeStatus::Scheduled(required(H)));
        assert_eq!(monitor.state.lock().metrics, expected);
        assert_eq!(monitor.update(&committee, &connected, stop), UpgradeStatus::Required(required(H)));
        assert_eq!(monitor.state.lock().metrics, expected);
    }

    #[test]
    fn the_metrics_report_a_record_until_it_is_refuted() {
        let (committee, addresses) = sample_committee();
        let dir = RecordDir::new();
        dir.write(&required(H).to_bytes_le().unwrap());
        let monitor = monitor(&addresses, &dir);

        assert_eq!(monitor.update(&committee, &HashSet::new(), 1), UpgradeStatus::Clear);
        assert_eq!(monitor.state.lock().metrics, (1, H, next_version()));

        monitor.record_schedule(addresses[0], Some(&own_schedule()));
        monitor.update(&committee, &connected(&addresses[..2]), 1);
        assert_eq!(monitor.state.lock().metrics, (0, 0, 0));
    }

    #[test]
    fn a_quorum_that_is_not_ahead_lets_a_block_be_built() {
        let Fixture { committee, addresses, dir: _dir, monitor } = setup();
        monitor.record_schedule(addresses[0], Some(&ahead_at(H)));
        monitor.record_schedule(addresses[1], Some(&own_schedule()));
        assert!(monitor.ensure_block_may_be_built(&committee, &connected(&addresses), H).is_ok());
    }

    #[test]
    fn a_block_is_built_when_no_validator_is_known_to_be_ahead() {
        // A known limit: a validator that never hears from one that is ahead builds the block.
        let Fixture { committee, addresses, dir: _dir, monitor } = setup();
        assert!(monitor.ensure_block_may_be_built(&committee, &connected(&addresses[..1]), H).is_ok());
    }

    #[test]
    fn a_build_that_schedules_the_recorded_upgrade_removes_the_record() {
        let (_, addresses) = sample_committee();
        let dir = RecordDir::new();
        let height = own_schedule().heights()[0];
        dir.write(&RequiredUpgrade { height, consensus_version: 1 }.to_bytes_le().unwrap());
        let _monitor = monitor(&addresses, &dir);
        assert_eq!(dir.read(), None);
    }

    #[test]
    fn a_record_holds_back_blocks_from_its_stop_height_until_refuted() {
        let (committee, addresses) = sample_committee();
        let dir = RecordDir::new();
        dir.write(&required(H).to_bytes_le().unwrap());
        let monitor = monitor(&addresses, &dir);
        let stop = required(H).stop_height();

        // Below the stop height, blocks are built.
        assert!(monitor.ensure_block_may_be_built(&committee, &HashSet::new(), stop - 1).is_ok());
        // From the stop height, this validator alone holds less than a quorum.
        assert!(monitor.ensure_block_may_be_built(&committee, &HashSet::new(), stop).is_err());
        // Two connected validators that are not ahead make a quorum with this one.
        monitor.record_schedule(addresses[0], Some(&own_schedule()));
        assert!(monitor.ensure_block_may_be_built(&committee, &connected(&addresses[..2]), stop).is_ok());
        assert_eq!(dir.read(), None);
    }

    #[test]
    fn a_record_is_refuted_only_at_its_activation_height() {
        let (committee, addresses) = sample_committee();
        let dir = RecordDir::new();
        dir.write(&required(H).to_bytes_le().unwrap());
        let monitor = monitor(&addresses, &dir);
        let stop = required(H).stop_height();

        // Validator 0 runs this build's version at the stop height but is ahead at H, so it leaves
        // the record unrefuted, although it makes a quorum with validator 1 at the stop height.
        monitor.record_schedule(addresses[0], Some(&ahead_at(H)));
        let connected = connected(&addresses[..2]);
        assert!(monitor.ensure_block_may_be_built(&committee, &connected, stop).is_err());
        assert_eq!(dir.read(), Some(required(H)));
    }

    #[test]
    fn a_record_is_confirmed_by_the_availability_threshold() {
        let (committee, addresses) = sample_committee();
        let dir = RecordDir::new();
        dir.write(&required(H).to_bytes_le().unwrap());
        let monitor = monitor(&addresses, &dir);
        two_ahead_at(&monitor, &addresses, H);
        assert_eq!(monitor.update(&committee, &connected(&addresses), H), UpgradeStatus::Required(required(H)));
        assert_eq!(dir.read(), Some(required(H)));
    }

    #[test]
    fn an_unreadable_record_holds_back_every_block_until_refuted() {
        let (committee, addresses) = sample_committee();
        let dir = RecordDir::new();
        dir.write(&[1, 2, 3]);
        let monitor = monitor(&addresses, &dir);
        assert!(monitor.ensure_block_may_be_built(&committee, &HashSet::new(), 1).is_err());
        assert!(monitor.ensure_block_may_be_built(&committee, &connected(&addresses[..2]), 1).is_ok());
        assert_eq!(dir.read(), None);
    }

    #[test]
    fn an_unreadable_record_is_refuted_only_by_a_quorum_that_is_not_ahead_at_any_height() {
        let (committee, addresses) = sample_committee();
        let dir = RecordDir::new();
        dir.write(&[1, 2, 3]);
        let monitor = monitor(&addresses, &dir);
        let stop = required(H).stop_height();

        // Validator 0 is ahead at H, so it and validator 1 leave the record unrefuted, although they
        // make a quorum with this validator at every height below H.
        monitor.record_schedule(addresses[0], Some(&ahead_at(H)));
        let connected = connected(&addresses[..2]);
        assert!(monitor.ensure_block_may_be_built(&committee, &connected, stop).is_err());
        assert_eq!(std::fs::read(dir.record_path()).ok(), Some(vec![1, 2, 3]));

        monitor.record_schedule(addresses[0], Some(&own_schedule()));
        assert!(monitor.ensure_block_may_be_built(&committee, &connected, stop).is_ok());
        assert!(!dir.record_path().exists());
    }

    #[test]
    fn a_scheduled_upgrade_is_logged_at_most_once_per_interval() {
        let Fixture { committee, addresses, dir: _dir, monitor } = setup();
        monitor.update(&committee, &connected(&addresses), 0);
        assert!(monitor.state.lock().warned_at.is_none());

        two_ahead_at(&monitor, &addresses, H);
        monitor.update(&committee, &connected(&addresses), 0);
        let warned_at = monitor.state.lock().warned_at;
        assert!(warned_at.is_some());
        monitor.update(&committee, &connected(&addresses), 1);
        assert_eq!(monitor.state.lock().warned_at, warned_at);
    }
}

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

//! An on-disk cache for test fixtures that are expensive to build.
//!
//! `cargo nextest` runs every test in a process of its own, so a `static` shares a fixture with no
//! other test. A file does.

use std::{
    fs::{self, File},
    hash::{Hash, Hasher},
    path::{Path, PathBuf},
    sync::{
        OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};

/// How long a stored fixture is used. A fixture often holds timestamps, so it is built again
/// rather than left to age without bound.
const MAX_ENTRY_AGE: Duration = Duration::from_secs(3 * 24 * 60 * 60);

/// Returns the fixture stored under `name` and `inputs`, building it with `create` if no test
/// process of this binary has stored it yet.
///
/// Every caller gets the value of whichever process stored first, so `create` must not be relied on
/// to consume randomness or to return a value specific to this process. See [`Fixture`] for the key
/// and for what concurrent callers do.
pub fn cached_fixture<T>(
    name: &str,
    inputs: &[u8],
    encode: impl FnOnce(&T) -> Vec<u8>,
    decode: impl FnOnce(&[u8]) -> Option<T>,
    create: impl FnOnce() -> T,
) -> T {
    let fixture = Fixture::open(name, inputs);
    if let Some(value) = fixture.load(decode) {
        return value;
    }
    let value = create();
    fixture.store(&encode(&value));
    value
}

/// An entry of the cache, held exclusively until it is dropped.
///
/// [`cached_fixture`] is the usual entry point. This is for a fixture that is built in an `async`
/// context: [`open`](Self::open) the entry, [`load`](Self::load) it, and if there is nothing to
/// load, build the fixture and [`store`](Self::store) it before dropping the entry.
///
/// The key of an entry covers the running executable, so a rebuilt test binary never reads what an
/// older build stored. `inputs` must cover everything else the fixture depends on.
///
/// A test that opens an entry another test holds waits for it. A test that holds an entry while it
/// builds the fixture therefore builds it for all of them.
pub struct Fixture {
    /// The file of the entry, or `None` if the cache directory cannot be created.
    path: Option<PathBuf>,
    /// The locked file that makes this the only holder of the entry.
    _lock: Option<File>,
}

impl Fixture {
    /// Opens the entry for `name` and `inputs`, once no other test holds it.
    pub fn open(name: &str, inputs: &[u8]) -> Self {
        let dir = std::env::temp_dir().join("snarkos-test-fixtures");
        if fs::create_dir_all(&dir).is_err() {
            return Self { path: None, _lock: None };
        }

        let mut hasher = std::hash::DefaultHasher::new();
        executable_identity().hash(&mut hasher);
        inputs.hash(&mut hasher);
        let file_name = format!("{name}-{:016x}", hasher.finish());

        let lock = File::create(dir.join(format!("{file_name}.lock"))).ok().filter(|file| file.lock().is_ok());
        Self { path: Some(dir.join(file_name)), _lock: lock }
    }

    /// Returns the stored fixture, if there is one that is not older than [`MAX_ENTRY_AGE`] and
    /// that `decode` accepts.
    pub fn load<T>(&self, decode: impl FnOnce(&[u8]) -> Option<T>) -> Option<T> {
        let path = self.path.as_ref()?;
        if is_old(&fs::metadata(path).ok()?) {
            return None;
        }
        decode(&fs::read(path).ok()?)
    }

    /// Stores `bytes` as the fixture, replacing any stored before.
    pub fn store(&self, bytes: &[u8]) {
        let Some(path) = &self.path else { return };

        // A test that could not lock the entry may be reading it, so the file is completed under a
        // name of its own and then renamed, which replaces the entry atomically.
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let mut temporary = path.clone().into_os_string();
        temporary.push(format!(".{}.{}.tmp", std::process::id(), COUNTER.fetch_add(1, Ordering::Relaxed)));
        if fs::write(&temporary, bytes).and_then(|_| fs::rename(&temporary, path)).is_err() {
            let _ = fs::remove_file(&temporary);
        }

        if let Some(dir) = path.parent() {
            remove_old_files(dir);
        }
    }
}

/// Identifies the build of the running executable by its path, size and modification time.
fn executable_identity() -> &'static (PathBuf, u64, Option<SystemTime>) {
    static IDENTITY: OnceLock<(PathBuf, u64, Option<SystemTime>)> = OnceLock::new();
    IDENTITY.get_or_init(|| {
        let path = std::env::current_exe().expect("a test fixture needs the path of the test binary");
        let metadata = fs::metadata(&path).expect("a test fixture needs the metadata of the test binary");
        (path, metadata.len(), metadata.modified().ok())
    })
}

/// Returns `true` if the file was last written more than [`MAX_ENTRY_AGE`] ago.
fn is_old(metadata: &fs::Metadata) -> bool {
    metadata.modified().ok().and_then(|at| at.elapsed().ok()).is_some_and(|age| age > MAX_ENTRY_AGE)
}

/// Removes the old files in `dir`. Each build of a test binary stores entries of its own, which
/// nothing else would remove.
fn remove_old_files(dir: &Path) {
    let Ok(entries) = fs::read_dir(dir) else { return };
    for entry in entries.flatten() {
        if entry.metadata().is_ok_and(|metadata| is_old(&metadata)) {
            let _ = fs::remove_file(entry.path());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::{sync::mpsc, thread};

    fn encode(value: &u64) -> Vec<u8> {
        value.to_le_bytes().to_vec()
    }

    fn decode(bytes: &[u8]) -> Option<u64> {
        Some(u64::from_le_bytes(bytes.try_into().ok()?))
    }

    /// A fixture name that no other run of `test` uses, whose files are removed when it is dropped.
    struct Name(String);

    impl Name {
        fn new(test: &str) -> Self {
            let nanos = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos();
            Self(format!("{test}-{}-{nanos}", std::process::id()))
        }
    }

    impl Drop for Name {
        fn drop(&mut self) {
            let dir = std::env::temp_dir().join("snarkos-test-fixtures");
            for entry in fs::read_dir(dir).unwrap().flatten() {
                if entry.file_name().to_string_lossy().starts_with(&self.0) {
                    let _ = fs::remove_file(entry.path());
                }
            }
        }
    }

    #[test]
    fn a_stored_fixture_is_returned_instead_of_a_new_one() {
        let name = Name::new("stored");
        assert_eq!(cached_fixture(&name.0, b"inputs", encode, decode, || 1), 1);
        assert_eq!(cached_fixture(&name.0, b"inputs", encode, decode, || 2), 1);
    }

    #[test]
    fn other_inputs_select_another_fixture() {
        let name = Name::new("inputs");
        assert_eq!(cached_fixture(&name.0, b"one", encode, decode, || 1), 1);
        assert_eq!(cached_fixture(&name.0, b"two", encode, decode, || 2), 2);
    }

    #[test]
    fn an_entry_that_does_not_decode_is_built_again() {
        let name = Name::new("undecodable");
        assert_eq!(cached_fixture(&name.0, b"", encode, decode, || 1), 1);
        assert_eq!(cached_fixture(&name.0, b"", encode, |_| None, || 2), 2);
        assert_eq!(cached_fixture(&name.0, b"", encode, decode, || 3), 2);
    }

    #[test]
    fn an_old_entry_is_built_again() {
        let name = Name::new("old");
        assert_eq!(cached_fixture(&name.0, b"", encode, decode, || 1), 1);

        let fixture = Fixture::open(&name.0, b"");
        let written_long_ago = SystemTime::now() - MAX_ENTRY_AGE - Duration::from_secs(60);
        File::options()
            .write(true)
            .open(fixture.path.as_ref().unwrap())
            .unwrap()
            .set_modified(written_long_ago)
            .unwrap();
        drop(fixture);

        assert_eq!(cached_fixture(&name.0, b"", encode, decode, || 2), 2);
    }

    #[test]
    fn a_second_holder_waits_for_the_first_to_store() {
        let name = Name::new("held");
        let first = Fixture::open(&name.0, b"");
        assert_eq!(first.load(decode), None);

        let (opening, opened) = mpsc::channel();
        let name_ = name.0.clone();
        let second = thread::spawn(move || {
            opening.send(()).unwrap();
            Fixture::open(&name_, b"").load(decode)
        });

        // The second holder is now opening the entry, and can only get it once the first one drops.
        opened.recv().unwrap();
        thread::sleep(Duration::from_millis(100));
        first.store(&encode(&7));
        drop(first);

        assert_eq!(second.join().unwrap(), Some(7));
    }
}

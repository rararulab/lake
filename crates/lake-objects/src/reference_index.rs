// Copyright 2026 Rararulab
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Bounded external sorting for retained managed-object references.

use std::{
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
};

use lake_common::{ObjectIdentity, ObjectReferenceDelta};
use uuid::Uuid;

use crate::{ObjectError, Result};

const MAX_RUN_CAPACITY: usize = 1_000_000;
const MAX_MERGE_FAN_IN: usize = 64;

/// Builds a globally URI-sorted live-reference index with bounded memory and
/// bounded open-file count.
#[derive(Clone, Debug)]
pub struct LiveReferenceIndexBuilder {
    work_root:    PathBuf,
    run_capacity: usize,
    merge_fan_in: usize,
}

impl LiveReferenceIndexBuilder {
    pub fn try_new(
        work_root: impl Into<PathBuf>,
        run_capacity: usize,
        merge_fan_in: usize,
    ) -> Result<Self> {
        if run_capacity == 0 || run_capacity > MAX_RUN_CAPACITY {
            return Err(ObjectError::InvalidGcConfig {
                message: format!("reference run capacity must be within 1..={MAX_RUN_CAPACITY}"),
            });
        }
        if !(2..=MAX_MERGE_FAN_IN).contains(&merge_fan_in) {
            return Err(ObjectError::InvalidGcConfig {
                message: format!("reference merge fan-in must be within 2..={MAX_MERGE_FAN_IN}"),
            });
        }
        Ok(Self {
            work_root: work_root.into(),
            run_capacity,
            merge_fan_in,
        })
    }

    pub fn build<I>(&self, deltas: I) -> Result<LiveReferenceIndex>
    where
        I: IntoIterator<Item = ObjectReferenceDelta>,
    {
        let mut build = self.begin()?;
        for delta in deltas {
            build.push_delta(delta)?;
        }
        build.finish()
    }

    pub fn begin(&self) -> Result<LiveReferenceIndexBuild> {
        create_dir_all(&self.work_root)?;
        let work_dir = self
            .work_root
            .join(format!("live-reference-index-{}", Uuid::new_v4()));
        create_dir_all(&work_dir)?;
        Ok(LiveReferenceIndexBuild {
            work_dir,
            run_capacity: self.run_capacity,
            merge_fan_in: self.merge_fan_in,
            buffer: Vec::with_capacity(self.run_capacity),
            runs: Vec::new(),
            sequence: 0,
            preserve: false,
        })
    }
}

pub struct LiveReferenceIndexBuild {
    work_dir:     PathBuf,
    run_capacity: usize,
    merge_fan_in: usize,
    buffer:       Vec<ObjectIdentity>,
    runs:         Vec<PathBuf>,
    sequence:     usize,
    preserve:     bool,
}

impl LiveReferenceIndexBuild {
    pub fn push_delta(&mut self, delta: ObjectReferenceDelta) -> Result<()> {
        if !delta.removed().is_empty() {
            return Err(ObjectError::GcReferenceRemovalsUnsupported);
        }
        for identity in delta.added() {
            self.buffer.push(identity.clone());
            if self.buffer.len() == self.run_capacity {
                self.flush_run()?;
            }
        }
        Ok(())
    }

    pub fn finish(mut self) -> Result<LiveReferenceIndex> {
        if !self.buffer.is_empty() {
            self.flush_run()?;
        }

        while self.runs.len() > 1 {
            let mut merged = Vec::with_capacity(self.runs.len().div_ceil(self.merge_fan_in));
            for group in self.runs.chunks(self.merge_fan_in) {
                let output = self
                    .work_dir
                    .join(format!("run-{:020}.jsonl", self.sequence));
                self.sequence += 1;
                merge_runs(group, &output)?;
                for input in group {
                    remove_file(input)?;
                }
                merged.push(output);
            }
            self.runs = merged;
        }

        let final_path = self.work_dir.join("live.jsonl");
        if let Some(run) = self.runs.pop() {
            rename(&run, &final_path)?;
        } else {
            create_file(&final_path)?;
        }
        self.preserve = true;
        Ok(LiveReferenceIndex { path: final_path })
    }

    fn flush_run(&mut self) -> Result<()> {
        self.runs
            .push(write_run(&self.work_dir, self.sequence, &mut self.buffer)?);
        self.sequence += 1;
        Ok(())
    }
}

impl Drop for LiveReferenceIndexBuild {
    fn drop(&mut self) {
        if !self.preserve {
            let _ = fs::remove_dir_all(&self.work_dir);
        }
    }
}

/// Durable, globally URI-sorted output of a completed reference-index build.
#[derive(Clone, Debug)]
pub struct LiveReferenceIndex {
    path: PathBuf,
}

impl LiveReferenceIndex {
    #[must_use]
    pub fn path(&self) -> &Path { &self.path }

    pub fn open(&self) -> Result<LiveReferenceIter> { LiveReferenceIter::open(&self.path) }
}

pub struct LiveReferenceIter {
    path:     PathBuf,
    lines:    std::io::Lines<BufReader<File>>,
    previous: Option<String>,
    done:     bool,
}

impl LiveReferenceIter {
    fn open(path: &Path) -> Result<Self> {
        let file = File::open(path).map_err(|source| index_io("opening", path, source))?;
        Ok(Self {
            path:     path.to_path_buf(),
            lines:    BufReader::new(file).lines(),
            previous: None,
            done:     false,
        })
    }
}

impl Iterator for LiveReferenceIter {
    type Item = Result<ObjectIdentity>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let line = match self.lines.next()? {
            Ok(line) => line,
            Err(source) => {
                self.done = true;
                return Some(Err(index_io("reading", &self.path, source)));
            }
        };
        let identity: ObjectIdentity = match serde_json::from_str(&line) {
            Ok(identity) => identity,
            Err(source) => {
                self.done = true;
                return Some(Err(ObjectError::GcReferenceIndexCorrupt {
                    path: self.path.clone(),
                    source,
                }));
            }
        };
        if self
            .previous
            .as_deref()
            .is_some_and(|previous| previous >= identity.uri.as_str())
        {
            self.done = true;
            return Some(Err(ObjectError::GcInputUnsorted {
                input: "reference index",
            }));
        }
        self.previous = Some(identity.uri.clone());
        Some(Ok(identity))
    }
}

struct RunReader {
    path:     PathBuf,
    lines:    std::io::Lines<BufReader<File>>,
    current:  Option<ObjectIdentity>,
    previous: Option<String>,
}

impl RunReader {
    fn open(path: &Path) -> Result<Self> {
        let file = File::open(path).map_err(|source| index_io("opening", path, source))?;
        let mut reader = Self {
            path:     path.to_path_buf(),
            lines:    BufReader::new(file).lines(),
            current:  None,
            previous: None,
        };
        reader.advance()?;
        Ok(reader)
    }

    fn advance(&mut self) -> Result<()> {
        let Some(line) = self.lines.next() else {
            self.current = None;
            return Ok(());
        };
        let line = line.map_err(|source| index_io("reading", &self.path, source))?;
        let identity: ObjectIdentity =
            serde_json::from_str(&line).map_err(|source| ObjectError::GcReferenceIndexCorrupt {
                path: self.path.clone(),
                source,
            })?;
        if self
            .previous
            .as_deref()
            .is_some_and(|previous| previous >= identity.uri.as_str())
        {
            return Err(ObjectError::GcInputUnsorted {
                input: "reference run",
            });
        }
        self.previous = Some(identity.uri.clone());
        self.current = Some(identity);
        Ok(())
    }
}

fn write_run(
    work_dir: &Path,
    sequence: usize,
    buffer: &mut Vec<ObjectIdentity>,
) -> Result<PathBuf> {
    buffer.sort();
    let path = work_dir.join(format!("run-{sequence:020}.jsonl"));
    let mut writer = writer(&path)?;
    let mut previous: Option<ObjectIdentity> = None;
    for identity in buffer.drain(..) {
        if let Some(prior) = &previous {
            if prior.uri == identity.uri {
                if prior != &identity {
                    return Err(ObjectError::GcIdentityConflict { uri: identity.uri });
                }
                continue;
            }
        }
        write_identity(&mut writer, &path, &identity)?;
        previous = Some(identity);
    }
    flush(writer, &path)?;
    Ok(path)
}

fn merge_runs(inputs: &[PathBuf], output: &Path) -> Result<()> {
    let mut readers = inputs
        .iter()
        .map(|path| RunReader::open(path))
        .collect::<Result<Vec<_>>>()?;
    let mut writer = writer(output)?;
    while let Some(uri) = readers
        .iter()
        .filter_map(|reader| {
            reader
                .current
                .as_ref()
                .map(|identity| identity.uri.as_str())
        })
        .min()
        .map(str::to_owned)
    {
        let mut selected: Option<ObjectIdentity> = None;
        for reader in &mut readers {
            if reader
                .current
                .as_ref()
                .is_some_and(|identity| identity.uri == uri)
            {
                let identity = reader.current.take().expect("current identity was checked");
                if selected
                    .as_ref()
                    .is_some_and(|previous| previous != &identity)
                {
                    return Err(ObjectError::GcIdentityConflict { uri });
                }
                selected = Some(identity);
                reader.advance()?;
            }
        }
        write_identity(
            &mut writer,
            output,
            selected.as_ref().expect("at least one run has the URI"),
        )?;
    }
    flush(writer, output)
}

fn writer(path: &Path) -> Result<BufWriter<File>> {
    File::create(path)
        .map(BufWriter::new)
        .map_err(|source| index_io("creating", path, source))
}

fn write_identity(writer: &mut BufWriter<File>, path: &Path, value: &ObjectIdentity) -> Result<()> {
    serde_json::to_writer(&mut *writer, value).map_err(|source| {
        ObjectError::GcReferenceIndexCorrupt {
            path: path.to_path_buf(),
            source,
        }
    })?;
    writer
        .write_all(b"\n")
        .map_err(|source| index_io("writing", path, source))
}

fn flush(mut writer: BufWriter<File>, path: &Path) -> Result<()> {
    writer
        .flush()
        .map_err(|source| index_io("flushing", path, source))?;
    writer
        .get_ref()
        .sync_all()
        .map_err(|source| index_io("syncing", path, source))
}

fn create_dir_all(path: &Path) -> Result<()> {
    fs::create_dir_all(path).map_err(|source| index_io("creating directory", path, source))
}

fn create_file(path: &Path) -> Result<()> {
    let file = File::create(path).map_err(|source| index_io("creating", path, source))?;
    file.sync_all()
        .map_err(|source| index_io("syncing", path, source))
}

fn remove_file(path: &Path) -> Result<()> {
    fs::remove_file(path).map_err(|source| index_io("removing", path, source))
}

fn rename(from: &Path, to: &Path) -> Result<()> {
    fs::rename(from, to).map_err(|source| index_io("renaming", from, source))
}

fn index_io(action: &'static str, path: &Path, source: std::io::Error) -> ObjectError {
    ObjectError::GcReferenceIndexIo {
        action,
        path: path.to_path_buf(),
        source,
    }
}

#[cfg(test)]
mod tests {
    use lake_common::{ObjectIdentity, ObjectReferenceDelta, Version};

    use super::*;

    fn object(uri: &str, sha256: &str) -> ObjectIdentity {
        ObjectIdentity {
            uri:          uri.to_owned(),
            content_type: "video/mp4".to_owned(),
            size_bytes:   42,
            sha256:       sha256.to_owned(),
        }
    }

    #[test]
    fn live_reference_index_merges_bounded_runs() {
        let temp = tempfile::tempdir().unwrap();
        let deltas = vec![
            ObjectReferenceDelta::try_new(
                Version(1),
                Version(2),
                vec![
                    object("s3://lake/objects/d", "dd"),
                    object("s3://lake/objects/b", "bb"),
                ],
                Vec::new(),
            )
            .unwrap(),
            ObjectReferenceDelta::try_new(
                Version(2),
                Version(3),
                vec![
                    object("s3://lake/objects/c", "cc"),
                    object("s3://lake/objects/a", "aa"),
                    object("s3://lake/objects/b", "bb"),
                ],
                Vec::new(),
            )
            .unwrap(),
        ];

        let index = LiveReferenceIndexBuilder::try_new(temp.path(), 2, 2)
            .unwrap()
            .build(deltas)
            .unwrap();
        let identities = index.open().unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(
            identities,
            vec![
                object("s3://lake/objects/a", "aa"),
                object("s3://lake/objects/b", "bb"),
                object("s3://lake/objects/c", "cc"),
                object("s3://lake/objects/d", "dd"),
            ]
        );

        let conflicting = vec![
            ObjectReferenceDelta::try_new(
                Version(1),
                Version(2),
                vec![object("s3://lake/objects/a", "aa")],
                Vec::new(),
            )
            .unwrap(),
            ObjectReferenceDelta::try_new(
                Version(2),
                Version(3),
                vec![object("s3://lake/objects/a", "different")],
                Vec::new(),
            )
            .unwrap(),
        ];
        assert!(matches!(
            LiveReferenceIndexBuilder::try_new(temp.path(), 1, 2)
                .unwrap()
                .build(conflicting),
            Err(crate::ObjectError::GcIdentityConflict { .. })
        ));

        let removed = ObjectReferenceDelta::try_new(
            Version(3),
            Version(4),
            Vec::new(),
            vec![object("s3://lake/objects/a", "aa")],
        )
        .unwrap();
        assert!(matches!(
            LiveReferenceIndexBuilder::try_new(temp.path(), 2, 2)
                .unwrap()
                .build(vec![removed]),
            Err(crate::ObjectError::GcReferenceRemovalsUnsupported)
        ));
    }
}

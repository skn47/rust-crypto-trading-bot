use crate::{
    config::Config,
    engine::{Engine, Sample},
    features,
    model::Model,
    types::{Event, MS},
};

fn write_then_apply<W: Write>(
    writer: &mut W,
    engine: &mut Engine,
    e: &Event,
) -> Result<Vec<crate::paper::Journal>> {
    json_line(writer, e)?;
    Ok(engine.on_event(e)?.1)
}
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::VecDeque,
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

pub fn parent(path: &Path) -> Result<()> {
    if let Some(p) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(p)?;
    }
    Ok(())
}
pub fn create(path: &Path) -> Result<File> {
    parent(path)?;
    Ok(OpenOptions::new().write(true).create_new(true).open(path)?)
}
pub fn json_line<W: Write, T: Serialize>(w: &mut W, value: &T) -> Result<()> {
    let mut bytes = serde_json::to_vec(value)?;
    bytes.push(b'\n');
    w.write_all(&bytes)?;
    Ok(())
}
pub fn atomic_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    parent(path)?;
    let temp = path.with_extension("tmp");
    let mut f = File::create(&temp)?;
    json_line(&mut f, value)?;
    f.sync_all()?;
    std::fs::rename(&temp, path)?;
    File::open(
        path.parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new(".")),
    )?
    .sync_all()?;
    Ok(())
}
pub fn hash(path: &Path) -> Result<String> {
    let mut f = File::open(path)?;
    let mut h = Sha256::new();
    let mut b = [0; 65536];
    loop {
        let n = f.read(&mut b)?;
        if n == 0 {
            break;
        }
        h.update(&b[..n]);
    }
    Ok(format!("{:x}", h.finalize()))
}
pub fn events(path: &Path) -> Result<impl Iterator<Item = Result<Event>>> {
    Ok(BufReader::new(File::open(path)?)
        .lines()
        .enumerate()
        .map(|(i, l)| serde_json::from_str(&l?).with_context(|| format!("event line {}", i + 1))))
}

pub fn dataset(input: &Path, output: &Path, config: Config, python: &str) -> Result<usize> {
    ensure!(!output.exists(), "dataset output already exists");
    let rows_path = output.with_extension("rows.jsonl");
    let mut rows = create(&rows_path)?;
    let mut engine = Engine::new(config.clone(), vec![])?;
    let mut pending: VecDeque<Sample> = VecDeque::new();
    let mut count = 0;
    for event in events(input)? {
        let e = event?;
        while pending
            .front()
            .is_some_and(|s| s.recv_ns + config.horizon_ms * MS < e.recv_ns)
        {
            let s = pending.pop_front().unwrap();
            let end = s.recv_ns + config.horizon_ms * MS;
            if s.epoch == engine.epoch && engine.valid_at(end) {
                for i in 0..2 {
                    let label = 10000.0 * (engine.books[i].mid().unwrap() / s.mids[i]).ln();
                    json_line(
                        &mut rows,
                        &serde_json::json!({"symbol":config.symbols[i],"recv_ns":s.recv_ns,"label_end_ns":end,"seq":s.seq,"epoch":s.epoch,"features":s.features,"mid":s.mids[i],"spread_bps":s.spreads_bps[i],"target_bps":label}),
                    )?;
                    count += 1;
                }
            }
        }
        let (sample, _) = engine.on_event(&e)?;
        if let Some(s) = sample {
            pending.push_back(s);
        }
    }
    // No extrapolation beyond the final recorded event.
    rows.sync_all()?;
    drop(rows);
    ensure!(
        count > 0,
        "no valid labels; collect at least 7 seconds of continuous books"
    );
    let manifest = output.with_extension("manifest.json");
    atomic_json(
        &manifest,
        &serde_json::json!({"version":1,"features":features::names(&config.symbols),"horizon_ms":1000,"source_sha256":hash(input)?,"rows":count,"config":config}),
    )?;
    let status = std::process::Command::new(python)
        .args(["-m", "research.pipeline", "parquet", "--rows"])
        .arg(&rows_path)
        .arg("--output")
        .arg(output)
        .status()?;
    ensure!(
        status.success(),
        "Parquet conversion failed; retained {}",
        rows_path.display()
    );
    std::fs::remove_file(rows_path)?;
    Ok(count)
}

#[derive(Serialize, Deserialize)]
struct Checkpoint {
    version: u32,
    engine: Engine,
    log_offset: u64,
    journal_offset: u64,
}
pub struct LiveStore {
    pub engine: Engine,
    log: File,
    journal: File,
    checkpoint: PathBuf,
    pub last_checkpoint_ns: u64,
    checkpoint_job: Option<std::thread::JoinHandle<Result<()>>>,
}
impl LiveStore {
    pub fn open(log_path: &Path, config: Config, models: Vec<Model>, resume: bool) -> Result<Self> {
        let checkpoint = log_path.with_extension("checkpoint.json");
        let journal_path = log_path.with_extension("journal.jsonl");
        if !resume {
            ensure!(
                !checkpoint.exists() && !journal_path.exists(),
                "existing run artifacts; use a new log path or --resume"
            );
            let s = Self {
                engine: Engine::new(config, models)?,
                log: create(log_path)?,
                journal: create(&journal_path)?,
                checkpoint,
                last_checkpoint_ns: 0,
                checkpoint_job: None,
            };
            s.log.try_lock()?;
            return Ok(s);
        }
        let mut log = OpenOptions::new().read(true).write(true).open(log_path)?;
        log.try_lock()?;
        let cp: Checkpoint = serde_json::from_reader(File::open(&checkpoint)?)?;
        ensure!(cp.version == 1, "checkpoint version mismatch");
        ensure!(
            serde_json::to_value(&cp.engine.config)? == serde_json::to_value(config)?
                && serde_json::to_value(&cp.engine.models)? == serde_json::to_value(models)?,
            "resume requires identical config and models"
        );
        let mut journal = OpenOptions::new()
            .read(true)
            .write(true)
            .open(journal_path)?;
        ensure!(
            log.metadata()?.len() >= cp.log_offset
                && journal.metadata()?.len() >= cp.journal_offset,
            "checkpoint exceeds durable files"
        );
        journal.set_len(cp.journal_offset)?;
        journal.seek(SeekFrom::End(0))?;
        log.seek(SeekFrom::Start(cp.log_offset))?;
        let mut reader = BufReader::new(log.try_clone()?);
        let mut engine = cp.engine;
        let mut offset = cp.log_offset;
        loop {
            let mut line = String::new();
            let n = reader.read_line(&mut line)?;
            if n == 0 {
                break;
            }
            if !line.ends_with('\n') {
                break;
            }
            let e: Event = serde_json::from_str(&line)?;
            let (_, records) = engine.on_event(&e)?;
            for r in records {
                json_line(&mut journal, &r)?;
            }
            offset += n as u64;
        }
        drop(reader);
        log.set_len(offset)?;
        log.seek(SeekFrom::End(0))?;
        let mut s = Self {
            engine,
            log,
            journal,
            checkpoint,
            last_checkpoint_ns: 0,
            checkpoint_job: None,
        };
        s.checkpoint()?;
        Ok(s)
    }
    pub fn push(&mut self, e: &Event) -> Result<()> {
        if self
            .checkpoint_job
            .as_ref()
            .is_some_and(|job| job.is_finished())
        {
            self.finish_checkpoint()?;
        }
        // Write-ahead log: a write failure prevents this event from producing a decision.
        let rows = write_then_apply(&mut self.log, &mut self.engine, e)?;
        for r in rows {
            json_line(&mut self.journal, &r)?;
        }
        Ok(())
    }
    pub fn checkpoint(&mut self) -> Result<()> {
        self.finish_checkpoint()?;
        self.log.sync_data()?;
        self.journal.sync_data()?;
        let cp = Checkpoint {
            version: 1,
            engine: self.engine.clone(),
            log_offset: self.log.stream_position()?,
            journal_offset: self.journal.stream_position()?,
        };
        atomic_json(&self.checkpoint, &cp)?;
        self.last_checkpoint_ns = self.engine.now;
        Ok(())
    }
    fn finish_checkpoint(&mut self) -> Result<()> {
        if let Some(job) = self.checkpoint_job.take() {
            job.join()
                .map_err(|_| anyhow::anyhow!("checkpoint worker panicked"))??;
        }
        Ok(())
    }
    pub fn checkpoint_background(&mut self) -> Result<()> {
        if self
            .checkpoint_job
            .as_ref()
            .is_some_and(|job| job.is_finished())
        {
            self.finish_checkpoint()?;
        }
        if self.checkpoint_job.is_some() {
            return Ok(());
        }
        let cp = Checkpoint {
            version: 1,
            engine: self.engine.clone(),
            log_offset: self.log.stream_position()?,
            journal_offset: self.journal.stream_position()?,
        };
        let log = self.log.try_clone()?;
        let journal = self.journal.try_clone()?;
        let path = self.checkpoint.clone();
        self.checkpoint_job = Some(std::thread::spawn(move || {
            log.sync_data()?;
            journal.sync_data()?;
            atomic_json(&path, &cp)
        }));
        self.last_checkpoint_ns = self.engine.now;
        Ok(())
    }
}
impl Drop for LiveStore {
    fn drop(&mut self) {
        let _ = self.finish_checkpoint();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    struct FailedDisk;
    impl Write for FailedDisk {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            Err(std::io::Error::other("disk full"))
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    #[test]
    fn failed_wal_write_never_advances_engine() {
        let config: Config = toml::from_str(include_str!("../config/default.toml")).unwrap();
        let mut engine = Engine::new(config, vec![]).unwrap();
        let e = Event {
            version: 1,
            session: "test".into(),
            seq: 1,
            recv_ns: 1,
            utc_ns: 1,
            exchange_ns: None,
            raw: None,
            kind: crate::types::Kind::Timer,
        };
        assert!(write_then_apply(&mut FailedDisk, &mut engine, &e).is_err());
        assert_eq!(engine.last_seq, 0);
        assert_eq!(engine.now, 0);
    }
}

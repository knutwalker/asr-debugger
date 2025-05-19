#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")] // hide console window on Windows in release

use std::{
    fs,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, AtomicUsize},
        Arc, Mutex, RwLock,
    },
    thread,
    time::{Duration, Instant, SystemTime},
};

use anyhow::Context;
use arc_swap::ArcSwapOption;
use atomic::Atomic;
use clap::Parser;
use hdrhistogram::Histogram;
use indexmap::IndexMap;
use livesplit_auto_splitting::{
    settings, time, wasi_path, AutoSplitter, CompiledAutoSplitter, Config, ExecutionGuard,
    LogLevel, Runtime, Timer, TimerState,
};
use time::UtcOffset;

#[derive(Parser)]
struct Args {
    #[arg(short, long)]
    debug: bool,
    wasm_path: PathBuf,
}

fn main() {
    let time_zone = UtcOffset::current_local_offset().unwrap_or(UtcOffset::UTC);

    let args = Args::parse();

    let shared_state = Arc::new(SharedState {
        auto_splitter: ArcSwapOption::new(None),
        memory_usage: AtomicUsize::new(0),
        handles: AtomicU64::new(0),
        tick_rate: Mutex::new(std::time::Duration::ZERO),
        slowest_tick: Mutex::new(std::time::Duration::ZERO),
        avg_tick_secs: Atomic::new(0.0),
        tick_times: Mutex::new(Histogram::new(1).unwrap()),
    });
    let timer = DebuggerTimer::new(time_zone);

    thread::Builder::new()
        .name("Auto Splitter Thread".into())
        .spawn({
            let timer = timer.clone();
            let shared_state = shared_state.clone();
            move || runtime_thread(shared_state, timer.clone())
        })
        .unwrap();

    let mut options = eframe::NativeOptions::default();
    options.viewport.inner_size = Some((1250.0, 800.0).into());

    let optimize = !args.debug;
    let mut state = AppState {
        path: None,
        script_path: None,
        module_modified_time: None,
        module: None,
        shared_state,
        timer,
        runtime: build_runtime(optimize),
    };

    state.load(Load::File(args.wasm_path));

    // let settings_map = self
    //     .state
    //     .shared_state
    //     .auto_splitter
    //     .load()
    //     .as_ref()
    //     .map(|r| r.settings_map());
    // let old = runtime.settings_map();
    // let mut new = old.clone();
    // new.insert(
    //     key.clone(),
    //     settings::Value::String(s.as_ref().into()),
    // );
    // if runtime.set_settings_map_if_unchanged(&old, new) {
    //     break;
    // }
}

struct SharedState {
    auto_splitter: ArcSwapOption<AutoSplitter<DebuggerTimer>>,
    tick_rate: Mutex<std::time::Duration>,
    slowest_tick: Mutex<std::time::Duration>,
    memory_usage: AtomicUsize,
    handles: AtomicU64,
    avg_tick_secs: Atomic<f64>,
    tick_times: Mutex<Histogram<u64>>,
}

impl SharedState {
    fn kill_auto_splitter_if_it_doesnt_react(&self) {
        let Some(auto_splitter) = &*self.auto_splitter.load() else {
            return;
        };
        if Self::try_lock(auto_splitter).is_none() {
            auto_splitter.interrupt_handle().interrupt();
        }
    }

    fn try_lock(
        auto_splitter: &AutoSplitter<DebuggerTimer>,
    ) -> Option<ExecutionGuard<'_, DebuggerTimer>> {
        for _ in 0..100 {
            if let Some(guard) = auto_splitter.try_lock() {
                return Some(guard);
            }
            thread::sleep(Duration::from_millis(1));
        }

        None
    }
}

fn runtime_thread(shared_state: Arc<SharedState>, timer: DebuggerTimer) {
    let mut next_tick = Instant::now();
    loop {
        let tick_rate = {
            if let Some(auto_splitter) = &*shared_state.auto_splitter.load() {
                let mut auto_splitter_lock = auto_splitter.lock();
                let now = Instant::now();
                let res = auto_splitter_lock.update();
                let time_of_tick = now.elapsed();
                let memory_usage = auto_splitter_lock.memory().len();
                // {
                //     let mut processes = shared_state.processes.lock().unwrap();
                //     processes.clear();
                //     auto_splitter_lock.attached_processes().for_each(|process| {
                //         use std::fmt::Write;
                //         let element = processes.push();
                //         let _ = write!(element.pid, "{}", process.pid());
                //         element
                //             .path
                //             .push_str(process.path().unwrap_or("Unnamed Process"));
                //     });
                // }
                let handles = auto_splitter_lock.handles();
                drop(auto_splitter_lock);

                shared_state
                    .memory_usage
                    .store(memory_usage, atomic::Ordering::Relaxed);
                shared_state
                    .handles
                    .store(handles, atomic::Ordering::Relaxed);

                {
                    let mut slowest_tick = shared_state.slowest_tick.lock().unwrap();
                    if time_of_tick > *slowest_tick {
                        *slowest_tick = time_of_tick;
                    }
                }

                *shared_state.tick_rate.lock().unwrap() = auto_splitter.tick_rate();
                *shared_state.tick_times.lock().unwrap() += time_of_tick.as_nanos() as u64;
                shared_state.avg_tick_secs.store(
                    0.999 * shared_state.avg_tick_secs.load(atomic::Ordering::Relaxed)
                        + 0.001 * time_of_tick.as_secs_f64(),
                    atomic::Ordering::Relaxed,
                );
                if let Err(e) = res {
                    timer.0.write().unwrap().log(
                        format!("{:?}", e.context("Failed executing the auto splitter.")).into(),
                        LogType::Runtime(LogLevel::Error),
                    )
                };
                auto_splitter.tick_rate()
            } else {
                // shared_state.processes.lock().unwrap().clear();

                // Tick at 10 Hz when no runtime is loaded.
                std::time::Duration::from_secs(1) / 10
            }
        };
        next_tick += tick_rate;

        let now = Instant::now();
        if let Some(sleep_time) = next_tick.checked_duration_since(now) {
            thread::sleep(sleep_time);
        } else {
            // In this case we missed the next tick already. This likely comes
            // up when the operating system was suspended for a while. Instead
            // of trying to catch up, we just reset the next tick to start from
            // now.
            next_tick = now;
        }
    }
}

struct AppState {
    path: Option<PathBuf>,
    script_path: Option<PathBuf>,
    module_modified_time: Option<SystemTime>,
    module: Option<CompiledAutoSplitter>,
    shared_state: Arc<SharedState>,
    timer: DebuggerTimer,
    runtime: livesplit_auto_splitting::Runtime,
}

enum Load {
    File(PathBuf),
}

impl AppState {
    fn load(&mut self, load: Load) {
        let settings_map = if let Load::File(path) = &load {
            self.path = Some(path.clone());
            None
        } else {
            self.shared_state
                .auto_splitter
                .load()
                .as_ref()
                .map(|r| r.settings_map())
        };

        let mut succeeded = true;

        if let (Load::File(_), Some(path)) = (&load, &self.path) {
            self.module = match fs::read(path)
                .context("Failed loading the auto splitter from the file system.")
                .and_then(|data| {
                    self.runtime
                        .compile(&data)
                        .context("Failed loading the auto splitter.")
                }) {
                Ok(module) => Some(module),
                Err(e) => {
                    succeeded = false;
                    self.timer
                        .0
                        .write()
                        .unwrap()
                        .log(format!("{e:?}").into(), LogType::Runtime(LogLevel::Error));
                    None
                }
            };
            self.module_modified_time = fs::metadata(path).ok().and_then(|m| m.modified().ok());
        }

        let new_auto_splitter = if let Some(module) = &self.module {
            match module
                .instantiate(
                    self.timer.clone(),
                    settings_map,
                    self.script_path.as_deref(),
                )
                .context("Failed starting the auto splitter.")
            {
                Ok(r) => Some(Arc::new(r)),
                Err(e) => {
                    succeeded = false;
                    self.timer
                        .0
                        .write()
                        .unwrap()
                        .log(format!("{e:?}").into(), LogType::Runtime(LogLevel::Error));
                    None
                }
            }
        } else {
            None
        };

        self.shared_state.kill_auto_splitter_if_it_doesnt_react();
        self.shared_state.auto_splitter.store(new_auto_splitter);

        *self.shared_state.slowest_tick.lock().unwrap() = std::time::Duration::ZERO;
        self.shared_state
            .avg_tick_secs
            .store(0.0, atomic::Ordering::Relaxed);
        self.shared_state.tick_times.lock().unwrap().clear();

        let mut timer = self.timer.0.write().unwrap();
        if let Load::File(_) = &load {
            timer.clear();
        }
        timer.variables.clear();

        if succeeded {
            timer.log(
                match load {
                    Load::File(_) => "Auto splitter loaded.",
                }
                .into(),
                LogType::Runtime(LogLevel::Info),
            );
        }
    }
}

fn build_runtime(optimize: bool) -> Runtime {
    let mut config = Config::default();
    config.debug_info = true;
    config.optimize = optimize;
    Runtime::new(config).unwrap()
}

const SECONDS_PER_MINUTE: u64 = 60;
const SECONDS_PER_HOUR: u64 = 60 * SECONDS_PER_MINUTE;

fn fmt_duration(time: time::Duration) -> String {
    let nanoseconds = time.subsec_nanoseconds();
    let total_seconds = time.whole_seconds();
    let (minus, total_seconds, nanoseconds) = if (total_seconds | nanoseconds as i64) < 0 {
        ("-", (-total_seconds) as u64, (-nanoseconds) as u32)
    } else {
        ("", total_seconds as u64, nanoseconds as u32)
    };
    let seconds = (total_seconds % SECONDS_PER_MINUTE) as u8;
    let minutes = ((total_seconds % SECONDS_PER_HOUR) / SECONDS_PER_MINUTE) as u8;
    let hours = total_seconds / SECONDS_PER_HOUR;
    if hours != 0 {
        format!("{minus}{hours}:{minutes:02}:{seconds:02}.{nanoseconds:09}")
    } else {
        format!("{minus}{minutes}:{seconds:02}.{nanoseconds:09}")
    }
}

fn timer_state_to_str(state: TimerState) -> &'static str {
    match state {
        TimerState::NotRunning => "Not running",
        TimerState::Running => "Running",
        TimerState::Paused => "Paused",
        TimerState::Ended => "Ended",
    }
}

enum LogType {
    Runtime(LogLevel),
    AutoSplitterMessage,
}

struct DebuggerTimerState {
    timer_state: TimerState,
    game_time: time::Duration,
    game_time_state: GameTimeState,
    split_index: usize,
    variables: IndexMap<Box<str>, String>,
    time_zone: UtcOffset,
    logs: Vec<LogMessage>,
}

impl DebuggerTimerState {
    fn new(time_zone: UtcOffset) -> Self {
        Self {
            timer_state: Default::default(),
            game_time: Default::default(),
            game_time_state: Default::default(),
            split_index: Default::default(),
            variables: Default::default(),
            time_zone,
            logs: Default::default(),
        }
    }

    fn log(&mut self, message: Box<str>, ty: LogType) {
        let (h, m, s) = time::OffsetDateTime::now_utc()
            .to_offset(self.time_zone)
            .time()
            .as_hms();
        self.logs.push(LogMessage {
            time: format!("{h:02}:{m:02}:{s:02}").into(),
            message,
            ty,
        });
    }
}

struct LogMessage {
    time: Box<str>,
    message: Box<str>,
    ty: LogType,
}

#[derive(Copy, Clone, Default, PartialEq)]
enum GameTimeState {
    #[default]
    NotInitialized,
    Paused,
    Running,
}

impl GameTimeState {
    fn to_str(self) -> &'static str {
        match self {
            GameTimeState::NotInitialized => "Not initialized",
            GameTimeState::Paused => "Paused",
            GameTimeState::Running => "Running",
        }
    }
}

#[derive(Clone)]
struct DebuggerTimer(Arc<RwLock<DebuggerTimerState>>);

impl DebuggerTimer {
    fn new(time_zone: UtcOffset) -> Self {
        Self(Arc::new(RwLock::new(DebuggerTimerState::new(time_zone))))
    }
}

impl Timer for DebuggerTimer {
    fn state(&self) -> TimerState {
        self.0.read().unwrap().timer_state
    }

    fn start(&mut self) {
        let mut state = self.0.write().unwrap();
        if state.timer_state == TimerState::NotRunning {
            state.start();
            state.log("Timer started.".into(), LogType::Runtime(LogLevel::Debug));
        }
    }

    fn split(&mut self) {
        let mut state = self.0.write().unwrap();
        if state.timer_state == TimerState::Running {
            state.split_index += 1;
            state.log("Splitted.".into(), LogType::Runtime(LogLevel::Debug));
        }
    }

    fn skip_split(&mut self) {
        let mut state = self.0.write().unwrap();
        if state.timer_state == TimerState::Running {
            state.split_index += 1;
            state.log("Split skipped.".into(), LogType::Runtime(LogLevel::Debug));
        }
    }

    fn undo_split(&mut self) {
        let mut state = self.0.write().unwrap();
        if state.timer_state == TimerState::Ended {
            state.timer_state = TimerState::Running;
        }
        if state.timer_state == TimerState::Running {
            state.split_index = state.split_index.saturating_sub(1);
            state.log("Split undone.".into(), LogType::Runtime(LogLevel::Debug));
        }
    }

    fn reset(&mut self) {
        let mut state = self.0.write().unwrap();
        state.reset();
        state.log("Run reset.".into(), LogType::Runtime(LogLevel::Debug));
    }

    fn set_game_time(&mut self, time: time::Duration) {
        let mut state = self.0.write().unwrap();
        state.game_time = time;
        if state.game_time_state == GameTimeState::NotInitialized {
            state.game_time_state = GameTimeState::Running;
        }
    }

    fn pause_game_time(&mut self) {
        self.0.write().unwrap().game_time_state = GameTimeState::Paused;
    }

    fn resume_game_time(&mut self) {
        self.0.write().unwrap().game_time_state = GameTimeState::Running;
    }

    fn set_variable(&mut self, key: &str, value: &str) {
        let mut guard = self.0.write().unwrap();
        let s = guard.variables.entry(key.into()).or_default();
        s.clear();
        s.push_str(value);
    }

    fn log_auto_splitter(&mut self, message: std::fmt::Arguments<'_>) {
        self.0.write().unwrap().log(
            match message.as_str() {
                Some(m) => m.into(),
                None => message.to_string().into(),
            },
            LogType::AutoSplitterMessage,
        );
    }

    fn log_runtime(&mut self, message: std::fmt::Arguments<'_>, log_level: LogLevel) {
        self.0.write().unwrap().log(
            match message.as_str() {
                Some(m) => m.into(),
                None => message.to_string().into(),
            },
            LogType::Runtime(log_level),
        );
    }
}

impl DebuggerTimerState {
    fn start(&mut self) {
        if self.timer_state == TimerState::NotRunning {
            self.timer_state = TimerState::Running;
        }
    }

    fn reset(&mut self) {
        self.timer_state = TimerState::NotRunning;
        self.split_index = 0;
        self.game_time = time::Duration::ZERO;
        self.game_time_state = GameTimeState::NotInitialized;
        self.variables.clear();
    }

    fn clear(&mut self) {
        self.reset();
    }
}

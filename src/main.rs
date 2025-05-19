#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")] // hide console window on Windows in release

use std::{
    fs,
    net::{TcpListener, TcpStream},
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex, RwLock,
    },
    thread,
    time::{Duration, Instant, SystemTime},
};

use anyhow::{Context, Result};
use clap::Parser;
use indexmap::IndexMap;
use livesplit_auto_splitting::{
    settings, time, wasi_path, AutoSplitter, CompiledAutoSplitter, Config, ExecutionGuard,
    LogLevel, Runtime, Timer, TimerState,
};
use time::UtcOffset;
use tungstenite::{Message, Utf8Bytes, WebSocket};

#[derive(Parser)]
struct Args {
    #[arg(short, long)]
    debug: bool,
    wasm_path: PathBuf,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let server = TcpListener::bind("127.0.0.1:9001").unwrap();
    for stream in server.incoming() {
        let stream = stream?;
        let path = args.wasm_path.clone();
        std::thread::spawn(move || -> Result<()> {
            let mut websocket = tungstenite::accept(stream)?;

            websocket.send(Messages::get_current_state())?;
            let state = websocket.read()?;
            eprintln!("state = {state:?}");

            // let time_zone = UtcOffset::current_local_offset().unwrap_or(UtcOffset::UTC);
            let timer = WebsocketTimer(websocket);

            let state = AppState::new(path, timer)?;
            let shared_state = Arc::clone(&state.shared_state);

            runtime_thread(shared_state);

            // thread::Builder::new()
            //     .name("Auto Splitter Thread".into())
            //     .spawn({
            //         let shared_state = shared_state.clone();
            //         move || runtime_thread(shared_state, timer)
            //     })
            //     .unwrap();

            // loop {
            //     let msg = websocket.read().unwrap();
            //     match msg {
            //         Message::Text(msg) => {
            //             // let msg =
            //             //     serde_json::from_str::<livesplit_core::event::Event>(msg.as_str())
            //             //         .unwrap();
            //             eprintln!("received: {:?}", msg.as_str());
            //         }
            //         Message::Binary(bytes) => todo!(),
            //         Message::Ping(bytes) => {
            //             websocket.send(Message::Pong(bytes)).unwrap();
            //         }
            //         Message::Pong(bytes) => todo!(),
            //         Message::Close(close_frame) => todo!(),
            //         Message::Frame(frame) => todo!(),
            //     }
            // }

            Ok(())
        });
    }

    // let time_zone = UtcOffset::current_local_offset().unwrap_or(UtcOffset::UTC);
    // let timer = DebuggerTimer::new(time_zone);
    //
    // let state = AppState::new(args.wasm_path, timer.clone())?;
    // let shared_state = Arc::clone(&state.shared_state);
    //
    // thread::Builder::new()
    //     .name("Auto Splitter Thread".into())
    //     .spawn({
    //         let shared_state = shared_state.clone();
    //         move || runtime_thread(shared_state, timer)
    //     })
    //     .unwrap();

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

    Ok(())
}

struct SharedState {
    auto_splitter: AutoSplitter<WebsocketTimer>,
    tick_rate: Mutex<std::time::Duration>,
    memory_usage: AtomicUsize,
}

impl SharedState {
    fn kill_auto_splitter_if_it_doesnt_react(&self) {
        let auto_splitter = &self.auto_splitter;
        if Self::try_lock(&self.auto_splitter).is_none() {
            auto_splitter.interrupt_handle().interrupt();
        }
    }

    fn try_lock(
        auto_splitter: &AutoSplitter<WebsocketTimer>,
    ) -> Option<ExecutionGuard<'_, WebsocketTimer>> {
        for _ in 0..100 {
            if let Some(guard) = auto_splitter.try_lock() {
                return Some(guard);
            }
            thread::sleep(Duration::from_millis(1));
        }

        None
    }
}

fn runtime_thread(shared_state: Arc<SharedState>) {
    let mut next_tick = Instant::now();
    loop {
        let auto_splitter = &shared_state.auto_splitter;
        let mut auto_splitter_lock = auto_splitter.lock();
        // does the actual work
        let res = auto_splitter_lock.update();
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

        drop(auto_splitter_lock);

        shared_state
            .memory_usage
            .store(memory_usage, Ordering::Relaxed);

        let tick_rate = auto_splitter.tick_rate();
        *shared_state.tick_rate.lock().unwrap() = tick_rate;

        if let Err(e) = res {
            eprintln!("{:?}", e.context("Failed executing the auto splitter."));
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
    path: PathBuf,
    module_modified_time: Option<SystemTime>,
    module: CompiledAutoSplitter,
    shared_state: Arc<SharedState>,
    // timer: DebuggerTimer,
    runtime: livesplit_auto_splitting::Runtime,
}

enum Load {
    File(PathBuf),
}

impl AppState {
    fn new(path: PathBuf, timer: WebsocketTimer) -> anyhow::Result<Self> {
        // let optimize = !args.debug;
        // let mut state = AppState {
        //     path: None,
        //     script_path: None,
        //     module_modified_time: None,
        //     module: None,
        //     shared_state,
        //     timer,
        //     runtime: build_runtime(optimize),
        // };
        //
        // state.load(Load::File(args.wasm_path));

        let settings_map = None;
        // let settings_map = if let Load::File(path) = &load {
        // } else {
        //     self.shared_state
        //         .auto_splitter
        //         .load()
        //         .as_ref()
        //         .map(|r| r.settings_map())
        // };

        let runtime = build_runtime(true);

        let module = fs::read(&path)
            .context("Failed loading the auto splitter from the file system.")
            .and_then(|data| {
                runtime
                    .compile(&data)
                    .context("Failed loading the auto splitter.")
            })?;
        let module_modified_time = fs::metadata(&path).ok().and_then(|m| m.modified().ok());

        let new_auto_splitter = module
            .instantiate(timer, settings_map, None)
            .context("Failed starting the auto splitter.")?;

        let shared_state = Arc::new(SharedState {
            auto_splitter: new_auto_splitter,
            memory_usage: AtomicUsize::new(0),
            tick_rate: Mutex::new(std::time::Duration::ZERO),
        });

        // let mut inner_timer = timer.0.write().unwrap();
        // // if let Load::File(_) = &load {
        // inner_timer.clear();
        // // }
        // inner_timer.variables.clear();
        // drop(inner_timer);

        Ok(AppState {
            path,
            module_modified_time,
            module,
            shared_state,
            // timer,
            runtime,
        })
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

/// A WebSocket echo server
fn wsmain() {
    let server = TcpListener::bind("127.0.0.1:9001").unwrap();
    for stream in server.incoming() {
        std::thread::spawn(move || {
            let mut websocket = tungstenite::accept(stream.unwrap()).unwrap();

            websocket.send(Messages::get_current_state()).unwrap();

            loop {
                let msg = websocket.read().unwrap();
                match msg {
                    Message::Text(msg) => {
                        // let msg =
                        //     serde_json::from_str::<livesplit_core::event::Event>(msg.as_str())
                        //         .unwrap();
                        eprintln!("received: {:?}", msg.as_str());
                    }
                    Message::Binary(bytes) => todo!(),
                    Message::Ping(bytes) => {
                        websocket.send(Message::Pong(bytes)).unwrap();
                    }
                    Message::Pong(bytes) => todo!(),
                    Message::Close(close_frame) => todo!(),
                    Message::Frame(frame) => todo!(),
                }
            }
        });
    }
}

macro_rules! cmd {
    ($command:literal) => {
        Utf8Bytes::from_static(concat!("{\"command\":\"", $command, "\"}"))
    };
}

struct Messages {}

impl Messages {
    const START: Utf8Bytes = cmd!("start");
    const SPLIT: Utf8Bytes = cmd!("split");
    const PING: Utf8Bytes = cmd!("ping");
    const GET_CURRENT_STATE: Utf8Bytes = cmd!("getCurrentState");

    fn start() -> Message {
        Message::Text(Self::START)
    }

    fn split() -> Message {
        Message::Text(Self::SPLIT)
    }

    fn ping() -> Message {
        Message::Text(Self::PING)
    }

    fn get_current_state() -> Message {
        Message::Text(Self::GET_CURRENT_STATE)
    }

    fn set_game_time(time: time::Duration) -> Message {
        Message::text(format!(
            "{{\"command\":\"setGameTime\",\"time\":\"{}\"}}",
            time.whole_seconds()
        ))
    }
}

struct WebsocketTimer(WebSocket<TcpStream>);

impl Timer for WebsocketTimer {
    fn state(&self) -> TimerState {
        // self.0.send(Messages::get_current_state()).unwrap();
        // let res = self.0.read().unwrap();
        // eprintln!("{:?}", res);
        todo!()
    }

    fn start(&mut self) {
        self.0.send(Messages::start()).unwrap();
    }

    fn split(&mut self) {
        self.0.send(Messages::split()).unwrap();
    }

    fn skip_split(&mut self) {
        todo!()
    }

    fn undo_split(&mut self) {
        todo!()
    }

    fn reset(&mut self) {
        todo!()
    }

    fn set_game_time(&mut self, time: time::Duration) {
        self.0.send(Messages::set_game_time(time)).unwrap();
    }

    fn pause_game_time(&mut self) {
        todo!()
    }

    fn resume_game_time(&mut self) {
        todo!()
    }

    fn set_variable(&mut self, key: &str, value: &str) {
        todo!()
    }

    fn log_auto_splitter(&mut self, message: std::fmt::Arguments<'_>) {
        todo!()
    }

    fn log_runtime(&mut self, message: std::fmt::Arguments<'_>, log_level: LogLevel) {
        todo!()
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

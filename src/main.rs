#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")] // hide console window on Windows in release

use std::{
    fs,
    net::{IpAddr, TcpListener, TcpStream},
    ops::ControlFlow,
    path::{Path, PathBuf},
    sync::{
        mpsc::{self, Receiver, Sender},
        Arc, RwLock,
    },
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use clap::Parser;
use livesplit_auto_splitting::{
    settings, time, AutoSplitter, Config, LogLevel, Runtime, Timer, TimerState,
};
use livesplit_core::event;
use log::{debug, error, info, trace};
use tungstenite::{Message, Utf8Bytes, WebSocket};

#[derive(Parser, Debug)]
#[command(about, long_about = None, arg_required_else_help(true))]
struct Args {
    /// Path to a settings file for the autosplitter (toml)
    #[arg(short, long)]
    settings: Option<PathBuf>,

    /// Websocket port
    #[arg(short, long, default_value_t = 9087)]
    port: u16,

    /// Websocket host
    #[arg(short = 'H', long, default_value = "0.0.0.0")]
    host: IpAddr,

    /// Path to the autosplitter wasm file
    wasm_path: PathBuf,
}

fn main() -> Result<()> {
    pretty_env_logger::init();

    let args = Args::parse();
    debug!("Args: {:?}", args);

    let server = TcpListener::bind((args.host, args.port))?;
    info!("Listening on {:?}", server.local_addr());

    for (counter, stream) in server.incoming().enumerate() {
        let stream = stream?;

        info!("Accepting connection from {:?}", stream.peer_addr());
        let ws = tungstenite::accept(stream)?;

        let timer_state = Arc::new(RwLock::new(TimerState::NotRunning));

        let (mut ws, tx) = WsThread::new(ws, Arc::clone(&timer_state));
        let _ = ws.handle(WsCommand::GetCurrentState(TimerState::NotRunning));

        let timer_state = CurrentTimerState::new(tx.clone(), timer_state);
        let timer = WebsocketTimer::new(timer_state.clone(), tx);
        let state = SplitterThread::new(
            &args.wasm_path,
            args.settings.as_deref(),
            timer,
            timer_state,
        )?;

        let ws = thread::Builder::new()
            .name(format!("Websocket Handler {counter}"))
            .spawn(move || ws.run())
            .unwrap();

        let state = thread::Builder::new()
            .name(format!("Auto Splitter Runtime {counter}"))
            .spawn(move || state.run())
            .unwrap();
    }

    Ok(())
}

struct WsThread {
    ws: WebSocket<TcpStream>,
    rx: Receiver<WsCommand>,
    timer_state: Arc<RwLock<TimerState>>,
}

#[derive(Debug, Clone)]
enum WsCommand {
    Start,
    Split,
    Reset,
    UndoSplit,
    SkipSplit,
    SetGameTime {
        time: time::Duration,
    },
    PauseGameTime,
    ResumeGameTime,
    SetCustomVariable {
        key: Box<str>,
        value: Box<str>,
    },
    Ping,
    GetCurrentState(TimerState),
}

macro_rules! cmd {
    ($command:literal) => {
        Message::Text(Utf8Bytes::from_static(concat!(
            "{\"command\":\"",
            $command,
            "\"}"
        )))
    };
}

impl WsThread {
    fn new(
        ws: WebSocket<TcpStream>,
        timer_state: Arc<RwLock<TimerState>>,
    ) -> (Self, Sender<WsCommand>) {
        let (tx, rx) = mpsc::channel();
        (
            Self {
                ws,
                rx,
                timer_state,
            },
            tx,
        )
    }

    const START: Message = cmd!("start");
    const SPLIT: Message = cmd!("split");
    const RESET: Message = cmd!("reset");
    const UNDO_SPLIT: Message = cmd!("undoSplit");
    const SKIP_SPLIT: Message = cmd!("skipSplit");
    const PAUSE_GAME_TIME: Message = cmd!("pauseGameTime");
    const RESUME_GAME_TIME: Message = cmd!("resumeGameTime");
    const GET_CURRENT_STATE: Message = cmd!("getCurrentState");
    const PING: Message = cmd!("ping");

    fn set_game_time(time: time::Duration) -> Message {
        Message::text(format!(
            "{{\"command\":\"setGameTime\",\"time\":\"{}\"}}",
            time.whole_seconds()
        ))
    }

    fn set_custom_variable(key: &str, value: &str) -> Message {
        Message::text(format!(
            "{{\"command\":\"setCustomVariable\",\"key\":\"{}\",\"value\":\"{}\"}}",
            key, value
        ))
    }

    fn parse_response(text: &str) -> Result<CommandResult> {
        let response = serde_json::from_str::<CommandResult>(text)?;
        trace!("Websocket response: {response:?}");
        Ok(response)
    }

    fn parse_state(res: CommandResult) -> Result<TimerState> {
        let state = match res {
            CommandResult::Success(Response::State(state)) => state,
            CommandResult::Success(success) => anyhow::bail!("Expected state, got {success:?}"),
            CommandResult::Error(error) => anyhow::bail!(format!("{error:?}")),
        };
        trace!("Current timer state: {state:?}");
        let state = match state {
            State::NotRunning => TimerState::NotRunning,
            State::Running(_) => TimerState::Running,
            State::Paused(_) => TimerState::Paused,
            State::Ended => TimerState::Ended,
        };
        Ok(state)
    }

    fn run(mut self) {
        loop {
            let Ok(cmd) = self.rx.recv() else { break };

            match self.handle(cmd) {
                ControlFlow::Continue(()) => {}
                ControlFlow::Break(()) => break,
            }
        }
    }

    fn handle(&mut self, cmd: WsCommand) -> ControlFlow<()> {
        macro_rules! send {
            ($msg:expr) => {
                if let Err(e) = self.ws.send($msg) {
                    error!("Websocket Write Error: {e:?}");
                }
            };
        }

        match &cmd {
            WsCommand::Start => send!(Self::START),
            WsCommand::Split => send!(Self::SPLIT),
            WsCommand::Reset => send!(Self::RESET),
            WsCommand::UndoSplit => send!(Self::UNDO_SPLIT),
            WsCommand::SkipSplit => send!(Self::SKIP_SPLIT),
            WsCommand::SetGameTime { time } => send!(Self::set_game_time(*time)),
            WsCommand::PauseGameTime => send!(Self::PAUSE_GAME_TIME),
            WsCommand::ResumeGameTime => send!(Self::RESUME_GAME_TIME),
            WsCommand::SetCustomVariable { key, value } => {
                send!(Self::set_custom_variable(key, value))
            }
            WsCommand::Ping => send!(Self::PING),
            WsCommand::GetCurrentState(_) => send!(Self::GET_CURRENT_STATE),
        }

        let msg = match self.ws.read() {
            Ok(msg) => msg,
            Err(e) => {
                error!("Websocket Read Error: {e:?}");
                return ControlFlow::Break(());
            }
        };

        debug!("Websocket message: {msg:?}");

        if let WsCommand::GetCurrentState(before) = cmd {
            let Ok(msg) = msg.to_text() else {
                error!("Not a test message: {msg:?}");
                return ControlFlow::Break(());
            };
            let msg = match Self::parse_response(msg) {
                Ok(msg) => msg,
                Err(e) => {
                    error!("Websocket Parse Error: {e:?}");
                    return ControlFlow::Continue(());
                }
            };
            let state = match Self::parse_state(msg) {
                Ok(msg) => msg,
                Err(e) => {
                    error!("Response Parse Error: {e:?}");
                    return ControlFlow::Continue(());
                }
            };

            if state != before {
                let mut guard = self.timer_state.write().unwrap_or_else(|e| e.into_inner());
                *guard = state;
            }
        }

        ControlFlow::Continue(())
    }
}

#[derive(Debug, serde_derive::Deserialize)]
#[serde(rename_all = "camelCase")]
enum CommandResult {
    Success(Response),
    Error(Error),
}

#[derive(Debug, serde_derive::Deserialize)]
#[serde(tag = "state", content = "index")]
enum State {
    NotRunning,
    Running(usize),
    Paused(usize),
    Ended,
}

#[derive(Debug, serde_derive::Deserialize)]
#[serde(untagged)]
enum Response {
    None,
    String(String),
    State(State),
}

#[derive(Debug, serde_derive::Deserialize)]
#[serde(tag = "code")]
enum Error {
    InvalidCommand {
        message: String,
    },
    InvalidIndex,
    #[serde(untagged)]
    Timer {
        code: event::Error,
    },
}

#[derive(Debug, Clone)]
struct CurrentTimerState {
    tx: Sender<WsCommand>,
    state: Arc<RwLock<TimerState>>,
}

impl CurrentTimerState {
    fn new(tx: Sender<WsCommand>, state: Arc<RwLock<TimerState>>) -> Self {
        Self { tx, state }
    }

    fn state(&self) -> TimerState {
        *self.state.read().unwrap_or_else(|e| e.into_inner())
    }

    fn refresh_state(&self) {
        self.send(WsCommand::GetCurrentState(self.state()));
    }

    fn send(&self, cmd: WsCommand) {
        if let Err(e) = self.tx.send(cmd) {
            error!("Could not send command to the websocket: {e:?}");
        }
    }
}

struct SplitterThread {
    splitter: AutoSplitter<WebsocketTimer>,
    timer_state: CurrentTimerState,
}

impl SplitterThread {
    fn new(
        path: &Path,
        settings: Option<&Path>,
        timer: WebsocketTimer,
        timer_state: CurrentTimerState,
    ) -> anyhow::Result<Self> {
        let module =
            fs::read(path).context("Failed loading the auto splitter from the file system.")?;

        let mut settings_map = settings::Map::new();
        if let Some(settings) = settings {
            SplitterThread::load_settings(settings, &mut settings_map)?;
        }

        let runtime = {
            let mut config = Config::default();
            config.debug_info = false;
            config.optimize = true;
            config.backtrace_details = false;
            Runtime::new(config).unwrap()
        };

        let module = runtime
            .compile(&module)
            .context("Failed loading the auto splitter.")?;

        let splitter = module
            .instantiate(timer, Some(settings_map), None)
            .context("Failed starting the auto splitter.")?;

        Ok(SplitterThread {
            splitter,
            timer_state,
        })
    }

    fn load_settings(file: &Path, settings_map: &mut settings::Map) -> anyhow::Result<()> {
        let settings = fs::read_to_string(file)?;
        let settings = toml::from_str::<toml::Table>(&settings)?;

        for (key, value) in settings {
            let value = match value {
                toml::Value::Boolean(value) => settings::Value::Bool(value),
                toml::Value::String(value) => settings::Value::String(value.into()),
                toml::Value::Integer(value) => settings::Value::I64(value),
                toml::Value::Float(value) => settings::Value::F64(value),
                _ => anyhow::bail!("Unsupported value type: {value:?}"),
            };

            settings_map.insert(key.into(), value);
        }

        Ok(())
    }

    fn run(self) {
        let mut next_tick = Instant::now();
        let mut last_state_check = next_tick - Duration::from_secs(1);

        loop {
            let auto_splitter = &self.splitter;

            let mut auto_splitter_lock = auto_splitter.lock();
            // does the actual work
            let res = auto_splitter_lock.update();
            drop(auto_splitter_lock);

            if let Err(e) = res {
                error!("{:?}", e.context("Failed executing the auto splitter."));
            };

            let tick_rate = auto_splitter.tick_rate();
            next_tick += tick_rate;

            let mut now = Instant::now();
            if next_tick.checked_duration_since(now).is_some()
                && now.saturating_duration_since(last_state_check) > Duration::from_secs(1)
            {
                self.timer_state.refresh_state();
                now = Instant::now();
                last_state_check = now;
            }

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
}

struct WebsocketTimer {
    timer_state: CurrentTimerState,
    rx: Sender<WsCommand>,
}

impl WebsocketTimer {
    fn new(timer_state: CurrentTimerState, rx: Sender<WsCommand>) -> Self {
        Self { timer_state, rx }
    }

    fn send(&self, cmd: WsCommand) {
        if let Err(e) = self.rx.send(cmd) {
            error!("Could not send command to the websocket: {e:?}");
        }
    }
}

impl Timer for WebsocketTimer {
    fn state(&self) -> TimerState {
        self.timer_state.state()
    }

    fn start(&mut self) {
        trace!("Start");
        self.send(WsCommand::Start);
    }

    fn split(&mut self) {
        trace!("Split");
        self.send(WsCommand::Split);
    }

    fn skip_split(&mut self) {
        trace!("Skip split");
        self.send(WsCommand::SkipSplit);
    }

    fn undo_split(&mut self) {
        trace!("Undo split");
        self.send(WsCommand::UndoSplit);
    }

    fn reset(&mut self) {
        trace!("Reset");
        self.send(WsCommand::Reset);
    }

    fn set_game_time(&mut self, time: time::Duration) {
        trace!("Set game time to {time:?}");
        self.send(WsCommand::SetGameTime { time });
    }

    fn pause_game_time(&mut self) {
        trace!("Pause game time");
        self.send(WsCommand::PauseGameTime);
    }

    fn resume_game_time(&mut self) {
        trace!("Resume game time");
        self.send(WsCommand::ResumeGameTime);
    }

    fn set_variable(&mut self, key: &str, value: &str) {
        trace!("Set variable {key} = {value}");
        self.send(WsCommand::SetCustomVariable {
            key: key.into(),
            value: value.into(),
        });
    }

    fn log_auto_splitter(&mut self, message: std::fmt::Arguments<'_>) {
        eprintln!("{message}");
    }

    fn log_runtime(&mut self, message: std::fmt::Arguments<'_>, log_level: LogLevel) {
        let level = match log_level {
            LogLevel::Trace => log::Level::Trace,
            LogLevel::Debug => log::Level::Debug,
            LogLevel::Info => log::Level::Info,
            LogLevel::Warning => log::Level::Warn,
            LogLevel::Error => log::Level::Error,
        };
        log::log!(level, "{message}");
    }
}

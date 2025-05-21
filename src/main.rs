
use std::{
    fs,
    net::{IpAddr, TcpListener, TcpStream},
    ops::ControlFlow,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
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
        let _ = ws.handle(WsCommand::GetCurrentState);

        let shared_state = SharedState::new(timer_state);
        let timer = WebsocketTimer::new(shared_state.clone(), tx);
        let state = SplitterThread::new(
            &args.wasm_path,
            args.settings.as_deref(),
            timer,
            shared_state,
        )?;

        thread::Builder::new()
            .name(format!("Websocket Handler {counter}"))
            .spawn(move || ws.run())
            .unwrap();

        thread::Builder::new()
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
    SetGameTime(time::Duration),
    PauseGameTime,
    ResumeGameTime,
    SetCustomVariable(Box<str>, Box<str>),
    GetCurrentState,
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

    fn run(mut self) {
        loop {
            let cb = match self.rx.recv_timeout(Duration::from_secs(1)) {
                Ok(cmd) => self.handle(cmd),
                Err(mpsc::RecvTimeoutError::Timeout) => self.read(),
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    info!("Sender disconnected");
                    ControlFlow::Break(())
                }
            };

            match cb {
                ControlFlow::Continue(()) => {}
                ControlFlow::Break(()) => break,
            };
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
            WsCommand::SetGameTime(time) => send!(Self::set_game_time(*time)),
            WsCommand::PauseGameTime => send!(Self::PAUSE_GAME_TIME),
            WsCommand::ResumeGameTime => send!(Self::RESUME_GAME_TIME),
            WsCommand::SetCustomVariable(key, value) => {
                send!(Self::set_custom_variable(key, value))
            }
            WsCommand::GetCurrentState => send!(Self::GET_CURRENT_STATE),
        }

        self.read()
    }

    fn read(&mut self) -> ControlFlow<()> {
        let msg = match self.ws.read() {
            Ok(msg) => msg,
            Err(e) => {
                error!("Websocket Read Error: {e:?}");
                return ControlFlow::Break(());
            }
        };
        trace!("Websocket message: {msg:?}");

        let msg = match msg {
            Message::Text(ref utf8_bytes) => utf8_bytes.as_str(),
            Message::Binary(ref bytes) => std::str::from_utf8(bytes).unwrap(),
            Message::Close(_) => {
                info!("Other side hang up, stopping");
                return ControlFlow::Break(());
            }
            otherwise => {
                error!("Not a text message: {otherwise:?}");
                return ControlFlow::Break(());
            }
        };

        let msg = match Self::parse_response(msg) {
            Ok(msg) => msg,
            Err(e) => {
                error!("Websocket Parse Error: {e:?}");
                return ControlFlow::Continue(());
            }
        };
        trace!("Websocket message parsed: {msg:?}");

        let state = match msg {
            CommandResult::Success(Response::State(state)) => Some(match state {
                State::NotRunning => TimerState::NotRunning,
                State::Running(_) => TimerState::Running,
                State::Paused(_) => TimerState::Paused,
                State::Ended => TimerState::Ended,
            }),
            CommandResult::Event(Event::Started) => Some(TimerState::Running),
            CommandResult::Event(Event::Paused) => Some(TimerState::Paused),
            CommandResult::Event(Event::Resumed) => Some(TimerState::Running),
            CommandResult::Event(Event::Finished) => Some(TimerState::Ended),
            CommandResult::Event(Event::Reset) => Some(TimerState::NotRunning),
            CommandResult::Success(_) | CommandResult::Event(_) | CommandResult::Error(_) => None,
        };

        debug!("Chaning timer state to {state:?}");

        if let Some(state) = state {
            let mut guard = self.timer_state.write().unwrap_or_else(|e| e.into_inner());
            *guard = state;
        }

        ControlFlow::Continue(())
    }
}

#[derive(Debug, serde_derive::Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(unused)]
enum CommandResult {
    Success(Response),
    Error(Error),
    Event(Event),
}

#[derive(Debug, serde_derive::Deserialize)]
#[serde(untagged)]
#[allow(unused)]
enum Response {
    None,
    String(String),
    State(State),
}

#[derive(Debug, serde_derive::Deserialize)]
#[serde(tag = "state", content = "index")]
#[allow(unused)]
enum State {
    NotRunning,
    Running(usize),
    Paused(usize),
    Ended,
}

#[derive(Debug, serde_derive::Deserialize)]
#[serde(tag = "code")]
#[allow(unused)]
enum Error {
    InvalidCommand {
        message: String,
    },
    InvalidIndex,
    #[serde(untagged)]
    Timer {
        code: EventError,
    },
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, serde_derive::Deserialize)]
enum EventError {
    /// The operation is not supported.
    Unsupported = 0,
    /// The timer can't be interacted with at the moment.
    Busy = 1,
    /// There is already a run in progress.
    RunAlreadyInProgress = 2,
    /// There is no run in progress.
    NoRunInProgress = 3,
    /// The run is already finished.
    RunFinished = 4,
    /// The time is negative, you can't split yet.
    NegativeTime = 5,
    /// The last split can't be skipped.
    CantSkipLastSplit = 6,
    /// There is no split to undo.
    CantUndoFirstSplit = 7,
    /// The timer is already paused.
    AlreadyPaused = 8,
    /// The timer is not paused.
    NotPaused = 9,
    /// The requested comparison doesn't exist.
    ComparisonDoesntExist = 10,
    /// The game time is already initialized.
    GameTimeAlreadyInitialized = 11,
    /// The game time is already paused.
    GameTimeAlreadyPaused = 12,
    /// The game time is not paused.
    GameTimeNotPaused = 13,
    /// The time could not be parsed.
    CouldNotParseTime = 14,
    /// The timer is currently paused.
    TimerPaused = 15,
    /// The runner decided to not reset the run.
    RunnerDecidedAgainstReset = 16,
    /// An unknown error occurred.
    #[serde(other)]
    Unknown,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, serde_derive::Deserialize)]
pub enum Event {
    /// The timer has been started.
    Started = 0,
    /// A split happened. Note that the final split is signaled by
    /// [`Finished`].
    Splitted = 1,
    /// The final split happened, the run is now finished, but has not been
    /// reset yet.
    Finished = 2,
    /// The timer has been reset.
    Reset = 3,
    /// The previous split has been undone.
    SplitUndone = 4,
    /// The current split has been skipped.
    SplitSkipped = 5,
    /// The timer has been paused.
    Paused = 6,
    /// The timer has been resumed.
    Resumed = 7,
    /// All the pauses have been undone.
    PausesUndone = 8,
    /// All the pauses have been undone and the timer has been resumed.
    PausesUndoneAndResumed = 9,
    /// The comparison has been changed.
    ComparisonChanged = 10,
    /// The timing method has been changed.
    TimingMethodChanged = 11,
    /// The game time has been initialized.
    GameTimeInitialized = 12,
    /// The game time has been set.
    GameTimeSet = 13,
    /// The game time has been paused.
    GameTimePaused = 14,
    /// The game time has been resumed.
    GameTimeResumed = 15,
    /// The loading times have been set.
    LoadingTimesSet = 16,
    /// A custom variable has been set.
    CustomVariableSet = 17,
    /// An unknown event occurred.
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone)]
struct SharedState {
    state: Arc<RwLock<TimerState>>,
    alive: Arc<AtomicBool>,
}

impl SharedState {
    fn new(state: Arc<RwLock<TimerState>>) -> Self {
        Self {
            state,
            alive: Arc::new(AtomicBool::new(true)),
        }
    }

    fn state(&self) -> TimerState {
        *self.state.read().unwrap_or_else(|e| e.into_inner())
    }

    fn alive(&self) -> bool {
        self.alive.load(Ordering::Relaxed)
    }

    fn deadge(&self) {
        self.alive.store(false, Ordering::Relaxed);
    }
}

struct SplitterThread {
    splitter: AutoSplitter<WebsocketTimer>,
    state: SharedState,
}

impl SplitterThread {
    fn new(
        path: &Path,
        settings: Option<&Path>,
        timer: WebsocketTimer,
        state: SharedState,
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

        Ok(SplitterThread { splitter, state })
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

        loop {
            let auto_splitter = &self.splitter;

            let mut auto_splitter_lock = auto_splitter.lock();
            // does the actual work
            let res = auto_splitter_lock.update();
            drop(auto_splitter_lock);

            if let Err(e) = res {
                error!("{:?}", e.context("Failed executing the auto splitter."));
            };

            if !self.state.alive() {
                break;
            }

            let tick_rate = auto_splitter.tick_rate();
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
}

struct WebsocketTimer {
    state: SharedState,
    rx: Sender<WsCommand>,
}

impl WebsocketTimer {
    fn new(state: SharedState, rx: Sender<WsCommand>) -> Self {
        Self { state, rx }
    }

    fn send(&self, cmd: WsCommand) {
        if let Err(e) = self.rx.send(cmd) {
            error!("Could not send command to the websocket: {e:?}");
            self.state.deadge();
        }
    }
}

impl Timer for WebsocketTimer {
    fn state(&self) -> TimerState {
        self.state.state()
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
        self.send(WsCommand::SetGameTime(time));
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
        self.send(WsCommand::SetCustomVariable(key.into(), value.into()));
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

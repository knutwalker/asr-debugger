#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")] // hide console window on Windows in release

use std::{
    fs,
    net::{IpAddr, TcpListener, TcpStream},
    path::PathBuf,
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use clap::Parser;
use livesplit_auto_splitting::{
    settings, time, AutoSplitter, Config, ExecutionGuard, LogLevel, Runtime, Timer, TimerState,
};
use livesplit_core::event;
use log::{debug, info, trace};
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

    for stream in server.incoming() {
        let stream = stream?;
        let path = args.wasm_path.clone();
        std::thread::spawn(move || -> Result<()> {
            let ws = tungstenite::accept(stream)?;

            let mut timer = WebsocketTimer::new(ws);
            timer.fetch_current_state()?;

            let state = SplitterState::new(path, timer)?;
            state.run();

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

    Ok(())
}

struct SplitterState {
    auto_splitter: AutoSplitter<WebsocketTimer>,
}

impl SplitterState {
    fn new(path: PathBuf, timer: WebsocketTimer) -> anyhow::Result<Self> {
        let module =
            fs::read(&path).context("Failed loading the auto splitter from the file system.")?;

        let mut settings_map = settings::Map::new();

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

        let auto_splitter = module
            .instantiate(timer, Some(settings_map), None)
            .context("Failed starting the auto splitter.")?;

        Ok(SplitterState { auto_splitter })
    }

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

    fn run(self) {
        let mut next_tick = Instant::now();
        loop {
            let auto_splitter = &self.auto_splitter;

            let mut auto_splitter_lock = auto_splitter.lock();
            // does the actual work
            let res = auto_splitter_lock.update();
            drop(auto_splitter_lock);

            let tick_rate = auto_splitter.tick_rate();

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
}

#[derive(Debug, serde_derive::Deserialize)]
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

macro_rules! cmd {
    ($command:literal) => {
        Message::Text(Utf8Bytes::from_static(concat!(
            "{\"command\":\"",
            $command,
            "\"}"
        )))
    };
}

struct WebsocketTimer {
    ws: WebSocket<TcpStream>,
    state: TimerState,
}

impl WebsocketTimer {
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

    fn new(ws: WebSocket<TcpStream>) -> Self {
        Self {
            ws,
            state: TimerState::NotRunning,
        }
    }

    fn fetch_current_state(&mut self) -> Result<TimerState> {
        self.ws.send(Self::GET_CURRENT_STATE)?;
        self.read_current_state()
    }

    fn read_current_state(&mut self) -> Result<TimerState> {
        let msg = self.read_message()?;
        let msg = Self::parse_response(&msg)?;
        let state = Self::parse_state(msg)?;
        self.state = state;
        Ok(state)
    }

    fn read_message(&mut self) -> Result<Utf8Bytes> {
        let msg = self.ws.read()?;
        trace!("Websocket message: {msg:?}");
        Ok(msg.into_text()?)
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

    fn parse_message(msg: Message) -> Result<CommandResult> {
        let msg = msg.to_text()?;
        Self::parse_response(msg)
    }
}

macro_rules! send {
    ($self:ident, $msg:expr) => {
        if let Err(e) = $self.ws.send($msg) {
            $self.log_runtime(format_args!("Error: {e:?}"), LogLevel::Error);
        }
    };
}

impl Timer for WebsocketTimer {
    fn state(&self) -> TimerState {
        self.state
    }

    fn start(&mut self) {
        trace!("Start");
        send!(self, Self::START);
    }

    fn split(&mut self) {
        trace!("Split");
        send!(self, Self::SPLIT);
    }

    fn skip_split(&mut self) {
        trace!("Skip split");
        send!(self, Self::SKIP_SPLIT);
    }

    fn undo_split(&mut self) {
        trace!("Undo split");
        send!(self, Self::UNDO_SPLIT);
    }

    fn reset(&mut self) {
        trace!("Reset");
        send!(self, Self::RESET);
    }

    fn set_game_time(&mut self, time: time::Duration) {
        trace!("Set game time to {time:?}");
        send!(self, Self::set_game_time(time));
    }

    fn pause_game_time(&mut self) {
        trace!("Pause game time");
        send!(self, Self::PAUSE_GAME_TIME);
    }

    fn resume_game_time(&mut self) {
        trace!("Resume game time");
        send!(self, Self::RESUME_GAME_TIME);
    }

    fn set_variable(&mut self, key: &str, value: &str) {
        trace!("Set variable {key} = {value}");
        send!(self, Self::set_custom_variable(key, value));
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

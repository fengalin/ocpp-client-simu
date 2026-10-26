use anyhow::{Context, bail};
use futures::prelude::*;
use ocpp_rs::v16::{
    call::{self, Action},
    data_types, enums,
    parse::{self, Message},
    pending::PendingCalls,
    typed_call_result::TypedCallResult,
};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite as ts};
use tracing::{debug, error, info, warn};

use std::{
    collections::VecDeque,
    ops::ControlFlow,
    pin::{Pin, pin},
    time::{Duration, Instant},
};

const SERVER_ADDRESS: &str = "ws://127.0.0.1:9000/";
const VENDOR_ID: &str = "OCPP client simu";
const TAG_ID: &str = "SIMUTAG";

const RECONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const EVENT_INTERVAL: Duration = Duration::from_secs(3);
const HEARTBEAT_TIMEOUT: Duration = Duration::from_mins(1);

#[derive(Debug)]
pub struct ChargePointSimu {
    scenario: VecDeque<CallEvent>,
    charging_steps_interval: Duration,
    with_heartbeats: bool,
    cur_tid: Option<i32>,
    ws_stream: Option<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    msg_id: usize,
    call_response_tracker: PendingCalls,
}

impl ChargePointSimu {
    pub fn new(charging_steps_interval: Duration, with_heartbeats: bool) -> Self {
        let scenario = VecDeque::from_iter([
            CallEvent::StatusNotification {
                connector_id: 0,
                status: enums::ChargePointStatus::Available,
            },
            CallEvent::StatusNotification {
                connector_id: 1,
                status: enums::ChargePointStatus::Available,
            },
            CallEvent::MeterValues {
                energy: 1234,
                power: 0,
                voltage: 231,
                temp: 25,
                transaction_id: None,
            },
            CallEvent::StatusNotification {
                connector_id: 1,
                status: enums::ChargePointStatus::Preparing,
            },
            CallEvent::StartTransaction { meter_start: 1234 },
            CallEvent::StatusNotification {
                connector_id: 1,
                status: enums::ChargePointStatus::Charging,
            },
            CallEvent::MeterValues {
                energy: 1235,
                power: 0,
                voltage: 231,
                temp: 25,
                transaction_id: Some(1),
            },
            CallEvent::MeterValues {
                energy: 1265,
                power: 3_600,
                voltage: 230,
                temp: 25,
                transaction_id: Some(1),
            },
            CallEvent::MeterValues {
                energy: 1325,
                power: 3_600,
                voltage: 229,
                temp: 26,
                transaction_id: Some(1),
            },
            CallEvent::MeterValues {
                energy: 1385,
                power: 3_600,
                voltage: 228,
                temp: 26,
                transaction_id: Some(1),
            },
            CallEvent::MeterValues {
                energy: 1505,
                power: 7_200,
                voltage: 227,
                temp: 27,
                transaction_id: Some(1),
            },
            CallEvent::MeterValues {
                energy: 1625,
                power: 7_200,
                voltage: 227,
                temp: 28,
                transaction_id: Some(1),
            },
            CallEvent::StatusNotification {
                connector_id: 1,
                status: enums::ChargePointStatus::SuspendedEVSE,
            },
            // CallEvent::StatusNotification {
            //     connector_id: 1,
            //     status: enums::ChargePointStatus::SuspendedEV,
            // },
        ]);

        ChargePointSimu {
            scenario,
            charging_steps_interval,
            with_heartbeats,
            cur_tid: None,
            ws_stream: None,
            msg_id: 0,
            call_response_tracker: PendingCalls::new(),
        }
    }

    pub async fn into_task(mut self, mut stop_rx: mpsc::Receiver<()>) {
        tokio::select! {
            biased;
            _  = stop_rx.recv() => {
                info!("shutting down due to stop request");
            }
            _ = self.listen() => (),
        }

        if let Some(mut ws_stream) = self.ws_stream.take() {
            match ws_stream
                .close(Some(ts::protocol::frame::CloseFrame {
                    code: ts::protocol::frame::coding::CloseCode::Normal,
                    reason: "".into(),
                }))
                .await
                .context("closing websocket")
            {
                Ok(_) => {
                    let recv_res = ws_stream.next().await;
                    info!(?recv_res, "server replied");
                }
                Err(err) => error!(?err, "closing socket"),
            }
        }
    }

    async fn listen(&mut self) {
        loop {
            match connect_async(SERVER_ADDRESS).await {
                Ok((ws_stream, _response)) => {
                    info!("connected");
                    self.ws_stream = Some(ws_stream);
                    if let Err(err) = self.start().await {
                        warn!(%err, "start()");
                    }
                }
                Err(err) => {
                    warn!(%err, "connecting");
                }
            }

            time::sleep(RECONNECT_TIMEOUT).await;
        }
    }

    async fn start(&mut self) -> anyhow::Result<()> {
        let mut next_evt = None;
        let mut last_evt_instant = Instant::now();
        let mut last_checkpoint_instant = Instant::now();

        loop {
            if next_evt.is_none() {
                next_evt = self.scenario.pop_front();
                if next_evt.is_some() {
                    last_evt_instant = Instant::now();
                }
            }
            // else previous instant has been sent yet

            let next_evt_timer_fut = if next_evt.is_some() {
                let next_evt_instant = if self.cur_tid.is_some() {
                    self.charging_steps_interval
                } else {
                    EVENT_INTERVAL
                };
                pin!(time::sleep(
                    next_evt_instant.saturating_sub(last_evt_instant.elapsed())
                )) as Pin<&mut (dyn Future<Output = ()> + Send)>
            } else {
                pin!(std::future::pending()) as _
            };

            let maybe_heartbeat_fut = if self.with_heartbeats {
                pin!(time::sleep(
                    HEARTBEAT_TIMEOUT.saturating_sub(last_checkpoint_instant.elapsed())
                )) as Pin<&mut (dyn Future<Output = ()> + Send)>
            } else {
                pin!(std::future::pending()) as _
            };

            tokio::select! {
                biased;
                server_msg = self.ws_stream.as_mut().unwrap().next() => {
                    let Some(server_msg) = server_msg else {
                        bail!("server disconnected");
                    };

                    last_checkpoint_instant = Instant::now();

                    debug!(?server_msg);
                    let server_msg = server_msg.context("server msg")?;
                    if self
                        .handle_incoming_msg(server_msg)
                        .await
                        .is_break()
                         {
                             info!("server closed socket");
                             break;
                         };
                }
                _ = next_evt_timer_fut => {
                    if let Some(next_evt) = next_evt.take() {
                        self.send_action(next_evt).await.context("sending evt")?;
                    }
                }
                _ = maybe_heartbeat_fut => {
                    debug!("heartbeat timeout");

                    last_checkpoint_instant = Instant::now();

                    self
                        .send_action(CallEvent::Heartbeat)
                        .await?;
                }
            }
        }

        Ok(())
    }

    async fn send_action(&mut self, mut evt: CallEvent) -> anyhow::Result<()> {
        if let CallEvent::MeterValues { transaction_id, .. } = &mut evt {
            *transaction_id = self.cur_tid;
        }

        let msg_id = self.next_msg_id();
        info!(?evt, %msg_id, "sending");
        self.ws_stream
            .as_mut()
            .unwrap()
            .send(ts::Message::Text(
                self.call_response_tracker
                    .send_call(call::Call::new(msg_id.to_string(), evt.into()))
                    .context("call tracker")?
                    .into(),
            ))
            .await?;

        Ok(())
    }

    async fn handle_incoming_msg(&mut self, msg: ts::Message) -> ControlFlow<()> {
        match &msg {
            ts::Message::Text(text) => {
                debug!("incoming text msg: {text}");

                let Ok(msg) = parse::deserialize_to_message(text) else {
                    error!("Failed to deserialize message: {msg}");
                    return ControlFlow::Continue(());
                };
                match msg {
                    Message::Call(call) => {
                        info!(?call);
                        match call.payload {
                            Action::StatusNotification(status) => {
                                if status.status == enums::ChargePointStatus::Available {
                                    self.cur_tid = None;
                                }
                            }
                            _ => (),
                        }
                    }
                    Message::CallResult(call_result) => {
                        info!(?call_result);
                        match self.call_response_tracker.resolve(call_result) {
                            Ok(TypedCallResult::StartTransaction(result)) => {
                                info!(tid = result.payload.transaction_id);
                                self.cur_tid = Some(result.payload.transaction_id);
                            }
                            Ok(TypedCallResult::StopTransaction(_)) => {
                                self.cur_tid = None;
                            }
                            _ => (),
                        }
                    }
                    Message::CallError(err) => {
                        error!(?err);
                    }
                }
            }
            ts::Message::Binary(binary) => {
                info!("incoming bin msg: {binary:?}");
            }
            ts::Message::Ping(_) => {
                info!("incoming ping");
            }
            ts::Message::Pong(_) => {
                info!("incoming pong");
            }
            ts::Message::Close(payload) => {
                info!("websocket closed {payload:?}");
                // forget it: we don't need to explicitely close it now
                let _ = self.ws_stream = None;
                return ControlFlow::Break(());
            }
            other => {
                info!("other incoming message: {other:?}");
            }
        }

        ControlFlow::Continue(())
    }

    fn next_msg_id(&mut self) -> usize {
        self.msg_id += 1;
        self.msg_id
    }
}

#[derive(Debug)]
enum CallEvent {
    Heartbeat,
    StatusNotification {
        connector_id: u32,
        status: enums::ChargePointStatus,
    },
    StartTransaction {
        meter_start: u64,
    },
    MeterValues {
        energy: u64,
        power: u64,
        voltage: u64,
        temp: u64,
        transaction_id: Option<i32>,
    },
}

impl From<CallEvent> for call::Action {
    fn from(evt: CallEvent) -> Self {
        match evt {
            CallEvent::Heartbeat => call::Action::Heartbeat(call::Heartbeat {}),
            CallEvent::StatusNotification {
                connector_id,
                status,
            } => call::Action::StatusNotification(call::StatusNotification {
                connector_id,
                error_code: enums::ChargePointErrorCode::NoError,
                status,
                timestamp: Some(data_types::DateTimeWrapper::new(chrono::Utc::now())),
                info: None,
                vendor_id: Some(VENDOR_ID.to_string()),
                vendor_error_code: None,
            }),
            CallEvent::StartTransaction { meter_start } => {
                call::Action::StartTransaction(call::StartTransaction {
                    connector_id: 1,
                    id_tag: TAG_ID.to_string(),
                    timestamp: data_types::DateTimeWrapper::new(chrono::Utc::now()),
                    reservation_id: None,
                    meter_start,
                })
            }
            CallEvent::MeterValues {
                energy,
                power,
                voltage,
                temp,
                transaction_id,
            } => call::Action::MeterValues(call::MeterValues {
                connector_id: 1,
                transaction_id,
                meter_value: vec![data_types::MeterValue {
                    timestamp: data_types::DateTimeWrapper::new(chrono::Utc::now()),
                    sampled_value: vec![
                        data_types::SampledValue {
                            value: energy.to_string(),
                            context: None,
                            format: None,
                            measurand: Some(enums::Measurand::EnergyActiveImportRegister),
                            phase: None,
                            location: None,
                            unit: Some(enums::UnitOfMeasure::Wh),
                        },
                        data_types::SampledValue {
                            value: power.to_string(),
                            context: None,
                            format: None,
                            measurand: Some(enums::Measurand::PowerActiveImport),
                            phase: None,
                            location: None,
                            unit: Some(enums::UnitOfMeasure::W),
                        },
                        data_types::SampledValue {
                            value: voltage.to_string(),
                            context: None,
                            format: None,
                            measurand: Some(enums::Measurand::Voltage),
                            phase: Some(enums::Phase::L1),
                            location: None,
                            unit: Some(enums::UnitOfMeasure::V),
                        },
                        data_types::SampledValue {
                            value: temp.to_string(),
                            context: None,
                            format: None,
                            measurand: Some(enums::Measurand::Temperature),
                            phase: None,
                            location: None,
                            unit: Some(enums::UnitOfMeasure::Celsius),
                        },
                    ],
                }],
            }),
        }
    }
}

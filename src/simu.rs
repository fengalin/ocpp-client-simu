use anyhow::{Context, bail};
use futures::prelude::*;
use ocpp_rs::v16::{call, data_types, enums};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite as ts};
use tracing::{debug, error, info, warn};

use std::{
    collections::VecDeque,
    ops::ControlFlow,
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
    ws_stream: Option<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    msg_id: usize,
}

impl ChargePointSimu {
    pub fn new() -> Self {
        let scenario = VecDeque::from_iter([
            CallEvent::StatusNotification {
                connector_id: 0,
                status: enums::ChargePointStatus::Available,
            },
            CallEvent::StatusNotification {
                connector_id: 1,
                status: enums::ChargePointStatus::Available,
            },
            CallEvent::StatusNotification {
                connector_id: 1,
                status: enums::ChargePointStatus::Preparing,
            },
            CallEvent::StartTransaction { meter_start: 1234 },
        ]);

        ChargePointSimu {
            scenario,
            ws_stream: None,
            msg_id: 0,
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
        let mut last_checkpoint_instant = Instant::now();

        loop {
            if let Some(evt) = self.scenario.pop_front() {
                time::sleep(EVENT_INTERVAL).await;
                self.send_action(evt).await.context("sending evt")?;
            }

            tokio::select! {
                biased;
                server_msg = self.ws_stream.as_mut().unwrap().next() => {
                    let Some(server_msg) = server_msg else {
                        bail!("server disconnected");
                    };

                    let now = Instant::now();
                    last_checkpoint_instant = now;

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
                _ = time::sleep(HEARTBEAT_TIMEOUT.saturating_sub(last_checkpoint_instant.elapsed())) => {
                    debug!("heartbeat timeout");

                    let now = Instant::now();
                    last_checkpoint_instant = now;

                    self
                        .send_action(CallEvent::Heartbeat)
                        .await?;
                }
            }
        }

        Ok(())
    }

    async fn send_action(&mut self, evt: CallEvent) -> anyhow::Result<()> {
        let msg_id = self.next_msg_id();
        info!(?evt, %msg_id, "sending");
        self.ws_stream
            .as_mut()
            .unwrap()
            .send(ts::Message::Text(
                serde_json::to_string(&call::Call::new(msg_id.to_string(), evt.into()))
                    .context("serializing")?
                    .into(),
            ))
            .await?;

        Ok(())
    }

    async fn handle_incoming_msg(&mut self, msg: ts::Message) -> ControlFlow<()> {
        match &msg {
            ts::Message::Text(text) => {
                info!("incoming msg text: {text}");
            }
            ts::Message::Binary(binary) => {
                info!("incoming msg bin: {binary:?}");
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
        }
    }
}

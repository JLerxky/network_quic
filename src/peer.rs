// Copyright Rivtower Technologies LLC.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use futures::SinkExt;
use quinn::ClientConfig;
use quinn::ConnectionError;
use quinn::NewConnection;
use quinn::RecvStream;
use quinn::SendStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time;

use tokio_stream::StreamExt;

use futures::future::poll_fn;

use tracing::{debug, info, warn};

use crate::codec::Codec;
use cita_cloud_proto::network::NetworkMsg;

type FramedWrite = tokio_util::codec::FramedWrite<SendStream, Codec>;
type FramedRead = tokio_util::codec::FramedRead<RecvStream, Codec>;

#[derive(Debug)]
pub struct PeersManger {
    known_peers: HashMap<String, PeerHandle>,
    connected_peers: HashSet<String>,
}

impl PeersManger {
    pub fn new(known_peers: HashMap<String, PeerHandle>) -> Self {
        Self {
            known_peers,
            connected_peers: HashSet::new(),
        }
    }

    pub fn get_known_peers(&self) -> &HashMap<String, PeerHandle> {
        &self.known_peers
    }

    pub fn add_known_peers(
        &mut self,
        domain: String,
        peer_handle: PeerHandle,
    ) -> Option<PeerHandle> {
        debug!("add_from_config_peers: {}", domain);
        self.known_peers.insert(domain, peer_handle)
    }

    pub fn get_connected_peers(&self) -> &HashSet<String> {
        &self.connected_peers
    }

    pub fn add_connected_peers(&mut self, domain: &str) -> bool {
        debug!("add_connected_peers: {}", domain);
        self.connected_peers.insert(domain.to_owned())
    }

    fn delete_connected_peers(&mut self, domain: &str) {
        if self.connected_peers.get(domain).is_some() {
            debug!("delete_connected_peers: {}", domain);
            self.connected_peers.remove(domain);
        }
    }

    pub fn delete_peer(&mut self, domain: &str) {
        if let Some(peer_handle) = self.known_peers.get(domain) {
            debug!("delete_peer: {}", domain);
            peer_handle.join_handle.abort();
            self.known_peers.remove(domain);
            self.delete_connected_peers(domain);
        }
    }
}

#[derive(Debug, Clone)]
pub struct PeerHandle {
    id: u64,
    host: String,
    port: u16,
    inbound_stream_tx: mpsc::Sender<NewConnection>,
    outbound_msg_tx: mpsc::Sender<NetworkMsg>,
    // run handle
    join_handle: Arc<JoinHandle<()>>,
}

impl PeerHandle {
    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn accept(&self, stream: NewConnection) {
        let inbound_stream_tx = self.inbound_stream_tx.clone();
        tokio::spawn(async move {
            let _ = inbound_stream_tx.send(stream).await;
        });
    }

    pub fn send_msg(&self, msg: NetworkMsg) {
        let outbound_msg_tx = self.outbound_msg_tx.clone();
        tokio::spawn(async move {
            let _ = outbound_msg_tx.send(msg).await;
        });
    }
}

pub struct Peer {
    id: u64,

    domain: String,
    host: String,
    port: u16,

    tls_config: Arc<ClientConfig>,
    reconnect_timeout: u64,

    // msg to send to this peer
    outbound_msg_rx: mpsc::Receiver<NetworkMsg>,
    // msg received from this peer
    inbound_msg_tx: mpsc::Sender<NetworkMsg>,

    inbound_stream_rx: mpsc::Receiver<NewConnection>,
}

impl Peer {
    pub fn init(
        id: u64,
        domain: String,
        host: String,
        port: u16,
        tls_config: Arc<ClientConfig>,
        reconnect_timeout: u64,
        inbound_msg_tx: mpsc::Sender<NetworkMsg>,
    ) -> PeerHandle {
        let (inbound_stream_tx, inbound_stream_rx) = mpsc::channel(1);
        let (outbound_msg_tx, outbound_msg_rx) = mpsc::channel(1024);

        let peer = Self {
            id,
            domain,
            host: host.clone(),
            port,
            tls_config,
            reconnect_timeout,
            outbound_msg_rx,
            inbound_msg_tx,
            inbound_stream_rx,
        };

        let join_handle = Arc::new(tokio::spawn(async move {
            peer.run().await;
        }));

        PeerHandle {
            id,
            host,
            port,
            inbound_stream_tx,
            outbound_msg_tx,
            join_handle,
        }
    }

    pub async fn run(mut self) {
        let mut new_conn: Option<quinn::NewConnection> = None;
        let mut conn_to_peer: Option<JoinHandle<Result<NewConnection, ConnectionError>>> = None;

        let reconnect_timeout = Duration::from_secs(self.reconnect_timeout);
        let reconnect_timeout_fut = time::sleep(Duration::from_secs(0));
        tokio::pin!(reconnect_timeout_fut);

        loop {
            tokio::select! {
                // spawn task to connect to this peer; outbound stream
                _ = reconnect_timeout_fut.as_mut(), if new_conn.is_none() && conn_to_peer.is_none() => {
                    let host = self.host.clone();
                    let port = self.port;

                    let tls_config = self.tls_config.clone();
                    info!(peer = %self.domain, host = %host, port = %port, "connecting..");

                    let domain = self.domain.clone();
                    let new_conn = tokio::spawn(async move {
                        let mut endpoint = quinn::Endpoint::client("[::]:0".parse::<std::net::SocketAddr>().unwrap()).unwrap();
                        endpoint.set_default_client_config(tls_config.as_ref().clone());
                        let conn = endpoint
                            .connect(
                                format!("{}:{}", host, port).parse::<std::net::SocketAddr>().unwrap(),
                                &domain,
                            )
                            .unwrap()
                            .await;
                        conn
                    });

                    conn_to_peer.replace(new_conn);
                }
                // handle previous connection task's result
                Ok(conn_result) = async { conn_to_peer.as_mut().unwrap().await }, if conn_to_peer.is_some() => {
                    conn_to_peer.take();

                    match conn_result {
                        Ok(conn) => {
                            info!(
                                peer = %self.domain,
                                host = %self.host,
                                port = %self.port,
                                r#type = %"outbound",
                                "new connection established"
                            );
                            new_conn.replace(conn);
                        }
                        Err(e) => {
                            debug!(
                                peer = %self.domain,
                                host = %self.host,
                                port = %self.port,
                                reason = %e,
                                "cannot connect to peer"
                            );
                            reconnect_timeout_fut.as_mut().reset(time::Instant::now() + reconnect_timeout);
                        }
                    }
                }
                // accept the established conn from this peer; inbound stream
                Some(conn) = self.inbound_stream_rx.recv() => {
                    if let Some(h) = conn_to_peer.take() {
                        h.abort();
                    }
                    // receive new stream
                    if new_conn.is_none() {
                        let incoming_peer_addr = conn.connection.remote_address();
                        info!(
                            peer = %self.domain,
                            host = %self.host,
                            port = %self.port,
                            incoming_peer_addr = ?incoming_peer_addr,
                            r#type = %"inbound",
                            "new connection established"
                        );
                        new_conn.replace(conn);
                    }
                }
                // send out msgs to this peer; outbound msgs
                Some(msg) = self.outbound_msg_rx.recv() => {
                    // drain all the available outbound msgs
                    let mut msgs = vec![msg];
                    poll_fn(|cx| {
                        while let Poll::Ready(Some(msg)) = self.outbound_msg_rx.poll_recv(cx) {
                            msgs.push(msg);
                        }
                        Poll::Ready(())
                    }).await;

                    if let Some(conn) = new_conn.as_mut() {
                        let mut last_result = Ok(());

                        if let Ok(send_stream) = conn.connection.open_uni().await {
                            let mut tx = FramedWrite::new(send_stream, Codec);
                            for msg in msgs {
                                last_result = tx.feed(msg).await;
                                if last_result.is_err() {
                                    break;
                                }
                            }

                            if last_result.is_ok() {
                                last_result = tx.flush().await;
                            }
                        } else {
                            warn!(
                                peer = %self.domain,
                                host = %self.host,
                                port = %self.port,
                                // reason = %e,
                                "send outbound msgs failed, drop the stream"
                            );
                            new_conn.take();
                            reconnect_timeout_fut.as_mut().reset(time::Instant::now() + reconnect_timeout);
                        }


                        if let Err(e) = last_result {
                            warn!(
                                peer = %self.domain,
                                host = %self.host,
                                port = %self.port,
                                reason = %e,
                                "send outbound msgs failed, drop the stream"
                            );
                            new_conn.take();
                            reconnect_timeout_fut.as_mut().reset(time::Instant::now() + reconnect_timeout);
                        }
                    } else {
                        warn!(
                            peer = %self.domain,
                            host = %self.host,
                            port = %self.port,
                            msgs_cnt = %msgs.len(),
                            "drop outbound msgs since no available stream to this peer"
                        );
                    }
                }
                // receive msgs from this peer; inbound msgs
                opt_res = async { new_conn.as_mut().unwrap().uni_streams.next().await }, if new_conn.is_some() => {
                    // handle items produced by the stream; return true if the stream should be dropped

                    let f =  |opt_res: Option<Result<RecvStream, ConnectionError>>| {
                        match opt_res {
                            Some(Ok(recv)) => {
                                let mut rx = FramedRead::new(recv, Codec);
                                let inbound_msg_tx = self.inbound_msg_tx.clone();
                                tokio::spawn(async move {
                                    while let Some(Ok(mut msg)) = rx.next().await {
                                        msg.origin = self.id;
                                        let _ = inbound_msg_tx.send(msg).await;
                                    }
                                    debug!("stream end");
                                });

                                false
                            }
                            Some(Err(e)) => {
                                warn!(
                                    peer = %self.domain,
                                    host = %self.host,
                                    port = %self.port,
                                    reason = %e,
                                    "framed stream report error, will drop the stream"
                                );
                                true
                            }
                            None => {
                                warn!(
                                    peer = %self.domain,
                                    host = %self.host,
                                    port = %self.port,
                                    "framed stream end, will drop it"
                                );
                                true
                            }
                        }
                    };

                    // drain all the available inbound msgs
                    let wants_drop = f(opt_res);

                    if wants_drop {
                        new_conn.take();
                        reconnect_timeout_fut.as_mut().reset(time::Instant::now() + reconnect_timeout);
                    }
                }
                else => {
                    info!(
                        peer = %self.domain,
                        host = %self.host,
                        port = %self.port,
                        "Peer stopped",
                    );
                }
            }
        }
    }
}

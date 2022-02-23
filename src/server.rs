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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::anchor::RootCertStore;
use crate::health_check::HealthCheckServer;
use crate::peer::PeersManger;
use crate::util::{load_certs, make_client_config, make_server_config, pki_error};
use crate::{
    config::{calculate_md5, load_config, NetworkConfig},
    peer::Peer,
};
use cita_cloud_proto::common::{Empty, NodeNetInfo, StatusCode, TotalNodeNetInfo};
use cita_cloud_proto::health_check::health_server::HealthServer;
use cita_cloud_proto::network::{
    network_msg_handler_service_client::NetworkMsgHandlerServiceClient,
    network_service_server::{NetworkService, NetworkServiceServer},
    NetworkMsg, NetworkStatusResponse, RegisterInfo,
};
use futures::StreamExt;
use parking_lot::RwLock;
use quinn::ClientConfig;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::SystemTime;
use tentacle_multiaddr::{MultiAddr, Protocol};
use tokio::sync::mpsc;
use tokio_rustls::rustls::client::{ServerCertVerified, ServerCertVerifier};
use tokio_rustls::rustls::server::{ClientCertVerified, ClientCertVerifier};
use tokio_rustls::rustls::ServerName;
use tokio_rustls::rustls::{Certificate, DistinguishedNames, Error as TlsError};
use tokio_rustls::webpki::{
    self, DnsNameRef, EndEntityCert, Time, TlsClientTrustAnchors, TlsServerTrustAnchors,
    TrustAnchor,
};
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status};
use tracing::{debug, info, trace, warn};
use x509_parser::extensions::GeneralName;
use x509_parser::prelude::X509Certificate;
use x509_parser::traits::FromDer;

type SignatureAlgorithms = &'static [&'static webpki::SignatureAlgorithm];

// The `ClientCertVerifier` impl is from [rustls](https://docs.rs/rustls/0.20.0/src/rustls/verify.rs.html)

/// Which signature verification mechanisms we support.  No particular
/// order.
static SUPPORTED_SIG_ALGS: SignatureAlgorithms = &[
    &webpki::ECDSA_P256_SHA256,
    &webpki::ECDSA_P256_SHA384,
    &webpki::ECDSA_P384_SHA256,
    &webpki::ECDSA_P384_SHA384,
    &webpki::ED25519,
    &webpki::RSA_PSS_2048_8192_SHA256_LEGACY_KEY,
    &webpki::RSA_PSS_2048_8192_SHA384_LEGACY_KEY,
    &webpki::RSA_PSS_2048_8192_SHA512_LEGACY_KEY,
    &webpki::RSA_PKCS1_2048_8192_SHA256,
    &webpki::RSA_PKCS1_2048_8192_SHA384,
    &webpki::RSA_PKCS1_2048_8192_SHA512,
    &webpki::RSA_PKCS1_3072_8192_SHA384,
];

type CertChainAndRoots<'a, 'b> = (EndEntityCert<'a>, Vec<&'a [u8]>, Vec<TrustAnchor<'b>>);

fn prepare<'a, 'b>(
    end_entity: &'a Certificate,
    intermediates: &'a [Certificate],
    roots: &'b RootCertStore,
) -> Result<CertChainAndRoots<'a, 'b>, TlsError> {
    // EE cert must appear first.
    let cert = EndEntityCert::try_from(end_entity.0.as_slice()).map_err(pki_error)?;
    let chain: Vec<&'a [u8]> = intermediates.iter().map(|cert| cert.0.as_ref()).collect();
    let trust_roots = roots
        .roots
        .iter()
        .map(|cer| cer.to_trust_anchor())
        .collect();

    Ok((cert, chain, trust_roots))
}

#[derive(Clone)]
pub struct AllowKnownPeerOnly {
    roots: RootCertStore,
    peers: Arc<RwLock<PeersManger>>,
}

impl AllowKnownPeerOnly {
    pub fn new(trust_roots: Vec<Certificate>) -> Self {
        let mut roots = RootCertStore::empty();
        for cert in &trust_roots {
            roots.add(cert).unwrap();
        }
        Self {
            roots,
            peers: Arc::new(RwLock::new(PeersManger::new(HashMap::new()))),
        }
    }
}

impl ClientCertVerifier for AllowKnownPeerOnly {
    fn offer_client_auth(&self) -> bool {
        true
    }

    fn client_auth_mandatory(&self) -> Option<bool> {
        Some(true)
    }

    fn client_auth_root_subjects(&self) -> Option<DistinguishedNames> {
        Some(self.roots.subjects())
    }

    fn verify_client_cert(
        &self,
        end_entity: &Certificate,
        intermediates: &[Certificate],
        now: SystemTime,
    ) -> Result<ClientCertVerified, TlsError> {
        let (cert, chain, trust_roots) = prepare(end_entity, intermediates, &self.roots)?;
        let now = Time::try_from(now).map_err(|_| TlsError::FailedToGetCurrentTime)?;
        cert.verify_is_valid_tls_client_cert(
            SUPPORTED_SIG_ALGS,
            &TlsClientTrustAnchors(&trust_roots),
            &chain,
            now,
        )
        .map_err(pki_error)?;

        let mut guard = self.peers.write();
        let known_peers = guard.get_known_peers().clone();
        let known = known_peers
            .keys()
            .map(|k| DnsNameRef::try_from_ascii(k.as_bytes()).unwrap());

        let valid_dns = cert
            .verify_is_valid_for_at_least_one_dns_name(known)
            .map_err(pki_error)?;

        for vd in valid_dns {
            guard.add_connected_peers(vd.into());
        }

        Ok(ClientCertVerified::assertion())
    }
}

impl ServerCertVerifier for AllowKnownPeerOnly {
    /// Will verify the certificate is valid in the following ways:
    /// - Signed by a  trusted `RootCertStore` CA
    /// - Not Expired
    /// - Valid for DNS entry
    /// - OCSP data is present
    fn verify_server_cert(
        &self,
        end_entity: &Certificate,
        intermediates: &[Certificate],
        server_name: &ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        ocsp_response: &[u8],
        now: SystemTime,
    ) -> Result<ServerCertVerified, TlsError> {
        let (cert, chain, trust_roots) = prepare(end_entity, intermediates, &self.roots)?;
        let now = Time::try_from(now).map_err(|_| TlsError::FailedToGetCurrentTime)?;
        cert.verify_is_valid_tls_server_cert(
            SUPPORTED_SIG_ALGS,
            &TlsServerTrustAnchors(&trust_roots),
            &chain,
            now,
        )
        .map_err(pki_error)?;

        if !ocsp_response.is_empty() {
            trace!("Unvalidated OCSP response: {:?}", ocsp_response.to_vec());
        }

        if let ServerName::DnsName(dns_name) = server_name {
            cert.verify_is_valid_for_dns_name(
                DnsNameRef::try_from_ascii_str(dns_name.as_ref()).unwrap(),
            )
            .map_err(pki_error)?;

            let mut guard = self.peers.write();
            guard.add_connected_peers(dns_name.as_ref());

            return Ok(ServerCertVerified::assertion());
        }
        Err(TlsError::UnsupportedNameType)
    }
}

pub struct Server {
    listen_port: u16,
    peers: Arc<RwLock<PeersManger>>,

    server_config: quinn::ServerConfig,
}

impl Server {
    pub async fn setup(config: NetworkConfig, path: String) {
        let roots = load_certs(&config.ca_cert);
        let verifier = AllowKnownPeerOnly::new(roots);
        let rustls_client_config =
            Arc::new(make_client_config(&config, Arc::new(verifier.clone())));

        const IDLE_TIMEOUT: Duration = Duration::from_millis(500);
        let mut transport_config = quinn::TransportConfig::default();
        transport_config.keep_alive_interval(Some(IDLE_TIMEOUT));

        let mut client_config = quinn::ClientConfig::new(rustls_client_config);
        client_config.transport = Arc::new(transport_config);

        let (inbound_msg_tx, inbound_msg_rx) = mpsc::channel(1024);
        let peers = {
            for c in config.peers.clone().into_iter() {
                let handle = Peer::init(
                    calculate_hash(&format!("{}:{}", &c.host, c.port)),
                    c.domain.clone(),
                    c.host,
                    c.port,
                    Arc::new(client_config.clone()),
                    config.reconnect_timeout,
                    inbound_msg_tx.clone(),
                );

                info!("peer: {}", c.domain);

                verifier
                    .peers
                    .write()
                    .add_known_peers(c.domain.clone(), handle);
            }
            Arc::clone(&verifier.peers)
        };

        let rustls_server_config =
            Arc::new(make_server_config(&config, Arc::new(verifier.clone())));
        let server_config = quinn::ServerConfig::with_crypto(rustls_server_config);
        let dispatch_table = Arc::new(RwLock::new(HashMap::new()));
        let dispatcher = NetworkMsgDispatcher {
            dispatch_table: dispatch_table.clone(),
            inbound_msg_rx,
        };
        tokio::spawn(async move {
            dispatcher.run().await;
        });

        let network_svc = CitaCloudNetworkServiceServer {
            dispatch_table,
            peers: peers.clone(),
            tls_config: Arc::new(client_config),
            inbound_msg_tx,
            reconnect_timeout: config.reconnect_timeout,
        };

        //hot update
        let network_svc_hot_update = network_svc.clone();
        let mut try_hot_update_interval =
            tokio::time::interval(Duration::from_secs(config.try_hot_update_interval));
        tokio::spawn(async move {
            info!("monitoring config file: ({})", &path[..]);
            if let Ok(mut md5) = calculate_md5(&path) {
                info!("config file initial md5: {:x}", md5);
                loop {
                    try_hot_update_interval.tick().await;
                    if let Ok(new_md5) = calculate_md5(&path) {
                        if new_md5 == md5 {
                            continue;
                        } else {
                            info!("config file new md5: {:x}", new_md5);
                            md5 = new_md5;
                            try_hot_update(&path, &network_svc_hot_update).await;
                        }
                    } else {
                        warn!("calculate config file md5 failed, make sure it's not removed");
                        continue;
                    };
                }
            } else {
                warn!("calculate config file md5 failed, hot update invalid");
            };
        });

        let grpc_addr = format!("0.0.0.0:{}", config.grpc_port).parse().unwrap();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(NetworkServiceServer::new(network_svc))
                .add_service(HealthServer::new(HealthCheckServer {}))
                .serve(grpc_addr)
                .await
                .unwrap();
        });

        let this = Self {
            listen_port: config.listen_port,
            peers,
            server_config,
        };

        this.serve().await;
    }

    async fn serve(self) {
        let addr = format!("0.0.0.0:{}", self.listen_port);
        if let Ok((endpoint, mut incoming)) = quinn::Endpoint::server(
            self.server_config,
            addr.parse::<std::net::SocketAddr>().unwrap(),
        ) {
            info!("listen on `{}`", endpoint.local_addr().unwrap());
            while let Some(conn) = incoming.next().await {
                match conn.await {
                    Ok(new_conn) => {
                        let addr = new_conn.connection.remote_address().to_string();
                        info!("connecting: {}", addr);
                        let certs = new_conn
                            .connection
                            .peer_identity()
                            .unwrap()
                            .downcast::<Vec<Certificate>>()
                            .unwrap();

                        let dns_s: Vec<String> = {
                            let cert = certs.first().unwrap();
                            let (_, parsed) = X509Certificate::from_der(cert.as_ref()).unwrap();
                            if let Some(san) =
                                parsed.tbs_certificate.subject_alternative_name().unwrap()
                            {
                                san.value
                                    .general_names
                                    .iter()
                                    .filter_map(|n| {
                                        if let GeneralName::DNSName(dns) = *n {
                                            Some(dns.to_owned())
                                        } else {
                                            None
                                        }
                                    })
                                    .collect()
                            } else {
                                vec![]
                            }
                        };

                        let guard = self.peers.read();
                        if let Some(peer) = dns_s
                            .iter()
                            .find_map(|dns| guard.get_known_peers().get(dns))
                        {
                            peer.accept(new_conn);
                        } else {
                            debug!(
                                peers = ?&*guard,
                                cert.dns = ?dns_s,
                                "no peer instance for this connection"
                            );
                        }
                    }
                    Err(e) => {
                        debug!("connecting error: {:?}", e);
                    }
                };
            }
        }
    }
}

#[derive(Clone)]
pub struct CitaCloudNetworkServiceServer {
    dispatch_table: Arc<RwLock<HashMap<String, NetworkMsgHandlerServiceClient<Channel>>>>,
    peers: Arc<RwLock<PeersManger>>,

    // for adding new node
    tls_config: Arc<ClientConfig>,
    inbound_msg_tx: mpsc::Sender<NetworkMsg>,
    reconnect_timeout: u64,
}

#[tonic::async_trait]
impl NetworkService for CitaCloudNetworkServiceServer {
    async fn send_msg(
        &self,
        request: Request<NetworkMsg>,
    ) -> Result<Response<StatusCode>, tonic::Status> {
        let mut msg = request.into_inner();
        // This origin only used in local context, and shouldn't leak outside.
        let origin = msg.origin;
        msg.origin = 0;
        let guard = self.peers.read();

        if let Some(peer) = guard
            .get_known_peers()
            .values()
            .find(|peer| peer.id() == origin)
        {
            peer.send_msg(msg);
        } else {
            // TODO: check if it's necessary
            // fallback to broadcast
            for domain in guard.get_connected_peers().iter() {
                guard
                    .get_known_peers()
                    .get(domain)
                    .unwrap()
                    .send_msg(msg.clone());
            }
        }
        Ok(Response::new(status_code::StatusCode::Success.into()))
    }

    async fn broadcast(
        &self,
        request: Request<NetworkMsg>,
    ) -> Result<Response<StatusCode>, tonic::Status> {
        let mut msg = request.into_inner();
        // This origin only used in local context, and shouldn't leak outside.
        msg.origin = 0;
        let guard = self.peers.read();

        for domain in guard.get_connected_peers().iter() {
            guard
                .get_known_peers()
                .get(domain)
                .unwrap()
                .send_msg(msg.clone());
        }

        Ok(Response::new(status_code::StatusCode::Success.into()))
    }

    async fn get_network_status(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<NetworkStatusResponse>, tonic::Status> {
        let reply = NetworkStatusResponse {
            peer_count: self.peers.read().get_connected_peers().len() as u64,
        };

        Ok(Response::new(reply))
    }

    async fn register_network_msg_handler(
        &self,
        request: Request<RegisterInfo>,
    ) -> Result<Response<StatusCode>, tonic::Status> {
        let info = request.into_inner();
        debug!("info: {:?}", &info);
        let module_name = info.module_name;
        let hostname = info.hostname;
        let port = info.port;

        let client = {
            let uri = format!("http://{}:{}", hostname, port);
            let channel = Endpoint::from_shared(uri)
                .map_err(|e| {
                    tonic::Status::invalid_argument(format!("invalid host and port: {}", e))
                })?
                .connect_lazy()
                .unwrap();
            NetworkMsgHandlerServiceClient::new(channel)
        };

        let mut dispatch_table = self.dispatch_table.write();
        dispatch_table.insert(module_name, client);

        Ok(Response::new(status_code::StatusCode::Success.into()))
    }

    async fn add_node(
        &self,
        request: Request<NodeNetInfo>,
    ) -> Result<Response<StatusCode>, tonic::Status> {
        let multiaddr = request.into_inner().multi_address;
        let (host, port, domain) = parse_multiaddr(&multiaddr).ok_or_else(|| {
            warn!(origin_str = %multiaddr, "parse_multiaddr: not a valid tls multi-address:");
            Status::invalid_argument(status_code::StatusCode::MultiAddrParseError.to_string())
        })?;
        info!(
            multiaddr = %multiaddr,
            host = %host, port = %port, domain = %domain,
            "attempt to add new peer"
        );

        let mut guard = self.peers.write();
        if guard.get_connected_peers().contains(&domain) {
            //add a connected peer
            return Ok(Response::new(
                status_code::StatusCode::AddExistedPeer.into(),
            ));
        }
        if guard.get_known_peers().contains_key(&domain) {
            //add a known peer which is already trying to connect, return success
            return Ok(Response::new(status_code::StatusCode::Success.into()));
        }

        let handle = Peer::init(
            calculate_hash(&format!("{}:{}", &host, port)),
            domain.clone(),
            host.clone(),
            port,
            self.tls_config.clone(),
            self.reconnect_timeout,
            self.inbound_msg_tx.clone(),
        );

        guard.add_known_peers(domain.clone(), handle);

        info!(
            multiaddr = %multiaddr,
            host = %host, port = %port, domain = %domain,
            "peer added: "
        );

        Ok(Response::new(status_code::StatusCode::Success.into()))
    }

    async fn get_peers_net_info(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<TotalNodeNetInfo>, tonic::Status> {
        let mut node_infos: Vec<NodeNetInfo> = vec![];
        let guard = self.peers.read();
        for domain in guard.get_connected_peers().iter() {
            let peer = guard.get_known_peers().get(domain).unwrap();
            let multiaddr = build_multiaddr(peer.host(), peer.port(), domain);
            node_infos.push(NodeNetInfo {
                multi_address: multiaddr.to_string(),
                origin: peer.id(),
            });
        }

        Ok(Response::new(TotalNodeNetInfo { nodes: node_infos }))
    }
}

pub struct NetworkMsgDispatcher {
    inbound_msg_rx: mpsc::Receiver<NetworkMsg>,
    dispatch_table: Arc<RwLock<HashMap<String, NetworkMsgHandlerServiceClient<Channel>>>>,
}

impl NetworkMsgDispatcher {
    async fn run(mut self) {
        while let Some(msg) = self.inbound_msg_rx.recv().await {
            let client = {
                let guard = self.dispatch_table.read();
                guard.get(&msg.module).cloned()
            };

            if let Some(mut client) = client {
                let msg_module = msg.module.clone();
                let msg_origin = msg.origin;
                tokio::spawn(async move {
                    if let Err(e) = client.process_network_msg(msg).await {
                        warn!(
                            msg.module = %msg_module,
                            msg.origin = %msg_origin,
                            error = %e,
                            "registered client processes network msg failed"
                        );
                    }
                });
            } else {
                warn!(
                    %msg.module,
                    %msg.origin,
                    "unregistered module, will drop msg"
                );
            }
        }
    }
}

fn parse_multiaddr(s: &str) -> Option<(String, u16, String)> {
    let multiaddr = s.parse::<MultiAddr>().ok()?;

    let mut iter = multiaddr.iter().peekable();

    while iter.peek().is_some() {
        match iter.peek() {
            Some(Protocol::Ip4(_))
            | Some(Protocol::Ip6(_) | Protocol::Dns4(_) | Protocol::Dns6(_)) => (),
            _ => {
                // ignore is true
                let _ignore = iter.next();
                continue;
            }
        }

        let proto1 = iter.next()?;
        let proto2 = iter.next()?;
        let proto3 = iter.next()?;

        match (proto1, proto2, proto3) {
            (Protocol::Ip4(ip), Protocol::Tcp(port), Protocol::Tls(d)) => {
                return Some((ip.to_string(), port, d.to_string()));
            }
            (Protocol::Ip6(ip), Protocol::Tcp(port), Protocol::Tls(d)) => {
                return Some((ip.to_string(), port, d.to_string()));
            }
            (Protocol::Dns4(ip), Protocol::Tcp(port), Protocol::Tls(d)) => {
                return Some((ip.to_string(), port, d.to_string()));
            }
            (Protocol::Dns6(ip), Protocol::Tcp(port), Protocol::Tls(d)) => {
                return Some((ip.to_string(), port, d.to_string()));
            }
            _ => (),
        }
    }
    None
}

fn build_multiaddr(host: &str, port: u16, domain: &str) -> String {
    // TODO: default to return Dns4 for host, consider if it' appropriate
    vec![
        Protocol::Dns4(host.into()),
        Protocol::Tcp(port),
        Protocol::Tls(domain.into()),
    ]
    .into_iter()
    .collect::<MultiAddr>()
    .to_string()
}

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

async fn try_hot_update(path: &str, network_svc_hot_update: &CitaCloudNetworkServiceServer) {
    if let Ok(new_config) = load_config(path) {
        let known_peers = network_svc_hot_update
            .peers
            .read()
            .get_known_peers()
            .iter()
            .map(|(s, _)| s.to_owned())
            .collect::<Vec<String>>();
        debug!("known peers: {:?}", known_peers);
        let new_peers = new_config
            .peers
            .iter()
            .map(|p| p.domain.to_owned())
            .collect::<Vec<String>>();
        debug!("peers in config file: {:?}", new_peers);
        //try to add node
        for p in new_config.peers {
            if !known_peers.contains(&p.domain) {
                let multiaddr = build_multiaddr(&p.host, p.port, &p.domain);

                let handle = Peer::init(
                    calculate_hash(&format!("{}:{}", &p.host, p.port)),
                    p.domain.clone(),
                    p.host.clone(),
                    p.port,
                    network_svc_hot_update.tls_config.clone(),
                    network_svc_hot_update.reconnect_timeout,
                    network_svc_hot_update.inbound_msg_tx.clone(),
                );

                let mut guard = network_svc_hot_update.peers.write();
                guard.add_known_peers(p.domain.clone(), handle);

                info!(
                    multiaddr = %multiaddr,
                    host = %p.host, port = %p.port, domain = %p.domain,
                    "peer added: "
                );
            }
        }
        //try to delete node
        for p in known_peers {
            if !new_peers.contains(&p) {
                let mut guard = network_svc_hot_update.peers.write();
                guard.delete_peer(p.as_str());
                info!("peer deleted: {}", p);
            }
        }
    } else {
        warn!("load config file: ({}) failed, check format", path);
    };
}

#[cfg(test)]
mod test {
    use super::build_multiaddr;
    use super::parse_multiaddr;

    #[test]
    fn test_build_multiaddr() {
        assert_eq!(
            build_multiaddr("localhost", 80, "fy"),
            "/dns4/localhost/tcp/80/tls/fy"
        );
    }

    #[test]
    fn test_parse_multiaddr() {
        assert!(parse_multiaddr("/ip4/127.0.0.1/tcp/5678/tls/fy").is_some());
    }
}

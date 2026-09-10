use std::{collections::VecDeque, time::{Duration, Instant}};

use crate::{
    common::{SeqId, is_seq_id_past}, error::Error, ping_tracker::PingTracker, raw_msg
};

use rbtl_core::{Status, Event};

use hashbrown::HashSet;
use steamworks::{
    Client as SteamClient,
    networking_sockets::{ListenSocket, NetConnection, NetworkingSockets},
    networking_types::{
        NetworkingConfigEntry, NetworkingConfigValue, NetworkingIdentity, NetworkingMessage, NetConnectionEvent,
        NetConnectionEnd, SendFlags, NetworkingConnectionState
    }
};

/// Represents the internal connection status of the Socket.
#[derive(Debug, Clone, PartialEq)]
pub enum SocketStatus {
    Connecting,
    /// Unfortunately steam doesn't allow us to make the distinction between timeout and an actual network error
    /// so they're both under this status
    LocalError,
    Connected,

    Terminated { by_remote: bool },
}

impl SocketStatus {
    pub fn to_rbtl_status(self) -> Status {
        match self {
            SocketStatus::Connected => Status::Ok,
            SocketStatus::Connecting => Status::Connecting,
            SocketStatus::LocalError => Status::Timeout,
            SocketStatus::Terminated { by_remote } => Status::Ended { by_remote },
        }
    }

    pub fn from_networking_conn_state(net_conn_state: &NetworkingConnectionState) -> Self {
        match net_conn_state {
            NetworkingConnectionState::ClosedByPeer => Self::Terminated { by_remote: true },
            NetworkingConnectionState::Connected => Self::Connected,
            NetworkingConnectionState::FindingRoute => Self::Connecting,
            NetworkingConnectionState::Connecting => Self::Connecting,
            NetworkingConnectionState::None => Self::LocalError,
            NetworkingConnectionState::ProblemDetectedLocally => Self::LocalError
        }
    }

    pub fn can_use_net_conn(&self) -> bool {
        matches!(self, Self::Connecting | Self::Connected)
    }

    pub fn is_connected(&self) -> bool {
        matches!(self, Self::Connected)
    }
}

#[derive(Debug)]
pub enum SocketEvent {
    Data(Box<[u8]>),
    StatusChanged(SocketStatus),
}

impl SocketEvent {
    pub fn to_rbtl_event(self) -> Event {
        match self {
            Self::Data(d) => Event::Data(d),
            Self::StatusChanged(s) => Event::StatusChanged(s.to_rbtl_status())
        }
    }
}

pub struct Socket {
    pub (crate) net_conn: NetConnection,
    // temporary buffer reused to avoid reallocation
    tmp_buff: Vec<u8>,
    ping_tracker: PingTracker,

    events: VecDeque<SocketEvent>,
    status: SocketStatus,
    remote_identity: NetworkingIdentity,
    config: SocketConfig,

    // seq ids that we have received from remote, and need to be acknowledged 
    received_acks_ids: VecDeque<SeqId>,

    next_seq_id: SeqId,
    // seq_ids where we are waiting for a confirmation. only used for `is_seq_id_received`
    waiting_acks: HashSet<SeqId>,
}

#[derive(Debug, Clone)]
pub struct SendOptions {
    pub reliable: bool,
}

impl SendOptions {
    pub fn new() -> Self {
        Self { reliable: true }
    }
}

impl Default for SendOptions {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct SocketConfig {
    pub timeout: Option<Duration>,
}

impl SocketConfig {
    pub fn new() -> Self {
        Self { timeout: None }
    }
}

impl Default for SocketConfig {
    fn default() -> Self {
        Self::new()
    }
}

pub struct SocketCreateParams {
    pub remote_identity: NetworkingIdentity,
    pub virt_port: i32
}

impl SocketCreateParams {
    pub fn new<I: Into<NetworkingIdentity>>(identity: I) -> Self {
        Self {
            remote_identity: identity.into(),
            virt_port: 0
        }
    }

    pub fn with_virt_port(mut self, virt_port: i32) -> Self {
        self.virt_port = virt_port;
        self
    }
}

impl Socket {
    pub fn get_networking_options(timeout: Option<Duration>) -> Vec<NetworkingConfigEntry> {
        let mut options = vec![];
        if let Some(timeout) = timeout {
            let timeout_ms = timeout.as_millis() as i32;
            options.push(NetworkingConfigEntry::new_int32(NetworkingConfigValue::TimeoutConnected, timeout_ms));
            options.push(NetworkingConfigEntry::new_int32(NetworkingConfigValue::TimeoutInitial, timeout_ms));
        }
        options
    }

    pub (crate) fn from_net_conn(net_conn: NetConnection, remote_identity: NetworkingIdentity, config: SocketConfig) -> Self {
        Self {
            net_conn,
            tmp_buff: Vec::new(),
            ping_tracker: PingTracker::new(),
            received_acks_ids: Default::default(),
            events: Default::default(),
            remote_identity,
            config,
            status: SocketStatus::Connecting,
            next_seq_id: 0,
            waiting_acks: HashSet::new(),
        }
    }

    /// Like `from_net_conn`, but instantly started in the connected state
    pub (crate) fn from_listener(net_conn: NetConnection, remote_identity: NetworkingIdentity) -> Self {
        let mut r = Self::from_net_conn(net_conn, remote_identity, Default::default());
        r.update_status(SocketStatus::Connected);
        r
    }

    pub fn new(sockets: &NetworkingSockets, create_params: SocketCreateParams) -> Result<Self, Error> {
        Self::new_with(sockets, create_params, Default::default())
    }

    pub fn new_with(sockets: &NetworkingSockets, params: SocketCreateParams, config: SocketConfig) -> Result<Self, Error> {
        let net_options = Self::get_networking_options(config.timeout);
        log::info!("trying to connect to id {:?}", params.remote_identity);
        let net_conn = sockets.connect_p2p(params.remote_identity.clone(), params.virt_port, net_options)
            .map_err(|e| Error::from_cause("failed to create steamworks-networkingsockets handle", e))?;

        Ok(Self::from_net_conn(net_conn, params.remote_identity, config))
    }

    /// Processes this socket's internals.
    ///
    /// Warning: must not be called if coming from a `Listener`. Nothing is going to happen if you do that,
    /// but the calls will be made twice.
    pub fn process(&mut self) {
        if !self.status.can_use_net_conn() {
            return
        }
        self.net_conn.run_callbacks();
        self.process_incoming_messages();
        self.process_status_change();
    }

    pub (crate) fn process_incoming_messages(&mut self) {
        self.net_conn.receive_messages_with(|networking_message| {
            let msg_flags = networking_message.send_flags();

            let Some(raw_msg_in) = raw_msg::RawMsgIn::decode_from(&mut self.tmp_buff, networking_message.data()) else {
                log::warn!("could not parse rbtl-steamworks msg {:?} from {:?}",
                    networking_message.message_number(),
                    networking_message.identity_peer()
                );
                return;
            };
            for ack_seq_id in raw_msg_in.common.acks() {
                self.waiting_acks.remove(ack_seq_id);
                self.ping_tracker.pong(*ack_seq_id);
            }
            if msg_flags.contains(SendFlags::RELIABLE) {
                self.received_acks_ids.push_back(raw_msg_in.common.seq_id);
            }
            if raw_msg_in.common.has_data {
                self.events.push_back(SocketEvent::Data(self.tmp_buff.clone().into_boxed_slice()));
            }
        })
    }

    fn update_status(&mut self, new_status: SocketStatus) {
        if new_status != self.status {
            self.status = new_status;
            self.events.push_back(SocketEvent::StatusChanged(self.status.clone()));
        }
    }

    fn process_status_change(&mut self) {
        while let Some(event) = self.net_conn.try_receive_event() {
            self.update_status(SocketStatus::from_networking_conn_state(&event.new_state));
        }
    }

    pub fn raw(&self) -> &NetConnection {
        &self.net_conn
    }

    pub fn raw_mut(&mut self) -> &mut NetConnection {
        &mut self.net_conn
    }

    pub fn status(&self) -> SocketStatus {
        self.status.clone()
    }

    pub fn drain_events(&mut self) -> impl Iterator<Item=SocketEvent> {
        self.events.drain(..)
    }

    pub fn next_event(&mut self) -> Option<SocketEvent> {
        self.events.pop_front()
    }

    pub fn avg_ping(&self, seconds: f32) -> Option<f32> {
        self.ping_tracker.avg_ping(seconds)
    }

    pub fn last_ping_info(&self) -> Option<(u32, Instant)> {
        self.ping_tracker.last_ping_info()
    }

    fn get_seq_id(&mut self) -> SeqId {
        let seq_id = self.next_seq_id;
        self.next_seq_id = self.next_seq_id.wrapping_add(1);
        seq_id
    }

    pub fn send_data(&mut self, data: &[u8], send_options: SendOptions) -> Result<SeqId, Error> {
        if !self.status.can_use_net_conn() {
            return Err(Error::new("failed to send p2p steamworks sockets msg: disconnected"));
        }
        let seq_id = self.get_seq_id();
        let send_flags = if send_options.reliable {
            SendFlags::RELIABLE_NO_NAGLE
        } else {
            SendFlags::UNRELIABLE_NO_NAGLE
        };
        if send_options.reliable {
            self.ping_tracker.ping(seq_id);
            self.waiting_acks.insert(seq_id);
        }

        let raw_msg_out = raw_msg::RawMsgOut::new(&mut self.received_acks_ids, seq_id, data);
        raw_msg_out.encode_into(&mut self.tmp_buff);
        self.net_conn.send_message(&self.tmp_buff, send_flags)
            .map_err(|e| Error::from_cause("failed to send p2p steamworks sockets msg", e))?;

        Ok(seq_id)
    }

    pub fn remote_identity(&self) -> NetworkingIdentity {
        self.remote_identity.clone()
    }

    pub fn is_seq_id_received(&self, seq_id: SeqId) -> bool {
        let last_sent_seq_id = self.next_seq_id.wrapping_sub(1);
        is_seq_id_past(last_sent_seq_id, seq_id) && !self.waiting_acks.contains(&seq_id)
    }

    pub (crate) fn apply_config(&mut self) -> bool {
        use steamworks::networking_types::{NetworkingConfigEntry, NetworkingConfigValue};
        if !self.status.can_use_net_conn() {
            return false
        }
        if let Some(timeout) = &self.config.timeout {
            let timeout_ms = timeout.as_millis() as i32;
            let ok1 = self.net_conn.set_config_value(NetworkingConfigEntry::new_int32(
                NetworkingConfigValue::TimeoutInitial, timeout_ms
            ));
            let ok2 = self.net_conn.set_config_value(NetworkingConfigEntry::new_int32(
                NetworkingConfigValue::TimeoutConnected, timeout_ms
            ));
            ok1 && ok2
        } else {
            let ok1 = self.net_conn.unset_config_value(NetworkingConfigValue::TimeoutInitial);
            let ok2 = self.net_conn.unset_config_value(NetworkingConfigValue::TimeoutConnected);
            ok1 && ok2
        }
    }

    pub fn set_config(&mut self, config: SocketConfig) -> bool {
        self.config = config;
        self.apply_config()
    }

    pub fn get_config(&self) -> SocketConfig {
        self.config.clone()
    }

    // Only returns true if the connection is disconnected and we don't have any more events to analyze
    pub fn should_clear(&self) -> bool {
        !self.status.can_use_net_conn() && self.events.is_empty()
    }

    pub fn send_end(&mut self) {
        self.set_terminated(false);
        // self.net_conn.close(NetConnectionEnd::MiscGeneric, None, enable_linger);
        // we can't really do anything here... steamworks's net_conn.close consumes the socket and with good
        // reason: it actually completely frees the underlying socket and makes it unusable
        // so basically this function is useless as-is
    }

    pub fn set_terminated(&mut self, by_remote: bool) {
        self.update_status(SocketStatus::Terminated { by_remote });
    }
}
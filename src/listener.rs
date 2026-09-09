use std::time::{Duration, Instant};

use steamworks::{
    Client as SteamClient, FriendFlags, networking_sockets::{ListenSocket, NetConnection},
    networking_types::{
        NetworkingConfigEntry, NetworkingConfigValue, ConnectionRequest,
        ListenSocketEvent, NetworkingIdentity
    }
};
use hashbrown::HashMap;

use crate::{ConnectInfo, error::Error, socket::{SendOptions, Socket, SocketConfig, SocketEvent}};

#[derive(Debug, Clone)]
pub struct ListenerConfig {
    pub accept_only_friends: bool,
    pub socket_config: SocketConfig,
}

impl ListenerConfig {
    pub fn new() -> Self {
        Self {
            accept_only_friends: false,
            socket_config: SocketConfig::new()
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.socket_config.timeout = Some(timeout);
        self
    }
}

impl Default for ListenerConfig {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Listener {
    steam_client: SteamClient,
    listen_socket: ListenSocket,

    listener_config: ListenerConfig,

    remotes: HashMap<NetworkingIdentity, Socket>,
}

impl Listener {
    pub fn new(steam_client: SteamClient) -> Result<Self, Error> {
        Self::new_with(steam_client, Default::default())
    }

    pub fn new_with(steam_client: SteamClient, listener_config: ListenerConfig) -> Result<Self, Error> {
        let networking_sockets = steam_client.networking_sockets();

        let options = Socket::get_networking_options(listener_config.socket_config.timeout);
        let listen_socket = networking_sockets.create_listen_socket_p2p(0, options)
            .map_err(|e| Error::from_cause("unable to create p2p socket", e))?;

        Ok(Self {
            steam_client,
            listen_socket,
            listener_config,
            remotes: Default::default()
        })
    }

    fn attempt_accept_connecting(&mut self, connection_request: ConnectionRequest) {
        use steamworks::networking_types::NetConnectionEnd;
        println!(
            "received event Connecting: {:?} user_data={}",
            connection_request.remote(),
            connection_request.user_data()
        );

        let remote = connection_request.remote();
        if self.listener_config.accept_only_friends {
            let Some(connecting_steam_id) = remote.steam_id() else {
                log::warn!("rejected connection with {:?} because not from steam, but needs to be friend",
                    remote
                );
                connection_request.reject(NetConnectionEnd::MiscGeneric, Some("not friends: not steam user"));
                return;
            };
            let is_friend = self.steam_client.friends()
                .get_friend(connecting_steam_id)
                .has_friend(FriendFlags::IMMEDIATE);
            if !is_friend {
                log::warn!("rejected connection with {:?} because not friends",
                    remote
                );
                connection_request.reject(NetConnectionEnd::MiscGeneric, Some("not friends"));
                return;
            }
        }
        if let Err(e) = connection_request.accept() {
            log::error!("accepting connection with user {:?} returned {}", remote, e);
        } else {
            log::info!("accepted connection with user {:?}", remote);
        }
    }

    fn process_events(&mut self) {
        while let Some(event) = self.listen_socket.try_receive_event() {
            match event {
                ListenSocketEvent::Connecting(connection_request) => {
                    self.attempt_accept_connecting(connection_request);
                }
                ListenSocketEvent::Connected(connected) => {
                    let remote_identity = connected.remote();
                    let remote = Socket::from_listener(
                        connected.take_connection(),
                        remote_identity.clone(),
                    );
                    self.remotes.insert(remote_identity, remote);
                }
                ListenSocketEvent::Disconnected(disconnected) => {
                    let remote_identity = disconnected.remote();
                    if let Some(remote) = self.remotes.get_mut(&remote_identity) {
                        remote.set_terminated(true);
                    }
                }
            }
        }
    }

    fn process_remotes(&mut self) {
        for (_net_id, remote) in &mut self.remotes {
            // important: do not process events here, it's always done at the listener level for the listener
            remote.net_conn.run_callbacks();
            remote.process_incoming_messages();
        }
    }

    fn cleanup_old_remotes(&mut self) {
        self.remotes.retain(|_, v| !v.should_clear());
    }

    pub fn process(&mut self) {
        self.cleanup_old_remotes();
        self.process_events();
        self.process_remotes();
    }

    fn apply_config(&mut self) -> bool {
        use steamworks::networking_types::{NetworkingConfigEntry, NetworkingConfigValue};
        if let Some(timeout) = &self.listener_config.socket_config.timeout {
            let timeout_ms = timeout.as_millis() as i32;
            let ok1 = self.listen_socket.set_config_value(NetworkingConfigEntry::new_int32(
                NetworkingConfigValue::TimeoutInitial, timeout_ms
            ));
            let ok2 = self.listen_socket.set_config_value(NetworkingConfigEntry::new_int32(
                NetworkingConfigValue::TimeoutConnected, timeout_ms
            ));
            ok1 && ok2
        } else {
            let ok1 = self.listen_socket.unset_config_value(NetworkingConfigValue::TimeoutInitial);
            let ok2 = self.listen_socket.unset_config_value(NetworkingConfigValue::TimeoutConnected);
            ok1 && ok2
        }
    }

    pub fn remotes_len(&self) -> usize {
        self.remotes.len()
    }

    pub fn connected_remotes_len(&self) -> usize {
        self.remotes.iter().filter(|r| r.1.status().is_connected()).count()
    }

    pub fn set_config(&mut self, config: ListenerConfig) {
        self.listener_config = config;
        self.apply_config();
    }

    pub fn get_config(&self) -> ListenerConfig {
        self.listener_config.clone()
    }

    pub fn raw(&self) -> &ListenSocket {
        &self.listen_socket
    }

    pub fn raw_mut(&mut self) -> &mut ListenSocket {
        &mut self.listen_socket
    }

    pub fn iter(&self) -> impl Iterator<Item=(&NetworkingIdentity, &Socket)> {
        self.remotes.iter()
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item=(&NetworkingIdentity, &mut Socket)> {
        self.remotes.iter_mut()
    }

    pub fn get(&self, identity: &NetworkingIdentity) -> Option<&Socket> {
        self.remotes.get(identity)
    }

    pub fn get_mut(&mut self, identity: &NetworkingIdentity) -> Option<&mut Socket> {
        self.remotes.get_mut(identity)
    }

    pub fn connect_info(&self) -> Option<Result<ConnectInfo, Error>> {
        Some(Ok(ConnectInfo::new(self.steam_client.user().steam_id())))
    }

    pub fn send_data(&mut self, data: &[u8], send_options: SendOptions) -> Result<(), Error> {
        let mut err: Option<Error> = None;
        let mut has_success = false;
        for socket in self.remotes.values_mut() {
            match socket.send_data(data, send_options.clone()) {
                Ok(_) => has_success = true,
                Err(e) => err = Some(e),
            }
        }
        if let Some(err) = err {
            if !has_success {
                return Err(err);
            }
        }
        Ok(())
    }
    
    /// Returns an iterator that drain events for all known remotes
    ///
    /// You must call `process` before hand to ensure all the messages are correctly processed internally
    pub fn drain_events<'a>(&'a mut self) -> impl 'a + Iterator<Item=(NetworkingIdentity, SocketEvent)> {
        self.remotes.iter_mut().flat_map(|(addr, socket)| {
            socket.drain_events().map(move |event| (addr.clone(), event) )
        })
    }
}

impl std::ops::Index<NetworkingIdentity> for Listener {
    type Output = Socket;

    fn index<'a>(&'a self, index: NetworkingIdentity) -> &'a Socket {
        self.get(&index).expect("id does not exist for this listener")
    }
}

impl std::ops::IndexMut<NetworkingIdentity> for Listener {
    fn index_mut<'a>(&'a mut self, index: NetworkingIdentity) -> &'a mut Socket {
        self.get_mut(&index).expect("id does not exist for this listener")
    }
}
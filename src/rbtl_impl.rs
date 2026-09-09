use std::sync::Arc;

use rbtl_core::{Client, Event, ServClient, Server, Status, ServerStateError};
use steamworks::{Client as SteamClient, networking_types::NetworkingIdentity};

use crate::{
    Listener, Socket, SocketConfig, SeqId, Error, ListenerConfig, ConnectInfo,
    SocketEvent, SocketInit, SocketStatus, SendOptions
};

impl Client for Socket {
    type Server = Listener;
    type ClientConfig = SocketConfig;
    type ConnectOptions = SocketConfig;
    type StateError = Error;
    type SendError = Error;
    type Init<'a> = SocketInit<'a>;
    type SendOptions = SendOptions;

    fn status(&self) -> Status {
        self.status().to_rbtl_status()
    }

    fn get_config(&self) -> Self::ClientConfig {
        self.get_config()
    }

    fn set_config(&mut self, config: Self::ClientConfig) {
        self.set_config(config);
    }

    /// Drain events aside from the "raw" ones.
    fn drain_events<'a>(&'a mut self) -> impl Iterator<Item=Event> + 'a {
        self.drain_events().map(|e| e.to_rbtl_event())
    }

    fn new<I: Into<Self::Init>>(init: I, options: Self::ConnectOptions) -> Result<Self, Self::StateError> where Self: Sized {
        Self::new_with(init.into(), options)
    }

    fn from_connect_info(connect_info: ConnectInfo, options: Self::ConnectOptions) ->
        Result<Self, Self::StateError> where Self: Sized {
        Socket::connect(connect_info.addr, options)
    }

    fn process(&mut self) {
        let _r = self.process();
    }

    fn ping(&self, seconds: f32) -> Option<f32> {
        self.avg_ping(seconds)
    }

    fn is_msg_received(&self, msg_id: &u32) -> Result<bool, ()> {
        Ok(self.is_seq_id_received(*msg_id))
    }

    fn send<B>(&mut self, bytes: B, send_opts: Self::SendOptions) -> Result<u32, Self::SendError>
            where B: Into<Arc<[u8]>> + AsRef<[u8]> + Clone {
        self.send_data(bytes.as_ref(), send_opts)
    }

    fn end(&mut self) {
        self.send_end();
    }
}

impl ServClient for Socket {
    type Server = Listener;

    fn send<B: Into<Arc<[u8]>> + AsRef<[u8]> + Clone>(&mut self, bytes: B, send_opts: <Self::Server as Server>::SendOptions)
            -> Result<<Self::Server as Server>::MessageId, <Self::Server as Server>::SendError> {
        self.send_data(bytes.as_ref(), send_opts)
    }

    fn is_msg_received(&self, msg_id: &u32) -> Result<bool, ()> {
        Ok(self.is_seq_id_received(*msg_id))
    }

    fn ping(&self, seconds: f32) -> Option<f32> {
        self.avg_ping(seconds)
    }

    fn status(&self) -> Status {
        self.status().to_rbtl_status()
    }
}

impl ServerStateError for Error {
    fn new_unavailable() -> Self where Self: Sized {
        Error::new("unavailable to connect to")
    }

    fn is_unavailable(&self) -> bool {
        self.msg.starts_with("unavailable")
    }
}

impl Server for Listener {
    const RBTL_PROTOCOL_ID: u8 = 33;
    const RBTL_PROTOCOL_NAME: &str = "steamsockets";

    type ServClient = Socket;
    type ConnectingClient = Socket;
    type Init = SteamClient;
    type Key = NetworkingIdentity;
    type SendOptions = SendOptions;
    type SendError = Error;
    type MessageId = u32;
    type ConnectInfo = ConnectInfo;
    type ServerConfig = ListenerConfig;
    type StateError = Error;

    fn drain_events<'a>(&'a mut self) -> impl Iterator<Item=(Self::Key, Event)> + 'a {
        self.drain_events()
            .map(|(id, ev)| (id, ev.to_rbtl_event()))
    }

    fn get(&self, k: &Self::Key) -> Option<&Self::ServClient> {
        self.get(k)
    }

    fn get_mut(&mut self, k: &Self::Key) -> Option<&mut Self::ServClient> {
        self.get_mut(k)
    }

    fn get_config(&self) -> Self::ServerConfig {
        self.get_config()
    }

    fn set_config(&mut self, listener_config: Self::ServerConfig) {
        self.set_config(listener_config);
    }

    fn iter(&self) -> impl Iterator<Item=(&Self::Key, &Self::ServClient)> {
        self.iter()
    }

    fn iter_mut(&mut self) -> impl Iterator<Item=(&Self::Key, &mut Self::ServClient)> {
        self.iter_mut()
    }

    fn connected_len(&self) -> usize {
        self.connected_remotes_len()
    }

    fn len(&self) -> usize {
        self.remotes_len()
    }

    fn connect_info(&self) -> Option<Result<Self::ConnectInfo, Self::StateError>> {
        self.connect_info()
    }

    fn new<I: Into<Self::Init>>(init: I) -> Result<Self, Self::StateError> where Self: Sized {
        let init = init.into();
        Self::new(init)
    }

    fn new_with<I: Into<Self::Init>>(local_addr: I, config: Self::ServerConfig) -> Result<Self, Self::StateError> where Self: Sized {
        let local_addr = local_addr.into();
        Self::new_with(local_addr, config)
    }

    fn process(&mut self) {
        self.process();
    }

    fn end(&mut self) {
        // self.disconnect();
    }

    fn send_all<B>(&mut self, bytes: B, send_opts: Self::SendOptions) -> Result<(), Self::SendError>
            where B: Into<Arc<[u8]>> + AsRef<[u8]> + Clone {
        self.send_data(bytes.as_ref(), send_opts)
    }
}
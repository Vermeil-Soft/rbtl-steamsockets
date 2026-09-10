rbtl::rbtl_structs! {
    [SteamSockets, rbtl_steamsockets::Listener]
}

use rbtl_steamsockets::{ConnectInfo, ListenerConfig};
use steamworks::{Client as SteamClient, SteamId};

const APP_ID: u32 = 480;

fn list_friends(steam: &SteamClient) {
    println!("- available friends: ");
    for f in steam.friends().get_friends(steamworks::FriendFlags::IMMEDIATE) {
        println!("-- {} {:?}: id {:16x}", f.name(), f.id(), f.id().raw());
    }
}

fn spawn_client(steam_id: Option<String>) {
    let steam = SteamClient::init_app(APP_ID).unwrap();
    let Some(steam_id) = steam_id else {
        list_friends(&steam);
        return;
    };

    let steam_id = u64::from_str_radix(&steam_id.trim(), 16).expect("unable to parse steam id from hex");
    let networking = steam.networking_sockets();

    let client_stem = RBTLClientStem { steam_sockets: Some(&networking) };
    let client_connect_info = RBTLClientConnectInfo {
        steam_sockets: Some(ConnectInfo::new(SteamId::from_raw(steam_id))),
        unknown: vec![]
    };
    let mut connector = RBTLConnector::new(&client_stem, client_connect_info, Default::default()).unwrap();
    println!("(client) created");
    let mut client = None;
    for _i in 0..1000 {
        steam.run_callbacks();
        match connector.attempt_connect(&client_stem) {
            None => { std::thread::sleep(std::time::Duration::from_millis(16)); },
            Some(Ok(new_client)) => {
                client = Some(new_client);
                break;
            },
            Some(Err(e)) => {
                panic!("could not connect: {}", e);
            }
        }
    }

    let client = client.unwrap();
    println!("(client) successfully connected with {:?} ({})", client.kind(), client.kind().rbtl_protocol_name());
    let mut client = RBTLAsyncClient::new(client);

    const BYTES: &[u8] = &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
    let mut last_msg_id: Option<RBTLMessageId> = None;
    for i in 0..1000 {
        steam.run_callbacks();
        for ev in client.drain_events() {
            println!("(client) >> new event {:?}", ev);
        }
        if i % 100 == 0 {
            let ping = client.with_lock(|c| c.ping(5.0));
            println!("(client) >> ping = {}ms", ping.unwrap_or(999.0));
        }
        if let Some(msg_id) = last_msg_id.as_ref() {
            if let Ok(true) = client.is_msg_received(&msg_id) {
                println!("(client) message {:?} has arrived", last_msg_id.as_ref().cloned().unwrap());
                last_msg_id = None;
            }
        } else {
            if i % 10 == 0 {
                last_msg_id = client.send(BYTES, Default::default()).ok();
                if last_msg_id.is_some() {
                    println!("(client) sending message {:?}", last_msg_id.as_ref().cloned().unwrap());
                }
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(16));
    }
    drop(client);
    std::thread::sleep(std::time::Duration::from_millis(100));
    drop(networking);
    println!("(client) shutting down");
}

fn spawn_server(only_friend: bool) {
    let steam = SteamClient::init_app(APP_ID).unwrap();

    let serv_stem = RBTLServStem { steam_sockets: Some(steam.clone()) };
    let mut listener_config = ListenerConfig::new();
    listener_config.accept_only_friends = only_friend;
    let create_params = RBTLServCreateParams { steam_sockets: Some(()) };
    let config = RBTLServConfig { steam_sockets: listener_config };
    let mut listener = RBTLListener::new_with(&serv_stem, create_params, config).unwrap();
    println!("(serv) >> starting server");

    const BYTES: &[u8] = &[9, 8, 7, 6, 5, 4, 3, 2, 1, 0];

    for i in 0.. {
        steam.run_callbacks();
        listener.process();

        for (key, ev) in listener.drain_events() {
            println!("(serv) >> new event {:?} from {:?}", ev, key);
        }

        if i % 100 == 0 {
            let _r = listener.send_all(BYTES, Default::default());
            if listener.connected_len() > 0 {
                println!("(serv) sending message to {} remotes...", listener.connected_len());
            }
        }
        listener.post_process();

        std::thread::sleep(std::time::Duration::from_millis(16));
    }
}

fn init_basic_logger() {
    struct Logger;
    impl log::Log for Logger {
        fn enabled(&self, _metadata: &log::Metadata) -> bool { true }

        fn log(&self, record: &log::Record) {
            println!("{}:{}", record.level(), record.args());
        }

        fn flush(&self) {}
    }
    log::set_logger(&Logger).unwrap();
    log::set_max_level(log::LevelFilter::Trace);
}

fn main() {
    init_basic_logger();
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    match args.get(0).map(|s| &**s) {
        Some("client") => spawn_client(args.get(1).cloned()),
        Some("server") => {
            if args.get(1).map(|s| &**s) == Some("onlyfriend") {
                spawn_server(true)
            } else {
                spawn_server(false)
            }
        },
        Some(_) | None => {
            eprintln!("usage:");
            eprintln!("  client [steam_id u64 hex] -> connect to steam_id");
            eprintln!("  client -> lists friends and their steam ids");
            eprintln!("  server <onlyfriend> -> start a server, if onlyfriend is true, only accept connections from friends");
            return;
        }
    }
}
mod ws;
mod aggregator;
mod prefecture;
mod window_aggregator;

use tower_http::cors::CorsLayer;
use crate::{
    ws::make_websocket_handler,
    aggregator::Aggregator,
};
use axum::{
    extract::{ws::WebSocketUpgrade, State},
    routing::get,
    Json,
    Router,
};
use bytes::Bytes;
use server::{
    observation_points::{load_observation_points, ObservationPoint},
};
use std::{
    collections::HashMap,
    net::{Ipv4Addr, SocketAddrV4},
    sync::Arc,
};
use std::collections::BTreeMap;
use axum::extract::Query;
use serde::Deserialize;
use tokio::{
    io::{
        AsyncReadExt, BufReader,
    },
    net::TcpStream,
    sync::{
        broadcast,
        broadcast::Sender,
    },
};
use crate::prefecture::get_prefectures;

pub(crate) struct AppState {
    pub observation_points: Arc<Vec<ObservationPoint>>,
    pub observation_point_map: Arc<BTreeMap<u32, ObservationPoint>>,
    pub tx2: Sender<Vec<(u32, Bytes)>>,
    pub tx3: Sender<Vec<(u32, Bytes)>>,
    pub tx4: Sender<Vec<(u32, Bytes)>>,
    pub tx5: Sender<Vec<(u32, Bytes)>>,
}

impl AppState {
    pub fn get_tx(&self, i: usize) -> &Sender<Vec<(u32, Bytes)>> {
        match i {
            0 => &self.tx2,
            1 => &self.tx3,
            2 => &self.tx4,
            3 => &self.tx5,
            _ => panic!("Invalid index"),
        }
    }
}

#[derive(Deserialize)]
struct Param {
    id: Option<u32>
}


#[tokio::main]
async fn main() {
    let points = load_observation_points("./server/data/observation.csv").unwrap();
    let (tx2, _rx) = broadcast::channel(16);
    let (tx3, _rx) = broadcast::channel(16);
    let (tx4, _rx) = broadcast::channel(16);
    let (tx5, _rx) = broadcast::channel(16);
    let state = Arc::new(AppState {
        observation_point_map: Arc::new(points.iter().map(|x| (x.id(), x.clone())).collect()),
        observation_points: Arc::new(points),
        tx2, tx3, tx4, tx5,
    });

    let cloned_state = state.clone();
    let socket_task = tokio::spawn(async move {
        let mut aggregator = Aggregator::new(cloned_state);

        let socket = TcpStream::connect(SocketAddrV4::new(Ipv4Addr::LOCALHOST, server::PORT)).await.unwrap();
        println!("Connected to server");
        let mut reader = BufReader::new(socket);

        let mut buffer = [0; 256];
        while let Ok(_) = reader.read_exact(&mut buffer[0..3]).await {
            let len = buffer[0] as usize;
            let total_size = 3 + 6 * len;

            let _ = reader.read_exact(&mut buffer[3..total_size]).await;

            let _ = aggregator.on_receive_data(Bytes::copy_from_slice(&buffer[..total_size]));
        }
    });

    let cors = CorsLayer::new().allow_origin(["http://localhost:5173".parse().unwrap()]);

    let app = Router::new()
        .route("/meta", get(meta))
        .route(
            "/ws2",
            get(|ws: WebSocketUpgrade, state: State<Arc<AppState>>, query: Query<Param>| {
                make_websocket_handler(0)(ws, state, query)
            }),
        )
        .route(
            "/ws3",
            get(|ws: WebSocketUpgrade, state: State<Arc<AppState>>, query: Query<Param>| {
                make_websocket_handler(1)(ws, state, query)
            }),
        )
        .route(
            "/ws4",
            get(|ws: WebSocketUpgrade, state: State<Arc<AppState>>, query: Query<Param>| {
                make_websocket_handler(2)(ws, state, query)
            }),
        )
        .route(
            "/ws5",
            get(|ws: WebSocketUpgrade, state: State<Arc<AppState>>, query: Query<Param>| {
                make_websocket_handler(3)(ws, state, query)
            }),
        )
        .layer(cors)
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:6070")
        .await
        .unwrap();
    println!("Listening on {:?}", listener.local_addr().unwrap());
    let listener_task = axum::serve(listener, app);

    tokio::select! {
        _ = socket_task => {
            println!("Socket task exited");
        },
        _ = listener_task => {
            println!("Listener task exited");
        }
    }
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct Meta {
    observation_points: HashMap<u32, ObservationPoint>,
    prefectures: HashMap<u32, String>,
}

async fn meta(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    Json(serde_json::json!(Meta {
        observation_points: state.observation_points.iter().map(|x| (x.id(), x.clone())).collect::<HashMap<u32, ObservationPoint>>(),
        prefectures: get_prefectures()
    }))
}

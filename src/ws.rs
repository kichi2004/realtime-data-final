use crate::{AppState, Param};
use axum::{
    extract::{State, WebSocketUpgrade, ws::WebSocket},
};
use futures_util::{
    SinkExt,
    stream::StreamExt,
    future::BoxFuture
};
use std::sync::Arc;
use axum::{
    extract::ws::Message::Binary,
    response::Response
};
use axum::extract::Query;
use bytes::Bytes;
use tokio::sync::broadcast::Receiver;


pub(crate) fn make_websocket_handler(index: usize) -> impl Fn(WebSocketUpgrade, State<Arc<AppState>>, Query<Param>) -> BoxFuture<'static, Response> {
    move |ws: WebSocketUpgrade, State(state): State<Arc<AppState>>, Query(param): Query<Param>| {
        Box::pin(async move {
            ws.on_upgrade(move |socket| websocket(socket, state.get_tx(index).subscribe(), index, param))
        })
    }
}

async fn websocket(stream: WebSocket, mut rx: Receiver<Vec<(u32, Bytes)>>, index: usize, param: Param) {
    println!("WebSocket connected (#{index}), id = {}", param.id.map_or("-".into(), |x| x.to_string()));
    let (mut sender, _) = stream.split();
    tokio::spawn(async move {
        'outer: loop {
            match rx.recv().await {
                Ok(vec) => {
                    for (id, msg) in vec {
                        if param.id.is_some_and(|x| id != x) {
                            continue;
                        }
                        if let Err(err) = sender.send(Binary(msg)).await {
                            println!("Task error (#{index}): {err:?}");
                            break 'outer;
                        }
                    }
                }
                Err(err) => {
                    println!("Task receive error (#{index}): {:?}", err);
                    break;
                }
            }
        }
    });
}
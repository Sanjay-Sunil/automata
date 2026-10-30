//! The control API (ARCHITECTURE §5.10): HTTP on `127.0.0.1` with a bearer
//! token, for the strategist's tools and the Telegram bot.
//!
//! The server owns no state. Each request becomes a [`Request`] sent to the
//! engine's loop, which answers it between ticks, so nothing here can race
//! the trading path. There is deliberately no endpoint that places, modifies
//! or cancels an order: the most a caller can do is submit a plan (which the
//! engine validates and clamps) or pull the kill switch.
//!
//! | Method | Path | Does |
//! |---|---|---|
//! | GET | `/health` | liveness, no auth |
//! | GET | `/state` | phase, health, feed, plan version, halts |
//! | GET | `/positions` | open trades with stops, unrealized P&L |
//! | GET | `/pnl` | day P&L, trades, win rate, drawdown, risk used |
//! | GET | `/bars?symbol=&n=` | recent bars + indicator snapshot |
//! | GET | `/signals?n=` | recent signals and what risk decided |
//! | GET | `/context` | index levels, VIX, breadth |
//! | GET | `/plan` | the plan in force |
//! | POST | `/plan` | submit a plan → accepted with clamp diff, or 422 |
//! | POST | `/halt` | kill switch: `{"reason": "..."}` |
//! | POST | `/resume` | lift it (refused after 14:45 / after the loss stop) |

use std::net::SocketAddr;
use std::time::Duration;

use axum::Router;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::{mpsc, oneshot};

/// What a caller asked for.
#[derive(Debug, Clone, PartialEq)]
pub enum Request {
    State,
    Positions,
    Pnl,
    Bars {
        symbol: String,
        n: usize,
    },
    Signals {
        n: usize,
    },
    /// Today's trades, open and closed, oldest first.
    Trades,
    Context,
    Plan,
    /// Raw JSON of a submitted plan.
    SubmitPlan(String),
    Halt(String),
    Resume,
}

/// The engine's answer: JSON, or an HTTP status and message.
pub type Reply = Result<Value, (u16, String)>;

pub type Envelope = (Request, oneshot::Sender<Reply>);

/// Sends requests to the engine loop.
#[derive(Clone)]
pub struct Handle {
    tx: mpsc::Sender<Envelope>,
}

impl Handle {
    /// A handle plus the receiver the engine loop answers from.
    pub fn channel(buffer: usize) -> (Handle, mpsc::Receiver<Envelope>) {
        let (tx, rx) = mpsc::channel(buffer);
        (Handle { tx }, rx)
    }

    pub async fn ask(&self, req: Request) -> Reply {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send((req, tx))
            .await
            .map_err(|_| (503, "engine is not running".to_string()))?;
        match tokio::time::timeout(Duration::from_secs(10), rx).await {
            Ok(Ok(r)) => r,
            Ok(Err(_)) => Err((503, "engine dropped the request".into())),
            Err(_) => Err((504, "engine did not answer in 10 s".into())),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ControlError {
    #[error("the control API binds to loopback only, not {0}")]
    NotLoopback(SocketAddr),
    #[error("CONTROL_API_TOKEN must be at least 16 characters")]
    WeakToken,
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Clone)]
struct App {
    handle: Handle,
    token: String,
}

/// Constant-time comparison, so the token cannot be guessed byte by byte.
fn token_ok(headers: &HeaderMap, token: &str) -> bool {
    let Some(given) = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
    else {
        return false;
    };
    let (a, b) = (given.as_bytes(), token.as_bytes());
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn respond(r: Reply) -> Response {
    match r {
        Ok(v) => (StatusCode::OK, axum::Json(v)).into_response(),
        Err((code, msg)) => (
            StatusCode::from_u16(code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            axum::Json(json!({ "error": msg })),
        )
            .into_response(),
    }
}

async fn call(app: &App, headers: &HeaderMap, req: Request) -> Response {
    if !token_ok(headers, &app.token) {
        return respond(Err((401, "missing or wrong bearer token".into())));
    }
    respond(app.handle.ask(req).await)
}

#[derive(Deserialize)]
struct BarsQ {
    symbol: String,
    #[serde(default = "default_n")]
    n: usize,
}

#[derive(Deserialize)]
struct NQ {
    #[serde(default = "default_n")]
    n: usize,
}

fn default_n() -> usize {
    20
}

#[derive(Deserialize, Default)]
struct HaltBody {
    #[serde(default)]
    reason: String,
}

pub fn router(handle: Handle, token: String) -> Router {
    let app = App { handle, token };
    Router::new()
        .route("/health", get(|| async { axum::Json(json!({"ok": true})) }))
        .route(
            "/state",
            get(|State(a): State<App>, h: HeaderMap| async move { call(&a, &h, Request::State).await }),
        )
        .route(
            "/positions",
            get(|State(a): State<App>, h: HeaderMap| async move { call(&a, &h, Request::Positions).await }),
        )
        .route(
            "/pnl",
            get(|State(a): State<App>, h: HeaderMap| async move { call(&a, &h, Request::Pnl).await }),
        )
        .route(
            "/bars",
            get(
                |State(a): State<App>, h: HeaderMap, Query(q): Query<BarsQ>| async move {
                    call(
                        &a,
                        &h,
                        Request::Bars {
                            symbol: q.symbol,
                            n: q.n.min(500),
                        },
                    )
                    .await
                },
            ),
        )
        .route(
            "/signals",
            get(|State(a): State<App>, h: HeaderMap, Query(q): Query<NQ>| async move {
                call(&a, &h, Request::Signals { n: q.n.min(500) }).await
            }),
        )
        .route(
            "/trades",
            get(|State(a): State<App>, h: HeaderMap| async move { call(&a, &h, Request::Trades).await }),
        )
        .route(
            "/context",
            get(|State(a): State<App>, h: HeaderMap| async move { call(&a, &h, Request::Context).await }),
        )
        .route(
            "/plan",
            get(|State(a): State<App>, h: HeaderMap| async move { call(&a, &h, Request::Plan).await }).post(
                |State(a): State<App>, h: HeaderMap, body: String| async move {
                    if body.len() > 256 * 1024 {
                        return respond(Err((413, "plan larger than 256 KiB".into())));
                    }
                    call(&a, &h, Request::SubmitPlan(body)).await
                },
            ),
        )
        .route(
            "/halt",
            post(|State(a): State<App>, h: HeaderMap, body: String| async move {
                let b: HaltBody = serde_json::from_str(&body).unwrap_or_default();
                let reason = if b.reason.trim().is_empty() {
                    "control API".to_string()
                } else {
                    b.reason.chars().take(200).collect()
                };
                call(&a, &h, Request::Halt(reason)).await
            }),
        )
        .route(
            "/resume",
            post(|State(a): State<App>, h: HeaderMap| async move { call(&a, &h, Request::Resume).await }),
        )
        .with_state(app)
}

/// Binds `addr` (loopback only) and serves until the runtime stops.
/// Returns the bound address (useful with port 0).
pub async fn serve(addr: SocketAddr, token: String, handle: Handle) -> Result<SocketAddr, ControlError> {
    if !addr.ip().is_loopback() {
        return Err(ControlError::NotLoopback(addr));
    }
    if token.len() < 16 {
        return Err(ControlError::WeakToken);
    }
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let local = listener.local_addr()?;
    let app = router(handle, token);
    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            tracing::error!(error = %e, "control API stopped");
        }
    });
    tracing::info!(%local, "control API listening");
    Ok(local)
}

#[cfg(test)]
mod tests {
    use super::*;

    const TOKEN: &str = "0123456789abcdef0123";

    async fn start() -> (String, mpsc::Receiver<Envelope>) {
        let (h, rx) = Handle::channel(8);
        let addr = serve("127.0.0.1:0".parse().unwrap(), TOKEN.into(), h).await.unwrap();
        (format!("http://{addr}"), rx)
    }

    /// Answers every request with its debug form.
    fn echo(mut rx: mpsc::Receiver<Envelope>) {
        tokio::spawn(async move {
            while let Some((req, tx)) = rx.recv().await {
                let r = match &req {
                    Request::SubmitPlan(s) if s.contains("bad") => Err((422, "bad plan".into())),
                    other => Ok(json!({ "req": format!("{other:?}") })),
                };
                let _ = tx.send(r);
            }
        });
    }

    #[tokio::test]
    async fn auth_routing_and_errors() {
        let (base, rx) = start().await;
        echo(rx);
        let c = reqwest::Client::new();
        let r = c.get(format!("{base}/health")).send().await.unwrap();
        assert_eq!(r.status(), 200);
        let r = c.get(format!("{base}/state")).send().await.unwrap();
        assert_eq!(r.status(), 401);
        let r = c
            .get(format!("{base}/state"))
            .bearer_auth("0123456789abcdef0124")
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 401);
        let r: Value = c
            .get(format!("{base}/bars?symbol=TCS-EQ&n=9999"))
            .bearer_auth(TOKEN)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(r["req"], r#"Bars { symbol: "TCS-EQ", n: 500 }"#);
        let r = c
            .post(format!("{base}/plan"))
            .bearer_auth(TOKEN)
            .body("bad")
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 422);
        let r: Value = c
            .post(format!("{base}/halt"))
            .bearer_auth(TOKEN)
            .body(r#"{"reason":"news"}"#)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(r["req"], r#"Halt("news")"#);
        // No order endpoints exist.
        for path in ["/order", "/orders", "/place", "/cancel"] {
            let r = c.post(format!("{base}{path}")).bearer_auth(TOKEN).send().await.unwrap();
            assert_eq!(r.status(), 404, "{path}");
        }
    }

    #[tokio::test]
    async fn refuses_public_bind_and_weak_tokens() {
        let (h, _rx) = Handle::channel(1);
        assert!(matches!(
            serve("0.0.0.0:0".parse().unwrap(), TOKEN.into(), h.clone()).await,
            Err(ControlError::NotLoopback(_))
        ));
        assert!(matches!(
            serve("127.0.0.1:0".parse().unwrap(), "short".into(), h).await,
            Err(ControlError::WeakToken)
        ));
    }

    #[tokio::test]
    async fn engine_down_is_503() {
        let (h, rx) = Handle::channel(1);
        drop(rx);
        assert_eq!(h.ask(Request::State).await.unwrap_err().0, 503);
    }
}

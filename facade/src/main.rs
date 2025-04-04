use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

use axum::extract::State;
use axum::response::IntoResponse;
use logging::logging_service_client::LoggingServiceClient;
use logging::{GetLogsRequest, Log};
use reqwest::StatusCode;
use tonic::transport::Endpoint;
use uuid::Uuid;

pub mod logging {
    tonic::include_proto!("logging");
}

use axum::{
    extract::Query,
    routing::{get, post},
    Router,
};

#[derive(Clone)]
struct Client {
    config: Arc<Mutex<reqwest::Client>>,
    config_url: String,
}

impl Client {
    async fn get_possible_addresses(&self, service: &str) -> Vec<String> {
        let mut addresses = Vec::new();

        let response = self
            .config
            .lock()
            .await
            .get(&format!("{}/get_ips/{}", self.config_url, service))
            .send()
            .await;

        if response.is_err() {
            println!(
                "Error while getting a message request: {}",
                response.err().unwrap()
            );
            return addresses;
        }

        let ips = match response.unwrap().json::<HashMap<String, String>>().await {
            Ok(ips) => ips,
            Err(err) => {
                println!("Error while getting a message request: {}", err);
                return addresses;
            }
        };

        for (_, ip) in ips {
            addresses.push(ip);
        }

        addresses
    }
}

async fn create_grpc_client(
    logging_url: &str,
) -> Result<LoggingServiceClient<tonic::transport::Channel>, tonic::transport::Error> {
    let channel = Endpoint::from_shared(logging_url.to_string())?
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(5))
        .tcp_keepalive(Some(Duration::from_secs(30)))
        .connect()
        .await?;

    Ok(LoggingServiceClient::new(channel))
}

use std::future::Future;
use tokio::time::sleep;

async fn retry<T, E: std::fmt::Debug, Fut, F, R, RFut>(
    mut operation: F,
    max_retries: u32,
    delay: Duration,
    mut refresh: Option<R>,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    R: FnMut(&E) -> RFut,
    RFut: Future<Output = ()>,
{
    let mut attempts = 0;
    loop {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(err) => {
                println!("Error on attempt {}: {:?}", attempts + 1, err);
                if attempts >= max_retries {
                    return Err(err);
                }
                if let Some(ref mut refresh_fn) = refresh {
                    refresh_fn(&err).await;
                }
                attempts += 1;
                sleep(delay).await;
            }
        }
    }
}

async fn add_log(
    State(client): State<Client>,
    Query(log): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let uuid = Uuid::new_v4().to_string();
    let message = match log.get("message") {
        Some(msg) => msg.to_string(),
        None => return StatusCode::INTERNAL_SERVER_ERROR,
    };

    let request = tonic::Request::new(Log { uuid, message });

    const MAX_RETRIES: u32 = 3;
    const RETRY_DELAY_MS: u64 = 500;

    let logging_urls = client.get_possible_addresses("logging").await;
    let logging_urls_len = logging_urls.len();
    let current_url_logging = Arc::new(AtomicUsize::new(0));

    let status = retry(
        || async {
            let current = current_url_logging.load(Ordering::Relaxed);
            let grpc_client = create_grpc_client(&("http://".to_owned() + &logging_urls[current])).await;
            match grpc_client {
                Ok(mut client) => client.add_log(request.get_ref().clone()).await,
                Err(err) => Err(tonic::Status::internal(err.to_string())),
            }
        },
        MAX_RETRIES,
        Duration::from_millis(RETRY_DELAY_MS),
        Some(|_: &tonic::Status| {
            let current = current_url_logging.load(Ordering::Relaxed);
            current_url_logging.store((current + 1) % logging_urls_len, Ordering::Relaxed);
            async {}
        }),
    )
    .await;

    match status {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

async fn get_logs(State(client): State<Client>) -> impl IntoResponse {
    let message_client = reqwest::Client::new();
    let message_urls = client.get_possible_addresses("messages").await;
    let current_url_messages = &message_urls[0];
    let message = match message_client.get("http://".to_owned() + current_url_messages).send().await {
        Ok(response) => match response.text().await {
            Ok(msg) => msg,
            Err(err) => {
                println!("Error while getting a message request: {}", err);
                return (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response();
            }
        },
        Err(err) => {
            println!("Error while creating a message request: {}", err);
            return (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response();
        }
    };

    const MAX_RETRIES: u32 = 3;
    const RETRY_DELAY_MS: u64 = 500;

    let logging_urls = client.get_possible_addresses("logging").await;
    let logging_urls_len = logging_urls.len();
    let current_url_logging = Arc::new(AtomicUsize::new(0));
    let logs = tonic::Request::new(GetLogsRequest {});

    let status = retry(
        || async {
            let current = current_url_logging.load(Ordering::Relaxed);
            let grpc_client = create_grpc_client(&("http://".to_owned() + &logging_urls[current])).await;
            match grpc_client {
                Ok(mut client) => client.get_logs(logs.get_ref().clone()).await,
                Err(err) => Err(tonic::Status::internal(err.to_string())),
            }
        },
        MAX_RETRIES,
        Duration::from_millis(RETRY_DELAY_MS),
        Some(|_: &tonic::Status| {
            let current = current_url_logging.load(Ordering::Relaxed);
            current_url_logging.store((current + 1) % logging_urls_len, Ordering::Relaxed);
            async {}
        }),
    )
    .await;

    match status {
        Ok(response) => {
            return (
                StatusCode::OK,
                (message + "\n" + &response.into_inner().logs_string),
            )
                .into_response()
        }
        Err(err) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, err.message().to_string()).into_response();
        }
    }
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let config_url: String;
    if args.len() > 2 {
        println!("Usage: {} <config_url>", args[0]);
        return;
    } else if args.len() == 1 {
        config_url = "http://localhost:8000".to_string();
    } else {
        config_url = args[1].clone();
    }

    let client = Client {
        config: Arc::new(Mutex::new(reqwest::Client::new())),
        config_url,
    };

    let app = Router::new()
        .route("/", get(get_logs))
        .with_state(client.clone())
        .route("/", post(add_log))
        .with_state(client);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:13226")
        .await
        .unwrap();

    axum::serve(listener, app).await.unwrap();
}

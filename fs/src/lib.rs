use reqwest;
use pyo3::prelude::*;
use pyo3::types::*;
use pyo3::exceptions::PyValueError;
use cloud_storage::client::Client;
use cloud_storage::*;
use std::sync::Arc;
use tokio::sync::Mutex;
use std::fmt::Display;
use std::fmt::Formatter;
use std::fs::File;
use std::io::BufReader;


struct GoogleStorageAsyncFS {
    client: Client,
}

#[pyclass(name = "GoogleStorageAsyncFS")]
struct PyGoogleStorageAsyncFS(Arc<Mutex<GoogleStorageAsyncFS>>);

#[pymethods]
impl PyGoogleStorageAsyncFS {
    pub fn read<'p>(&self, py: Python<'p>, url: &'p PyString) -> PyResult<&'p PyAny> {
        let url: String = url.extract()?;
        let url = url.clone();
        if url.starts_with("gs://") {
            match (&url[5..]).find('/') {
                Some(first_slash) => {
                    let inner = self.0.clone();
                    pyo3_asyncio::tokio::future_into_py(py, async move {
                        let bucket = &url[5..(5+first_slash)];
                        let path = &url[(6+first_slash)..];
                        match inner.lock().await.client.object().download(bucket, path).await {
                            Ok(bytes) => {
                                Ok(bytes)
                            }
                            Err(e) => {
                                Err(PyValueError::new_err("HTTP error ".to_owned() + &e.to_string()))
                            }
                        }
                    })
                }
                None => {
                    Err(PyValueError::new_err("bad url ".to_owned() + &url))
                }
            }
        } else {
            Err(PyValueError::new_err("bad url ".to_owned() + &url))
        }
    }
}

#[derive(serde::Serialize)]
struct Claims {
    iss: String,
    scope: String,
    aud: String,
    exp: u64,
    iat: u64,
}

#[derive(serde::Deserialize, Debug)]
// #[allow(dead_code)]
struct TokenResponse {
    access_token: String,
    expires_in: u64,
    // token_type: String,
}

/// This struct contains a token, an expiry, and an access scope.
pub struct Token {
    // this field contains the JWT and the expiry thereof. They are in the same Option because if
    // one of them is `Some`, we require that the other be `Some` as well.
    token: tokio::sync::RwLock<Option<DefaultTokenData>>,
    // store the access scope for later use if we need to refresh the token
    access_scope: String,
}

#[derive(Debug, Clone)]
pub struct DefaultTokenData(String, u64);

impl Display for DefaultTokenData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Default for Token {
    fn default() -> Self {
        Token::new("https://www.googleapis.com/auth/devstorage.full_control")
    }
}

impl Token {
    pub(crate) fn new(scope: &str) -> Self {
        Self {
            token: tokio::sync::RwLock::new(None),
            access_scope: scope.to_string(),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct ApplicationDefaultCredentials {
    client_id: String,
    client_secret: String,
    quota_project_id: String,
    refresh_token: String,
    #[serde(rename="type")]
    _type: String,
}

#[async_trait::async_trait]
impl TokenCache for Token {
    async fn scope(&self) -> String {
        self.access_scope.clone()
    }

    async fn token_and_exp(&self) -> Option<(String, u64)> {
        self.token.read().await.as_ref().map(|d| (d.0.clone(), d.1))
    }

    async fn set_token(&self, token: String, exp: u64) -> crate::Result<()> {
        *self.token.write().await = Some(DefaultTokenData(token, exp));
        Ok(())
    }

    async fn fetch_token(&self, client: &reqwest::Client) -> crate::Result<(String, u64)> {
        let now = now();

        let path = match std::env::var("GOOGLE_APPLICATION_CREDENTIALS") {
            Ok(file) => { Ok(file) }
            Err(_e) => {
                match std::env::var("HOME") {
                    Ok(home) => { Ok(home + "/.config/gcloud/application_default_credentials.json") }
                    Err(e) => { Err(e) }
                }
            }
        }.unwrap();
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let credentials: ApplicationDefaultCredentials = serde_json::from_reader(reader)?;
        let body = [
            ("grant_type", "refresh_token"),
            ("client_id", &credentials.client_id),
            ("client_secret", &credentials.client_secret),
            ("refresh_token", &credentials.refresh_token),
        ];
        let response: TokenResponse = client
            .post("https://www.googleapis.com/oauth2/v4/token")
            .form(&body)
            .send()
            .await?
            .json()
            .await?;
        Ok((response.access_token, now + response.expires_in))
    }
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}


#[pyfunction]
fn create_client() -> PyResult<PyGoogleStorageAsyncFS> {
    let client = Client::with_cache(Token::default());
    return Ok(PyGoogleStorageAsyncFS(Arc::new(Mutex::new(GoogleStorageAsyncFS { client: client }))));
}

struct HTTPAsyncFS {
    client: reqwest::Client,
}

#[pyclass(name = "HTTPAsyncFS")]
struct PyHTTPAsyncFS(Arc<Mutex<HTTPAsyncFS>>);

struct HTTPAsyncFSStream {
    resp: reqwest::Response,
}

#[pyclass(name = "HTTPAsyncFSStream")]
struct PyHTTPAsyncFSStream(Arc<Mutex<HTTPAsyncFSStream>>);

#[pymethods]
impl PyHTTPAsyncFS {
    pub fn open<'p>(&self, py: Python<'p>, url: &'p PyString) -> PyResult<&'p PyAny> {
        let url: String = url.extract()?;
        let inner = self.0.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let resp = inner.lock().await.client.get(url).send().await;
            return match resp {
                Ok(resp) => {
                    Ok(PyHTTPAsyncFSStream(Arc::new(Mutex::new(HTTPAsyncFSStream { resp: resp }))))
                }
                Err(e) => {
                    Err(PyValueError::new_err("HTTP error ".to_owned() + &e.to_string()))
                }
            }
        })
    }
}

#[pymethods]
impl PyHTTPAsyncFSStream {
    pub fn chunk<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        let inner = self.0.clone();
        pyo3_asyncio::tokio::future_into_py(py, async move {
            let maybe_bytes = inner.lock().await.resp.chunk().await;
            match maybe_bytes {
                Ok(Some(maybe_bytes)) => {
                    Ok(Some(maybe_bytes.to_vec()))
                }
                Ok(None) => {
                    Ok(None)
                }
                Err(e) => {
                    Err(PyValueError::new_err("HTTP error ".to_owned() + &e.to_string()))
                }
            }
        })
    }
}

#[pyfunction]
fn create_http_client() -> PyResult<PyHTTPAsyncFS> {
    let client = reqwest::Client::new();
    return Ok(PyHTTPAsyncFS(Arc::new(Mutex::new(HTTPAsyncFS { client: client }))));
}


/// A Python module implemented in Rust. The name of this function must match
/// the `lib.name` setting in the `Cargo.toml`, else Python will not be able to
/// import the module.
#[pymodule]
fn fs(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(create_client, m)?)?;
    m.add_function(wrap_pyfunction!(create_http_client, m)?)?;
    Ok(())
}

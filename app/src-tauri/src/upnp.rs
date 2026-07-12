//! Direct UPnP/DLNA MediaRenderer client (WiiM etc.): SSDP discovery, then
//! AVTransport polling for now-playing metadata + live position, so the UI can
//! show lyrics for whatever the network player is playing. No Home Assistant
//! or other intermediary — we read the device itself.

use serde::Serialize;
use std::sync::Arc;
use std::time::Duration;
use tauri::Emitter;

const AVT: &str = "urn:schemas-upnp-org:service:AVTransport:1";

#[derive(Clone, Serialize, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RemoteState {
    pub device: String,
    pub playing: bool,
    pub title: String,
    pub artist: String,
    pub album: String,
    pub art: String,
    pub position: f64,
    pub duration: f64,
}

#[derive(Clone)]
pub struct RemoteCtl {
    /// (control_url, device_name) once a renderer is found.
    pub target: Arc<tokio::sync::Mutex<Option<(String, String)>>>,
}

impl RemoteCtl {
    pub fn new() -> Self {
        Self {
            target: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }
}

/// Extract the text content of the first `<tag>` or `<ns:tag>` element.
fn tag_content(xml: &str, tag: &str) -> Option<String> {
    // plain <tag>
    let open = format!("<{tag}>");
    if let Some(i) = xml.find(&open) {
        let s = i + open.len();
        if let Some(e) = xml[s..].find(&format!("</{tag}>")) {
            return Some(xml[s..s + e].to_string());
        }
    }
    // namespaced <ns:tag>
    let needle = format!(":{tag}>");
    let mut from = 0;
    while let Some(i) = xml[from..].find(&needle) {
        let abs = from + i;
        if let Some(lt) = xml[..abs].rfind('<') {
            let name = &xml[lt + 1..abs + needle.len() - 1];
            if !name.starts_with('/') && !name.contains(' ') && !name.contains('<') {
                let s = abs + needle.len();
                if let Some(e) = xml[s..].find(&format!("</{name}>")) {
                    return Some(xml[s..s + e].to_string());
                }
            }
        }
        from = abs + needle.len();
    }
    None
}

fn xml_unescape(s: &str) -> String {
    s.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

/// "0:02:35" -> 155.0
fn hms_to_secs(s: &str) -> f64 {
    s.split(':')
        .filter_map(|p| p.parse::<f64>().ok())
        .fold(0.0, |acc, p| acc * 60.0 + p)
}

/// Send an SSDP M-SEARCH and collect MediaRenderer description URLs (blocking).
fn ssdp_search() -> Vec<String> {
    let mut found = Vec::new();
    let Ok(sock) = std::net::UdpSocket::bind(("0.0.0.0", 0)) else {
        return found;
    };
    let _ = sock.set_read_timeout(Some(Duration::from_millis(500)));
    let msearch = format!(
        "M-SEARCH * HTTP/1.1\r\nHOST: 239.255.255.250:1900\r\nMAN: \"ssdp:discover\"\r\nMX: 2\r\nST: urn:schemas-upnp-org:device:MediaRenderer:1\r\n\r\n"
    );
    for _ in 0..2 {
        let _ = sock.send_to(msearch.as_bytes(), ("239.255.255.250", 1900));
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    let mut buf = [0u8; 2048];
    while std::time::Instant::now() < deadline {
        match sock.recv_from(&mut buf) {
            Ok((n, _)) => {
                let resp = String::from_utf8_lossy(&buf[..n]);
                for line in resp.lines() {
                    let lower = line.to_lowercase();
                    if let Some(rest) = lower.strip_prefix("location:") {
                        let url = line[line.len() - rest.trim().len()..].trim().to_string();
                        if !found.contains(&url) {
                            found.push(url);
                        }
                    }
                }
            }
            Err(_) => {}
        }
    }
    found
}

/// Fetch a device description and return (friendly_name, absolute AVTransport control URL).
async fn probe_device(client: &reqwest::Client, location: &str) -> Option<(String, String)> {
    let xml = client
        .get(location)
        .timeout(Duration::from_secs(4))
        .send()
        .await
        .ok()?
        .text()
        .await
        .ok()?;
    let name = tag_content(&xml, "friendlyName").unwrap_or_else(|| "Network player".into());
    let mut control = None;
    for chunk in xml.split("<service>") {
        if chunk.contains(AVT) {
            control = tag_content(chunk, "controlURL");
            break;
        }
    }
    let control = control?;
    // absolutize against the description URL's origin
    let base = reqwest::Url::parse(location).ok()?;
    let ctrl_url = base.join(&control).ok()?.to_string();
    Some((ctrl_url, name))
}

async fn soap(
    client: &reqwest::Client,
    ctrl_url: &str,
    action: &str,
    args: &str,
) -> Option<String> {
    let envelope = format!(
        r#"<?xml version="1.0" encoding="utf-8"?><s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/"><s:Body><u:{action} xmlns:u="{AVT}"><InstanceID>0</InstanceID>{args}</u:{action}></s:Body></s:Envelope>"#
    );
    let resp = client
        .post(ctrl_url)
        .header("SOAPAction", format!("\"{AVT}#{action}\""))
        .header("Content-Type", "text/xml; charset=\"utf-8\"")
        .timeout(Duration::from_secs(4))
        .body(envelope)
        .send()
        .await
        .ok()?;
    if !resp.status().is_success() {
        return None;
    }
    resp.text().await.ok()
}

/// Issue a transport command ("play" | "pause" | "skip") to the current device.
pub async fn command(client: &reqwest::Client, ctl: &RemoteCtl, cmd: &str) -> Result<(), String> {
    let target = ctl.target.lock().await.clone();
    let (ctrl_url, _) = target.ok_or("no network player found")?;
    let (action, args) = match cmd {
        "play" => ("Play", "<Speed>1</Speed>"),
        "pause" => ("Pause", ""),
        "skip" => ("Next", ""),
        _ => return Err(format!("unknown remote command: {cmd}")),
    };
    soap(client, &ctrl_url, action, args)
        .await
        .map(|_| ())
        .ok_or_else(|| format!("{action} failed"))
}

async fn poll_state(
    client: &reqwest::Client,
    ctrl_url: &str,
    device: &str,
) -> Option<RemoteState> {
    let transport = soap(client, ctrl_url, "GetTransportInfo", "").await?;
    let state = tag_content(&transport, "CurrentTransportState").unwrap_or_default();
    let playing = state == "PLAYING" || state == "TRANSITIONING";

    let pos = soap(client, ctrl_url, "GetPositionInfo", "").await?;
    let duration = hms_to_secs(&tag_content(&pos, "TrackDuration").unwrap_or_default());
    let position = hms_to_secs(&tag_content(&pos, "RelTime").unwrap_or_default());
    let didl = xml_unescape(&tag_content(&pos, "TrackMetaData").unwrap_or_default());

    Some(RemoteState {
        device: device.to_string(),
        playing,
        title: xml_unescape(&tag_content(&didl, "title").unwrap_or_default()),
        artist: xml_unescape(
            &tag_content(&didl, "artist")
                .or_else(|| tag_content(&didl, "creator"))
                .unwrap_or_default(),
        ),
        album: xml_unescape(&tag_content(&didl, "album").unwrap_or_default()),
        art: xml_unescape(&tag_content(&didl, "albumArtURI").unwrap_or_default()),
        position,
        duration,
    })
}

/// Background task: discover a MediaRenderer, then poll it every second and
/// emit `remote://state` whenever anything changes.
pub fn start(app: tauri::AppHandle, ctl: RemoteCtl) {
    tauri::async_runtime::spawn(async move {
        let client = reqwest::Client::new();
        let mut last = RemoteState::default();
        let mut failures = 0u32;
        loop {
            let have_target = ctl.target.lock().await.is_some();
            if !have_target {
                let locations =
                    tauri::async_runtime::spawn_blocking(ssdp_search).await.unwrap_or_default();
                let mut best: Option<(String, String)> = None;
                for loc in &locations {
                    if let Some((ctrl_url, name)) = probe_device(&client, loc).await {
                        eprintln!("[upnp] found renderer: {name} @ {ctrl_url}");
                        // prefer a WiiM if several renderers answer
                        let is_wiim = name.to_lowercase().contains("wiim")
                            || name.to_lowercase().contains("speaker");
                        if best.is_none() || is_wiim {
                            best = Some((ctrl_url, name));
                        }
                    }
                }
                if let Some(t) = best {
                    *ctl.target.lock().await = Some(t);
                } else {
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    continue;
                }
            }

            let target = ctl.target.lock().await.clone();
            if let Some((ctrl_url, name)) = target {
                match poll_state(&client, &ctrl_url, &name).await {
                    Some(st) => {
                        failures = 0;
                        if st != last {
                            let _ = app.emit("remote://state", &st);
                            last = st;
                        }
                    }
                    None => {
                        failures += 1;
                        if failures >= 5 {
                            eprintln!("[upnp] {name} unreachable — rediscovering");
                            *ctl.target.lock().await = None;
                            failures = 0;
                            let _ = app.emit("remote://state", &RemoteState::default());
                            last = RemoteState::default();
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });
}

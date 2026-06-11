// Copyright (c) 2021 Alibaba Cloud
// Copyright (c) 2021, 2023 IBM Corporation
// Copyright (c) 2022 Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0
//

use anyhow::{anyhow, bail, Context, Result};
use image_rs::image::ImageClient;
use image_rs::shared_rootfs::{self, SharedRootfsBundleEntry};
use image_rs::vsock_ttrpc_client;
use kata_sys_util::validate::verify_id;
use oci_spec::runtime as oci;
use safe_path::scoped_join;
use serde::Deserialize;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use tokio::sync::Mutex;

use crate::config::ImageCVMRole;
use crate::network::setup_guest_dns;
use crate::rpc::CONTAINER_BASE;
use crate::AGENT_CONFIG;

const KATA_IMAGE_WORK_DIR: &str = "/run/kata-containers/image/";
const CONFIG_JSON: &str = "config.json";
const KATA_PAUSE_BUNDLE: &str = "/pause_bundle";
const K8S_IS_IMAGE_CVM: &str = "io.kata-containers.is-image-cvm";
const NERDCTL_DNS: &str = "nerdctl/dns";
const K8S_CONTAINER_TYPE_KEYS: [&str; 2] = [
    "io.kubernetes.cri.container-type",
    "io.kubernetes.cri-o.ContainerType",
];

#[rustfmt::skip]
lazy_static! {
    pub static ref IMAGE_SERVICE: Arc<Mutex<Option<ImageService>>> = Arc::new(Mutex::new(None));
}

// Convenience function to obtain the scope logger.
fn sl() -> slog::Logger {
    slog_scope::logger().new(o!("subsystem" => "image"))
}

// Function to copy a file if it does not exist at the destination
fn copy_if_not_exists(src: &Path, dst: &Path) -> Result<()> {
    if let Some(dst_dir) = dst.parent() {
        fs::create_dir_all(dst_dir)?;
    }
    fs::copy(src, dst)?;
    Ok(())
}

#[derive(Debug, Deserialize)]
struct NerdctlDNSConfig {
    #[serde(default, rename = "DNSServers")]
    dns_servers: Option<Vec<String>>,
}

fn setup_dns_for_image_pull(image_metadata: &HashMap<String, String>) -> Result<()> {
    let Some(raw_dns) = image_metadata.get(NERDCTL_DNS) else {
        return Ok(());
    };

    let dns_config: NerdctlDNSConfig =
        serde_json::from_str(raw_dns).context("parse nerdctl DNS metadata")?;
    let dns_lines: Vec<String> = dns_config
        .dns_servers
        .unwrap_or_default()
        .into_iter()
        .map(|server| server.trim().to_string())
        .filter(|server| !server.is_empty())
        .map(|server| format!("nameserver {server}"))
        .collect();

    if dns_lines.is_empty() {
        return Ok(());
    }

    info!(sl(), "setup DNS for image pull"; "dns" => format!("{dns_lines:?}"));
    setup_guest_dns(sl(), &dns_lines).context("setup DNS for image pull")
}

pub struct ImageService {
    image_client: ImageClient,
}

impl ImageService {
    pub fn new() -> Self {
        let image_client = ImageClient::new(PathBuf::from(KATA_IMAGE_WORK_DIR));
        #[cfg(feature = "guest-pull")]
        if !AGENT_CONFIG.image_registry_auth.is_empty() {
            let registry_auth = &AGENT_CONFIG.image_registry_auth;
            debug!(sl(), "Set registry auth file {:?}", registry_auth);
            //image_client.config.file_paths.auth_file = registry_auth.clone();
            //image_client.config.auth = true;
        }

        Self { image_client }
    }

    /// pause image is packaged in rootfs
    fn unpack_pause_image(cid: &str) -> Result<String> {
        verify_id(cid).context("The guest pause image cid contains invalid characters.")?;

        let guest_pause_bundle = Path::new(KATA_PAUSE_BUNDLE);
        if !guest_pause_bundle.exists() {
            bail!("Pause image not present in rootfs");
        }
        let guest_pause_config = scoped_join(guest_pause_bundle, CONFIG_JSON)?;
        info!(sl(), "use guest pause image cid {:?}", cid);

        let image_oci = oci::Spec::load(guest_pause_config.to_str().ok_or_else(|| {
            anyhow!(
                "Failed to load the guest pause image config from {:?}",
                guest_pause_config
            )
        })?)
        .context("load image config file")?;

        let image_oci_process = image_oci.process().as_ref().ok_or_else(|| {
            anyhow!("The guest pause image config does not contain a process specification. Please check the pause image.")
        })?;
        info!(
            sl(),
            "pause image oci process {:?}",
            image_oci_process.clone()
        );

        // Ensure that the args vector is not empty before accessing its elements.
        // Check the number of arguments.
        let args = if let Some(args_vec) = image_oci_process.args() {
            args_vec
        } else {
            bail!("The number of args should be greater than or equal to one! Please check the pause image.");
        };

        let pause_bundle = scoped_join(CONTAINER_BASE, cid)?;
        fs::create_dir_all(&pause_bundle)?;
        let pause_rootfs = scoped_join(&pause_bundle, "rootfs")?;
        fs::create_dir_all(&pause_rootfs)?;
        info!(sl(), "pause_rootfs {:?}", pause_rootfs);

        copy_if_not_exists(&guest_pause_config, &pause_bundle.join(CONFIG_JSON))?;
        let arg_path = Path::new(&args[0]).strip_prefix("/")?;
        copy_if_not_exists(
            &guest_pause_bundle.join("rootfs").join(arg_path),
            &pause_rootfs.join(arg_path),
        )?;
        Ok(pause_rootfs.display().to_string())
    }

    /// pull_image is used for call image-rs to pull image in the guest.
    /// # Parameters
    /// - `image`: Image name (exp: quay.io/prometheus/busybox:latest)
    /// - `cid`: Container id
    /// - `image_metadata`: Annotations about the image (exp: "containerd.io/snapshot/cri.layer-digest": "sha256:24fb2886d6f6c5d16481dd7608b47e78a8e92a13d6e64d87d57cb16d5f766d63")
    /// # Returns
    /// - The image rootfs bundle path. (exp. /run/kata-containers/cb0b47276ea66ee9f44cc53afa94d7980b57a52c3f306f68cb034e58d9fbd3c6/rootfs)
    pub async fn pull_image(
        &mut self,
        image: &str,
        cid: &str,
        image_metadata: &HashMap<String, String>,
    ) -> Result<String> {
        info!(sl(), "image metadata: {image_metadata:?}");
        setup_dns_for_image_pull(image_metadata)?;

        //Check whether the image is for sandbox or for container.
        let mut is_sandbox = false;
        for key in K8S_CONTAINER_TYPE_KEYS.iter() {
            if let Some(value) = image_metadata.get(key as &str) {
                if value == "sandbox" {
                    is_sandbox = true;
                    break;
                }
            }
        }

        if is_sandbox {
            let mount_path = Self::unpack_pause_image(cid)?;
            return Ok(mount_path);
        }

        // Image layers will store at KATA_IMAGE_WORK_DIR, generated bundles
        // with rootfs and config.json will store under CONTAINER_BASE/cid/images.
        let bundle_path = scoped_join(CONTAINER_BASE, cid)?;
        fs::create_dir_all(&bundle_path)?;
        info!(sl(), "pull image {image:?}, bundle path {bundle_path:?}");
        let is_image_cvm = image_metadata
            .get(K8S_IS_IMAGE_CVM)
            .map_or(false, |val| val == "true");
        if !is_image_cvm {
            let start = Instant::now();
            let res = self
                .image_client
                .guest_pull_image(image, &bundle_path, &None, &None)
                .await;
            let duration = start.elapsed();
            match res {
                Ok(image) => {
                    info!(
                        sl(),
                        "[MZH]pull and unpack image {image:?}, cid: {cid:?} succeeded.(guest_pull took: {} ms)",
                        duration.as_millis()
                    );
                }
                Err(e) => {
                    error!(
                        sl(),
                        "pull and unpack image {image:?}, cid: {cid:?} failed with {:?}.",
                        e.to_string()
                    );
                    return Err(e);
                }
            };
        } else {
            let marked_shared_rootfs_pending = match shared_rootfs::read_shared_rootfs_cache_entry(
                image,
            ) {
                Ok(Some(_)) => false,
                Ok(None) => match shared_rootfs::mark_shared_rootfs_cache_pending(image) {
                    Ok(()) => true,
                    Err(err) => {
                        warn!(
                            sl(),
                            "failed to mark shared rootfs cache pending before image pull";
                            "image_ref" => image,
                            "error" => format!("{err:#}")
                        );
                        false
                    }
                },
                Err(err) => {
                    warn!(
                        sl(),
                        "shared rootfs cache entry invalid before image pull";
                        "image_ref" => image,
                        "error" => format!("{err:#}")
                    );
                    match shared_rootfs::mark_shared_rootfs_cache_pending(image) {
                        Ok(()) => true,
                        Err(err) => {
                            warn!(
                                sl(),
                                "failed to mark shared rootfs cache pending after invalid entry";
                                "image_ref" => image,
                                "error" => format!("{err:#}")
                            );
                            false
                        }
                    }
                }
            };

            let start = Instant::now();
            let res = self
                .image_client
                .pull_image(image, &bundle_path, &None, &None)
                .await;
            let duration = start.elapsed();
            match res {
                Ok(image_id) => {
                    info!(
                        sl(),
                        "[MZH]pull and unpack image {image_id:?}, cid: {cid:?} succeeded. (pull took: {} ms)",
                        duration.as_millis()
                        );
                    if let Err(err) =
                        shared_rootfs::write_shared_rootfs_bundle_entry(&SharedRootfsBundleEntry {
                            image_ref: image.to_string(),
                            image_id: image_id.clone(),
                            bundle_path: bundle_path.clone(),
                        })
                    {
                        warn!(
                            sl(),
                            "failed to write shared rootfs bundle entry";
                            "image_ref" => image,
                            "image_id" => image_id.clone(),
                            "bundle_path" => bundle_path.display().to_string(),
                            "error" => format!("{err:#}")
                        );
                    }
                    warm_shared_rootfs_cache_async(
                        image.to_string(),
                        image_id,
                        bundle_path.clone(),
                        marked_shared_rootfs_pending,
                    );
                }
                Err(e) => {
                    if marked_shared_rootfs_pending {
                        shared_rootfs::clear_shared_rootfs_cache_pending(image);
                    }
                    error!(
                        sl(),
                        "pull and unpack image {image:?}, cid: {cid:?} failed with {:?}.",
                        e.to_string()
                    );
                    return Err(e);
                }
            };
        }
        let image_bundle_path = scoped_join(&bundle_path, "rootfs")?;
        Ok(image_bundle_path.as_path().display().to_string())
    }
}

fn warm_shared_rootfs_cache_async(
    image_ref: String,
    image_id: String,
    bundle_path: PathBuf,
    owns_pending_marker: bool,
) {
    if shared_rootfs::read_shared_rootfs_cache_entry(&image_ref)
        .map(|entry| entry.is_some())
        .unwrap_or(false)
    {
        if owns_pending_marker {
            shared_rootfs::clear_shared_rootfs_cache_pending(&image_ref);
        }
        info!(sl(), "shared rootfs cache already warm"; "image_ref" => image_ref);
        return;
    }
    if !owns_pending_marker && shared_rootfs::shared_rootfs_cache_pending(&image_ref) {
        info!(sl(), "shared rootfs cache warmup already pending"; "image_ref" => image_ref);
        return;
    }

    thread::spawn(move || {
        let start = Instant::now();
        let result = shared_rootfs::prepare_shared_rootfs_cache_from_bundle(
            &image_ref,
            &image_id,
            &bundle_path,
        );
        if owns_pending_marker {
            shared_rootfs::clear_shared_rootfs_cache_pending(&image_ref);
        }

        match result {
            Ok(entry) => info!(
                sl(),
                "shared rootfs cache warmup completed";
                "image_ref" => image_ref,
                "share_id" => entry.share_id,
                "fs_type" => entry.fs_type,
                "image_size" => entry.image_size,
                "pages" => entry.page_count,
                "elapsed_ms" => start.elapsed().as_millis()
            ),
            Err(err) => warn!(
                sl(),
                "shared rootfs cache warmup failed";
                "image_ref" => image_ref,
                "elapsed_ms" => start.elapsed().as_millis(),
                "error" => format!("{err:#}")
            ),
        }
    });
}

/// Set proxy environment from AGENT_CONFIG
pub async fn set_proxy_env_vars() {
    if env::var("HTTPS_PROXY").is_err() {
        let https_proxy = &AGENT_CONFIG.https_proxy;
        if !https_proxy.is_empty() {
            env::set_var("HTTPS_PROXY", https_proxy);
        }
    }

    match env::var("HTTPS_PROXY") {
        Ok(val) => info!(sl(), "https_proxy is set to: {}", val),
        Err(e) => info!(sl(), "https_proxy is not set ({})", e),
    };

    if env::var("NO_PROXY").is_err() {
        let no_proxy = &AGENT_CONFIG.no_proxy;
        if !no_proxy.is_empty() {
            env::set_var("NO_PROXY", no_proxy);
        }
    }

    match env::var("NO_PROXY") {
        Ok(val) => info!(sl(), "no_proxy is set to: {}", val),
        Err(e) => info!(sl(), "no_proxy is not set ({})", e),
    };
}

/// Init the image service
pub async fn init_image_service() {
    let image_service = ImageService::new();
    *IMAGE_SERVICE.lock().await = Some(image_service);
    if AGENT_CONFIG.image_cvm_role == ImageCVMRole::Runtime {
        let image_cvm_ref = AGENT_CONFIG.image_cvm_ref.clone();
        tokio::spawn(async {
            if image_cvm_ref.is_empty() {
                vsock_ttrpc_client::preconnect_fast_image_share().await;
            } else {
                vsock_ttrpc_client::prefetch_prepare_rootfs_fast(image_cvm_ref).await;
            }
        });
    }
}

pub async fn pull_image(
    image: &str,
    cid: &str,
    image_metadata: &HashMap<String, String>,
) -> Result<String> {
    let image_service = IMAGE_SERVICE.clone();
    let mut image_service = image_service.lock().await;
    let image_service = image_service
        .as_mut()
        .expect("Image Service not initialized");

    image_service.pull_image(image, cid, image_metadata).await
}

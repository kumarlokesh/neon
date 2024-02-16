use std::{
    path::Path,
    process::{Command, Stdio},
    sync::Arc,
};

use compute_api::{requests::UpgradeRequest, responses::ComputeStatus};
use hyper::{Body, Request, StatusCode};
use tracing::info;

use crate::compute::ComputeNode;

use anyhow::{anyhow, Error, Result};

fn upgrade(compute: &Arc<ComputeNode>, pg_version: &str) -> Result<(), Error> {
    let old_bindir = compute.get_my_pg_bindir();
    let new_bindir = compute.get_pg_bindir(pg_version);

    let old_datadir = &compute.pgdata;
    let new_datadir = &Path::new(&compute.pgdata)
        .parent()
        .unwrap()
        .join("new-pgdata")
        .into_os_string()
        .into_string()
        .unwrap();

    // Step 1: Create new cluster
    info!(
        "Running initdb to start a cluster upgrade from v{} to v{}",
        compute.pgversion, pg_version
    );

    let initdb_bin = compute.get_pg_binary(pg_version, "initdb");
    let mut initdb_cmd = Command::new(&initdb_bin);
    match initdb_cmd.args(&["--pgdata", new_datadir]).output() {
        Ok(_) => (),
        Err(_) => return Err(anyhow!("failed to initialize the new database")),
    };

    // Step 2: Run pg_upgrade
    info!(
        "Running pg_upgrade to upgrade from v{} to v{}",
        compute.pgversion, pg_version
    );

    let pg_upgrade_bin = compute.get_pg_binary(pg_version, "pg_upgrade");
    let mut cmd = Command::new(&pg_upgrade_bin);
    let mut child = cmd
        .args(&[
            "--old-bindir",
            &old_bindir.into_os_string().into_string().unwrap(),
        ])
        .args(&["--old-datadir", old_datadir])
        .args(&[
            "--new-bindir",
            &new_bindir.into_os_string().into_string().unwrap(),
        ])
        .args(&["--new-datadir", new_datadir])
        .args(&["--username", "cloud_admin"])
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()?;

    match child.wait() {
        Ok(_) => info!("pg_upgrade was successful"),
        Err(_) => return Err(anyhow!("pg_upgrade failed")),
    }

    // Step 3: Upload to safekeepers

    Ok(())
}

pub async fn handle(
    req: Request<Body>,
    compute: &Arc<ComputeNode>,
) -> Result<(), (Error, StatusCode)> {
    let body_bytes = hyper::body::to_bytes(req.into_body()).await.unwrap();
    let body_str = String::from_utf8(body_bytes.to_vec()).unwrap();

    let body = match serde_json::from_str::<UpgradeRequest>(&body_str) {
        Ok(r) => r,
        Err(e) => return Err((Into::into(e), StatusCode::BAD_REQUEST)),
    };

    // No sense in trying to upgrade to the same version.
    let curr_version = compute.pgversion.clone();
    let new_version = body.pg_version;
    if curr_version == new_version {
        return Err((
            anyhow!("cannot upgrade endpoint to the same version"),
            StatusCode::UNPROCESSABLE_ENTITY,
        ));
    }

    // Check that we are in the running state before trying to upgrade.
    match compute.get_status() {
        ComputeStatus::Prepared => (),
        ComputeStatus::Upgrading => {
            return Err((anyhow!("upgrade already in progress"), StatusCode::CONFLICT));
        }
        _ => {
            return Err((
                anyhow!("expected compute to be in the prepared state"),
                StatusCode::CONFLICT,
            ));
        }
    }

    compute.set_status(ComputeStatus::Upgrading);

    let c = compute.clone();
    tokio::spawn(async move {
        let _ = upgrade(&c, &new_version);

        c.set_status(ComputeStatus::Running);
    });

    Ok(())
}

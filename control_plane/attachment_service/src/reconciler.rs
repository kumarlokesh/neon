use crate::persistence::Persistence;
use crate::service;
use hyper::StatusCode;
use pageserver_api::models::{
    LocationConfig, LocationConfigMode, LocationConfigSecondary, TenantConfig,
};
use pageserver_api::shard::{ShardIdentity, TenantShardId};
use pageserver_client::mgmt_api;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use utils::generation::Generation;
use utils::id::{NodeId, TimelineId};
use utils::lsn::Lsn;
use utils::sync::gate::GateGuard;

use crate::compute_hook::{ComputeHook, NotifyError};
use crate::node::Node;
use crate::tenant_state::{IntentState, ObservedState, ObservedStateLocation};

const DEFAULT_HEATMAP_PERIOD: &str = "60s";

/// Object with the lifetime of the background reconcile task that is created
/// for tenants which have a difference between their intent and observed states.
pub(super) struct Reconciler {
    /// See [`crate::tenant_state::TenantState`] for the meanings of these fields: they are a snapshot
    /// of a tenant's state from when we spawned a reconcile task.
    pub(super) tenant_shard_id: TenantShardId,
    pub(crate) shard: ShardIdentity,
    pub(crate) generation: Option<Generation>,
    pub(crate) intent: TargetState,

    /// Nodes not referenced by [`Self::intent`], from which we should try
    /// to detach this tenant shard.
    pub(crate) detach: Vec<Node>,

    pub(crate) config: TenantConfig,
    pub(crate) observed: ObservedState,

    pub(crate) service_config: service::Config,

    /// A hook to notify the running postgres instances when we change the location
    /// of a tenant.  Use this via [`Self::compute_notify`] to update our failure flag
    /// and guarantee eventual retries.
    pub(crate) compute_hook: Arc<ComputeHook>,

    /// To avoid stalling if the cloud control plane is unavailable, we may proceed
    /// past failures in [`ComputeHook::notify`], but we _must_ remember that we failed
    /// so that we can set [`crate::tenant_state::TenantState::pending_compute_notification`] to ensure a later retry.
    pub(crate) compute_notify_failure: bool,

    /// A means to abort background reconciliation: it is essential to
    /// call this when something changes in the original TenantState that
    /// will make this reconciliation impossible or unnecessary, for
    /// example when a pageserver node goes offline, or the PlacementPolicy for
    /// the tenant is changed.
    pub(crate) cancel: CancellationToken,

    /// Reconcilers are registered with a Gate so that during a graceful shutdown we
    /// can wait for all the reconcilers to respond to their cancellation tokens.
    pub(crate) _gate_guard: GateGuard,

    /// Access to persistent storage for updating generation numbers
    pub(crate) persistence: Arc<Persistence>,
}

/// This is a snapshot of [`crate::tenant_state::IntentState`], but it does not do any
/// reference counting for Scheduler.  The IntentState is what the scheduler works with,
/// and the TargetState is just the instruction for a particular Reconciler run.
#[derive(Debug)]
pub(crate) struct TargetState {
    pub(crate) attached: Option<Node>,
    pub(crate) secondary: Vec<Node>,
}

impl TargetState {
    pub(crate) fn from_intent(nodes: &HashMap<NodeId, Node>, intent: &IntentState) -> Self {
        Self {
            attached: intent.get_attached().map(|n| {
                nodes
                    .get(&n)
                    .expect("Intent attached referenced non-existent node")
                    .clone()
            }),
            secondary: intent
                .get_secondary()
                .iter()
                .map(|n| {
                    nodes
                        .get(n)
                        .expect("Intent secondary referenced non-existent node")
                        .clone()
                })
                .collect(),
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum ReconcileError {
    #[error(transparent)]
    Remote(#[from] mgmt_api::Error),
    #[error(transparent)]
    Notify(#[from] NotifyError),
    #[error("Cancelled")]
    Cancel,
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl Reconciler {
    async fn location_config(
        &mut self,
        node: &Node,
        config: LocationConfig,
        flush_ms: Option<Duration>,
        lazy: bool,
    ) -> Result<(), ReconcileError> {
        self.observed
            .locations
            .insert(node.get_id(), ObservedStateLocation { conf: None });

        // TODO: amend locations that use long-polling: they will hit this timeout.
        let timeout = Duration::from_secs(25);

        tracing::info!("location_config({node}) calling: {:?}", config);
        let tenant_shard_id = self.tenant_shard_id;
        let config_ref = &config;
        match node
            .with_client_retries(
                |client| async move {
                    let config = config_ref.clone();
                    client
                        .location_config(tenant_shard_id, config.clone(), flush_ms, lazy)
                        .await
                },
                &self.service_config.jwt_token,
                1,
                3,
                timeout,
                &self.cancel,
            )
            .await
        {
            Some(Ok(_)) => {}
            Some(Err(e)) => return Err(e.into()),
            None => return Err(ReconcileError::Cancel),
        };
        tracing::info!("location_config({node}) complete: {:?}", config);

        self.observed
            .locations
            .insert(node.get_id(), ObservedStateLocation { conf: Some(config) });

        Ok(())
    }

    fn get_node(&self, node_id: &NodeId) -> Option<&Node> {
        if let Some(node) = self.intent.attached.as_ref() {
            if node.get_id() == *node_id {
                return Some(node);
            }
        }

        if let Some(node) = self
            .intent
            .secondary
            .iter()
            .find(|n| n.get_id() == *node_id)
        {
            return Some(node);
        }

        if let Some(node) = self.detach.iter().find(|n| n.get_id() == *node_id) {
            return Some(node);
        }

        None
    }

    async fn maybe_live_migrate(&mut self) -> Result<(), ReconcileError> {
        let destination = if let Some(node) = &self.intent.attached {
            match self.observed.locations.get(&node.get_id()) {
                Some(conf) => {
                    // We will do a live migration only if the intended destination is not
                    // currently in an attached state.
                    match &conf.conf {
                        Some(conf) if conf.mode == LocationConfigMode::Secondary => {
                            // Fall through to do a live migration
                            node
                        }
                        None | Some(_) => {
                            // Attached or uncertain: don't do a live migration, proceed
                            // with a general-case reconciliation
                            tracing::info!("maybe_live_migrate: destination is None or attached");
                            return Ok(());
                        }
                    }
                }
                None => {
                    // Our destination is not attached: maybe live migrate if some other
                    // node is currently attached.  Fall through.
                    node
                }
            }
        } else {
            // No intent to be attached
            tracing::info!("maybe_live_migrate: no attached intent");
            return Ok(());
        };

        let mut origin = None;
        for (node_id, state) in &self.observed.locations {
            if let Some(observed_conf) = &state.conf {
                if observed_conf.mode == LocationConfigMode::AttachedSingle {
                    // We will only attempt live migration if the origin is not offline: this
                    // avoids trying to do it while reconciling after responding to an HA failover.
                    if let Some(node) = self.get_node(node_id) {
                        if node.is_available() {
                            origin = Some(node.clone());
                            break;
                        }
                    }
                }
            }
        }

        let Some(origin) = origin else {
            tracing::info!("maybe_live_migrate: no origin found");
            return Ok(());
        };

        // We have an origin and a destination: proceed to do the live migration
        tracing::info!("Live migrating {}->{}", origin, destination);
        self.live_migrate(origin, destination.clone()).await?;

        Ok(())
    }

    async fn get_lsns(
        &self,
        tenant_shard_id: TenantShardId,
        node: &Node,
    ) -> anyhow::Result<HashMap<TimelineId, Lsn>> {
        let client =
            mgmt_api::Client::new(node.base_url(), self.service_config.jwt_token.as_deref());

        let timelines = client.timeline_list(&tenant_shard_id).await?;
        Ok(timelines
            .into_iter()
            .map(|t| (t.timeline_id, t.last_record_lsn))
            .collect())
    }

    async fn secondary_download(
        &self,
        tenant_shard_id: TenantShardId,
        node: &Node,
    ) -> Result<(), ReconcileError> {
        // This is not the timeout for a request, but the total amount of time we're willing to wait
        // for a secondary location to get up to date before
        const TOTAL_DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(300);

        // This the long-polling interval for the secondary download requests we send to destination pageserver
        // during a migration.
        const REQUEST_DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(20);

        let started_at = Instant::now();

        loop {
            let (status, progress) = match node
                .with_client_retries(
                    |client| async move {
                        client
                            .tenant_secondary_download(
                                tenant_shard_id,
                                Some(REQUEST_DOWNLOAD_TIMEOUT),
                            )
                            .await
                    },
                    &self.service_config.jwt_token,
                    1,
                    3,
                    REQUEST_DOWNLOAD_TIMEOUT * 2,
                    &self.cancel,
                )
                .await
            {
                None => Err(ReconcileError::Cancel),
                Some(Ok(v)) => Ok(v),
                Some(Err(e)) => {
                    // Give up, but proceed: it's unfortunate if we couldn't freshen the destination before
                    // attaching, but we should not let an issue with a secondary location stop us proceeding
                    // with a live migration.
                    tracing::warn!("Failed to prepare by downloading layers on node {node}: {e})");
                    return Ok(());
                }
            }?;

            if status == StatusCode::OK {
                tracing::info!(
                    "Downloads to {} complete: {}/{} layers, {}/{} bytes",
                    node,
                    progress.layers_downloaded,
                    progress.layers_total,
                    progress.bytes_downloaded,
                    progress.bytes_total
                );
                return Ok(());
            } else if status == StatusCode::ACCEPTED {
                let total_runtime = started_at.elapsed();
                if total_runtime > TOTAL_DOWNLOAD_TIMEOUT {
                    tracing::warn!("Timed out after {}ms downloading layers to {node}.  Progress so far: {}/{} layers, {}/{} bytes",
                        total_runtime.as_millis(),
                        progress.layers_downloaded,
                        progress.layers_total,
                        progress.bytes_downloaded,
                        progress.bytes_total
                    );
                    // Give up, but proceed: an incompletely warmed destination doesn't prevent migration working,
                    // it just makes the I/O performance for users less good.
                    return Ok(());
                }

                // Log and proceed around the loop to retry.  We don't sleep between requests, because our HTTP call
                // to the pageserver is a long-poll.
                tracing::info!(
                    "Downloads to {} not yet complete: {}/{} layers, {}/{} bytes",
                    node,
                    progress.layers_downloaded,
                    progress.layers_total,
                    progress.bytes_downloaded,
                    progress.bytes_total
                );
            }
        }
    }

    async fn await_lsn(
        &self,
        tenant_shard_id: TenantShardId,
        node: &Node,
        baseline: HashMap<TimelineId, Lsn>,
    ) -> anyhow::Result<()> {
        loop {
            let latest = match self.get_lsns(tenant_shard_id, node).await {
                Ok(l) => l,
                Err(e) => {
                    tracing::info!("🕑 Can't get LSNs on node {node} yet, waiting ({e})",);
                    std::thread::sleep(Duration::from_millis(500));
                    continue;
                }
            };

            let mut any_behind: bool = false;
            for (timeline_id, baseline_lsn) in &baseline {
                match latest.get(timeline_id) {
                    Some(latest_lsn) => {
                        tracing::info!("🕑 LSN origin {baseline_lsn} vs destination {latest_lsn}");
                        if latest_lsn < baseline_lsn {
                            any_behind = true;
                        }
                    }
                    None => {
                        // Expected timeline isn't yet visible on migration destination.
                        // (IRL we would have to account for timeline deletion, but this
                        //  is just test helper)
                        any_behind = true;
                    }
                }
            }

            if !any_behind {
                tracing::info!("✅ LSN caught up.  Proceeding...");
                break;
            } else {
                std::thread::sleep(Duration::from_millis(500));
            }
        }

        Ok(())
    }

    pub async fn live_migrate(
        &mut self,
        origin_ps: Node,
        dest_ps: Node,
    ) -> Result<(), ReconcileError> {
        // `maybe_live_migrate` is responsibble for sanity of inputs
        assert!(origin_ps.get_id() != dest_ps.get_id());

        fn build_location_config(
            shard: &ShardIdentity,
            config: &TenantConfig,
            mode: LocationConfigMode,
            generation: Option<Generation>,
            secondary_conf: Option<LocationConfigSecondary>,
        ) -> LocationConfig {
            LocationConfig {
                mode,
                generation: generation.map(|g| g.into().unwrap()),
                secondary_conf,
                tenant_conf: config.clone(),
                shard_number: shard.number.0,
                shard_count: shard.count.literal(),
                shard_stripe_size: shard.stripe_size.0,
            }
        }

        tracing::info!("🔁 Switching origin node {origin_ps} to stale mode",);

        // FIXME: it is incorrect to use self.generation here, we should use the generation
        // from the ObservedState of the origin pageserver (it might be older than self.generation)
        let stale_conf = build_location_config(
            &self.shard,
            &self.config,
            LocationConfigMode::AttachedStale,
            self.generation,
            None,
        );
        self.location_config(&origin_ps, stale_conf, Some(Duration::from_secs(10)), false)
            .await?;

        let baseline_lsns = Some(self.get_lsns(self.tenant_shard_id, &origin_ps).await?);

        // If we are migrating to a destination that has a secondary location, warm it up first
        if let Some(destination_conf) = self.observed.locations.get(&dest_ps.get_id()) {
            if let Some(destination_conf) = &destination_conf.conf {
                if destination_conf.mode == LocationConfigMode::Secondary {
                    tracing::info!("🔁 Downloading latest layers to destination node {dest_ps}",);
                    self.secondary_download(self.tenant_shard_id, &dest_ps)
                        .await?;
                }
            }
        }

        // Increment generation before attaching to new pageserver
        self.generation = Some(
            self.persistence
                .increment_generation(self.tenant_shard_id, dest_ps.get_id())
                .await?,
        );

        let dest_conf = build_location_config(
            &self.shard,
            &self.config,
            LocationConfigMode::AttachedMulti,
            self.generation,
            None,
        );

        tracing::info!("🔁 Attaching to pageserver {dest_ps}");
        self.location_config(&dest_ps, dest_conf, None, false)
            .await?;

        if let Some(baseline) = baseline_lsns {
            tracing::info!("🕑 Waiting for LSN to catch up...");
            self.await_lsn(self.tenant_shard_id, &dest_ps, baseline)
                .await?;
        }

        tracing::info!("🔁 Notifying compute to use pageserver {dest_ps}");

        // During a live migration it is unhelpful to proceed if we couldn't notify compute: if we detach
        // the origin without notifying compute, we will render the tenant unavailable.
        while let Err(e) = self.compute_notify().await {
            match e {
                NotifyError::Fatal(_) => return Err(ReconcileError::Notify(e)),
                _ => {
                    tracing::warn!(
                        "Live migration blocked by compute notification error, retrying: {e}"
                    );
                }
            }
        }

        // Downgrade the origin to secondary.  If the tenant's policy is PlacementPolicy::Single, then
        // this location will be deleted in the general case reconciliation that runs after this.
        let origin_secondary_conf = build_location_config(
            &self.shard,
            &self.config,
            LocationConfigMode::Secondary,
            None,
            Some(LocationConfigSecondary { warm: true }),
        );
        self.location_config(&origin_ps, origin_secondary_conf.clone(), None, false)
            .await?;
        // TODO: we should also be setting the ObservedState on earlier API calls, in case we fail
        // partway through.  In fact, all location conf API calls should be in a wrapper that sets
        // the observed state to None, then runs, then sets it to what we wrote.
        self.observed.locations.insert(
            origin_ps.get_id(),
            ObservedStateLocation {
                conf: Some(origin_secondary_conf),
            },
        );

        tracing::info!("🔁 Switching to AttachedSingle mode on node {dest_ps}",);
        let dest_final_conf = build_location_config(
            &self.shard,
            &self.config,
            LocationConfigMode::AttachedSingle,
            self.generation,
            None,
        );
        self.location_config(&dest_ps, dest_final_conf.clone(), None, false)
            .await?;
        self.observed.locations.insert(
            dest_ps.get_id(),
            ObservedStateLocation {
                conf: Some(dest_final_conf),
            },
        );

        tracing::info!("✅ Migration complete");

        Ok(())
    }

    async fn maybe_refresh_observed(&mut self) -> Result<(), ReconcileError> {
        // If the attached node has uncertain state, read it from the pageserver before proceeding: this
        // is important to avoid spurious generation increments.
        //
        // We don't need to do this for secondary/detach locations because it's harmless to just PUT their
        // location conf, whereas for attached locations it can interrupt clients if we spuriously destroy/recreate
        // the `Timeline` object in the pageserver.

        let Some(attached_node) = self.intent.attached.as_ref() else {
            // Nothing to do
            return Ok(());
        };

        if matches!(
            self.observed.locations.get(&attached_node.get_id()),
            Some(ObservedStateLocation { conf: None })
        ) {
            let tenant_shard_id = self.tenant_shard_id;
            let observed_conf = match attached_node
                .with_client_retries(
                    |client| async move { client.get_location_config(tenant_shard_id).await },
                    &self.service_config.jwt_token,
                    1,
                    1,
                    Duration::from_secs(5),
                    &self.cancel,
                )
                .await
            {
                Some(Ok(observed)) => Some(observed),
                Some(Err(mgmt_api::Error::ApiError(status, _msg)))
                    if status == StatusCode::NOT_FOUND =>
                {
                    None
                }
                Some(Err(e)) => return Err(e.into()),
                None => return Err(ReconcileError::Cancel),
            };
            tracing::info!("Scanned location configuration on {attached_node}: {observed_conf:?}");
            match observed_conf {
                Some(conf) => {
                    // Pageserver returned a state: update it in observed.  This may still be an indeterminate (None) state,
                    // if internally the pageserver's TenantSlot was being mutated (e.g. some long running API call is still running)
                    self.observed
                        .locations
                        .insert(attached_node.get_id(), ObservedStateLocation { conf });
                }
                None => {
                    // Pageserver returned 404: we have confirmation that there is no state for this shard on that pageserver.
                    self.observed.locations.remove(&attached_node.get_id());
                }
            }
        }

        Ok(())
    }

    /// Reconciling a tenant makes API calls to pageservers until the observed state
    /// matches the intended state.
    ///
    /// First we apply special case handling (e.g. for live migrations), and then a
    /// general case reconciliation where we walk through the intent by pageserver
    /// and call out to the pageserver to apply the desired state.
    pub(crate) async fn reconcile(&mut self) -> Result<(), ReconcileError> {
        // Prepare: if we have uncertain `observed` state for our would-be attachement location, then refresh it
        self.maybe_refresh_observed().await?;

        // Special case: live migration
        self.maybe_live_migrate().await?;

        // If the attached pageserver is not attached, do so now.
        if let Some(node) = self.intent.attached.as_ref() {
            // If we are in an attached policy, then generation must have been set (null generations
            // are only present when a tenant is initially loaded with a secondary policy)
            debug_assert!(self.generation.is_some());
            let Some(generation) = self.generation else {
                return Err(ReconcileError::Other(anyhow::anyhow!(
                    "Attempted to attach with NULL generation"
                )));
            };

            let mut wanted_conf = attached_location_conf(
                generation,
                &self.shard,
                &self.config,
                !self.intent.secondary.is_empty(),
            );
            match self.observed.locations.get(&node.get_id()) {
                Some(conf) if conf.conf.as_ref() == Some(&wanted_conf) => {
                    // Nothing to do
                    tracing::info!(node_id=%node.get_id(), "Observed configuration already correct.")
                }
                observed => {
                    // In all cases other than a matching observed configuration, we will
                    // reconcile this location.  This includes locations with different configurations, as well
                    // as locations with unknown (None) observed state.

                    // The general case is to increment the generation.  However, there are cases
                    // where this is not necessary:
                    // - if we are only updating the TenantConf part of the location
                    // - if we are only changing the attachment mode (e.g. going to attachedmulti or attachedstale)
                    //   and the location was already in the correct generation
                    let increment_generation = match observed {
                        None => true,
                        Some(ObservedStateLocation { conf: None }) => true,
                        Some(ObservedStateLocation {
                            conf: Some(observed),
                        }) => {
                            let generations_match = observed.generation == wanted_conf.generation;

                            use LocationConfigMode::*;
                            let mode_transition_requires_gen_inc =
                                match (observed.mode, wanted_conf.mode) {
                                    // Usually the short-lived attachment modes (multi and stale) are only used
                                    // in the case of [`Self::live_migrate`], but it is simple to handle them correctly
                                    // here too.  Locations are allowed to go Single->Stale and Multi->Single within the same generation.
                                    (AttachedSingle, AttachedStale) => false,
                                    (AttachedMulti, AttachedSingle) => false,
                                    (lhs, rhs) => lhs != rhs,
                                };

                            !generations_match || mode_transition_requires_gen_inc
                        }
                    };

                    if increment_generation {
                        let generation = self
                            .persistence
                            .increment_generation(self.tenant_shard_id, node.get_id())
                            .await?;
                        self.generation = Some(generation);
                        wanted_conf.generation = generation.into();
                    }
                    tracing::info!(node_id=%node.get_id(), "Observed configuration requires update.");

                    // Because `node` comes from a ref to &self, clone it before calling into a &mut self
                    // function: this could be avoided by refactoring the state mutated by location_config into
                    // a separate type to Self.
                    let node = node.clone();

                    // Use lazy=true, because we may run many of Self concurrently, and do not want to
                    // overload the pageserver with logical size calculations.
                    self.location_config(&node, wanted_conf, None, true).await?;
                    self.compute_notify().await?;
                }
            }
        }

        // Configure secondary locations: if these were previously attached this
        // implicitly downgrades them from attached to secondary.
        let mut changes = Vec::new();
        for node in &self.intent.secondary {
            let wanted_conf = secondary_location_conf(&self.shard, &self.config);
            match self.observed.locations.get(&node.get_id()) {
                Some(conf) if conf.conf.as_ref() == Some(&wanted_conf) => {
                    // Nothing to do
                    tracing::info!(node_id=%node.get_id(), "Observed configuration already correct.")
                }
                _ => {
                    // In all cases other than a matching observed configuration, we will
                    // reconcile this location.
                    tracing::info!(node_id=%node.get_id(), "Observed configuration requires update.");
                    changes.push((node.clone(), wanted_conf))
                }
            }
        }

        // Detach any extraneous pageservers that are no longer referenced
        // by our intent.
        for node in &self.detach {
            changes.push((
                node.clone(),
                LocationConfig {
                    mode: LocationConfigMode::Detached,
                    generation: None,
                    secondary_conf: None,
                    shard_number: self.shard.number.0,
                    shard_count: self.shard.count.literal(),
                    shard_stripe_size: self.shard.stripe_size.0,
                    tenant_conf: self.config.clone(),
                },
            ));
        }

        for (node, conf) in changes {
            if self.cancel.is_cancelled() {
                return Err(ReconcileError::Cancel);
            }
            self.location_config(&node, conf, None, false).await?;
        }

        Ok(())
    }

    pub(crate) async fn compute_notify(&mut self) -> Result<(), NotifyError> {
        // Whenever a particular Reconciler emits a notification, it is always notifying for the intended
        // destination.
        if let Some(node) = &self.intent.attached {
            let result = self
                .compute_hook
                .notify(
                    self.tenant_shard_id,
                    node.get_id(),
                    self.shard.stripe_size,
                    &self.cancel,
                )
                .await;
            if let Err(e) = &result {
                // It is up to the caller whether they want to drop out on this error, but they don't have to:
                // in general we should avoid letting unavailability of the cloud control plane stop us from
                // making progress.
                tracing::warn!("Failed to notify compute of attached pageserver {node}: {e}");
                // Set this flag so that in our ReconcileResult we will set the flag on the shard that it
                // needs to retry at some point.
                self.compute_notify_failure = true;
            }
            result
        } else {
            Ok(())
        }
    }
}

/// We tweak the externally-set TenantConfig while configuring
/// locations, using our awareness of whether secondary locations
/// are in use to automatically enable/disable heatmap uploads.
fn ha_aware_config(config: &TenantConfig, has_secondaries: bool) -> TenantConfig {
    let mut config = config.clone();
    if has_secondaries {
        if config.heatmap_period.is_none() {
            config.heatmap_period = Some(DEFAULT_HEATMAP_PERIOD.to_string());
        }
    } else {
        config.heatmap_period = None;
    }
    config
}

pub(crate) fn attached_location_conf(
    generation: Generation,
    shard: &ShardIdentity,
    config: &TenantConfig,
    has_secondaries: bool,
) -> LocationConfig {
    LocationConfig {
        mode: LocationConfigMode::AttachedSingle,
        generation: generation.into(),
        secondary_conf: None,
        shard_number: shard.number.0,
        shard_count: shard.count.literal(),
        shard_stripe_size: shard.stripe_size.0,
        tenant_conf: ha_aware_config(config, has_secondaries),
    }
}

pub(crate) fn secondary_location_conf(
    shard: &ShardIdentity,
    config: &TenantConfig,
) -> LocationConfig {
    LocationConfig {
        mode: LocationConfigMode::Secondary,
        generation: None,
        secondary_conf: Some(LocationConfigSecondary { warm: true }),
        shard_number: shard.number.0,
        shard_count: shard.count.literal(),
        shard_stripe_size: shard.stripe_size.0,
        tenant_conf: ha_aware_config(config, true),
    }
}

//! Part of Safekeeper pretending to be Postgres, i.e. handling Postgres
//! protocol commands.

use crate::json_ctrl::{handle_json_ctrl, AppendLogicalMessage};
use crate::receive_wal::ReceiveWalConn;
use crate::safekeeper::{AcceptorProposerMessage, ProposerAcceptorMessage};
use crate::send_wal::ReplicationConn;
use crate::timeline::Timeline;
use crate::{GlobalTimelines, SafeKeeperConf};
use anyhow::{bail, Context, Result};

use postgres_ffi::PG_TLI;
use regex::Regex;
use std::sync::Arc;
use tracing::info;
use utils::{
    lsn::Lsn,
    postgres_backend::{self, PostgresBackend},
    pq_proto::{BeMessage, FeStartupPacket, RowDescriptor, INT4_OID, TEXT_OID},
    zid::{ZTenantId, ZTenantTimelineId, ZTimelineId},
};

/// Safekeeper handler of postgres commands
pub struct SafekeeperPostgresHandler {
    pub conf: SafeKeeperConf,
    /// assigned application name
    pub appname: Option<String>,
    pub ztenantid: Option<ZTenantId>,
    pub ztimelineid: Option<ZTimelineId>,
    pub timeline: Option<Arc<Timeline>>,
}

/// Parsed Postgres command.
enum SafekeeperPostgresCommand {
    StartWalPush,
    StartReplication { start_lsn: Lsn },
    IdentifySystem,
    JSONCtrl { cmd: AppendLogicalMessage },
}

fn parse_cmd(cmd: &str) -> Result<SafekeeperPostgresCommand> {
    if cmd.starts_with("START_WAL_PUSH") {
        Ok(SafekeeperPostgresCommand::StartWalPush)
    } else if cmd.starts_with("START_REPLICATION") {
        let re =
            Regex::new(r"START_REPLICATION(?: PHYSICAL)? ([[:xdigit:]]+/[[:xdigit:]]+)").unwrap();
        let mut caps = re.captures_iter(cmd);
        let start_lsn = caps
            .next()
            .map(|cap| cap[1].parse::<Lsn>())
            .context("failed to parse start LSN from START_REPLICATION command")??;
        Ok(SafekeeperPostgresCommand::StartReplication { start_lsn })
    } else if cmd.starts_with("IDENTIFY_SYSTEM") {
        Ok(SafekeeperPostgresCommand::IdentifySystem)
    } else if cmd.starts_with("JSON_CTRL") {
        let cmd = cmd.strip_prefix("JSON_CTRL").context("invalid prefix")?;
        Ok(SafekeeperPostgresCommand::JSONCtrl {
            cmd: serde_json::from_str(cmd)?,
        })
    } else {
        bail!("unsupported command {}", cmd);
    }
}

impl postgres_backend::Handler for SafekeeperPostgresHandler {
    // ztenant id and ztimeline id are passed in connection string params
    fn startup(&mut self, _pgb: &mut PostgresBackend, sm: &FeStartupPacket) -> Result<()> {
        if let FeStartupPacket::StartupMessage { params, .. } = sm {
            if let Some(options) = params.options_raw() {
                for opt in options {
                    match opt.split_once('=') {
                        Some(("ztenantid", value)) => {
                            self.ztenantid = Some(value.parse()?);
                        }
                        Some(("ztimelineid", value)) => {
                            self.ztimelineid = Some(value.parse()?);
                        }
                        _ => continue,
                    }
                }
            }

            if let Some(app_name) = params.get("application_name") {
                self.appname = Some(app_name.to_owned());
            }

            Ok(())
        } else {
            bail!("Safekeeper received unexpected initial message: {:?}", sm);
        }
    }

    fn process_query(&mut self, pgb: &mut PostgresBackend, query_string: &str) -> Result<()> {
        let cmd = parse_cmd(query_string)?;

        info!(
            "got query {:?} in timeline {:?}",
            query_string, self.ztimelineid
        );

        let tenantid = self.ztenantid.context("tenantid is required")?;
        let timelineid = self.ztimelineid.context("timelineid is required")?;
        if self.timeline.is_none() {
            self.timeline = Some(GlobalTimelines::get(ZTenantTimelineId::new(
                tenantid, timelineid,
            )));
        }

        match cmd {
            SafekeeperPostgresCommand::StartWalPush => ReceiveWalConn::new(pgb)
                .run(self)
                .context("failed to run ReceiveWalConn"),
            SafekeeperPostgresCommand::StartReplication { start_lsn } => ReplicationConn::new(pgb)
                .run(self, pgb, start_lsn)
                .context("failed to run ReplicationConn"),
            SafekeeperPostgresCommand::IdentifySystem => self.handle_identify_system(pgb),
            SafekeeperPostgresCommand::JSONCtrl { ref cmd } => handle_json_ctrl(self, pgb, cmd),
        }
        .context(format!("timeline {timelineid}"))?;

        Ok(())
    }
}

impl SafekeeperPostgresHandler {
    pub fn new(conf: SafeKeeperConf) -> Self {
        SafekeeperPostgresHandler {
            conf,
            appname: None,
            ztenantid: None,
            ztimelineid: None,
            timeline: None,
        }
    }

    /// Shortcut for calling `process_msg` in the timeline.
    pub fn process_safekeeper_msg(
        &self,
        msg: &ProposerAcceptorMessage,
    ) -> Result<Option<AcceptorProposerMessage>> {
        self.timeline
            .as_ref()
            .unwrap()
            .process_msg(msg)
            .context("failed to process ProposerAcceptorMessage")
    }

    ///
    /// Handle IDENTIFY_SYSTEM replication command
    ///
    fn handle_identify_system(&mut self, pgb: &mut PostgresBackend) -> Result<()> {
        let tli = self.timeline.as_ref().unwrap();

        let lsn = if self.is_walproposer_recovery() {
            // walproposer should get all local WAL until flush_lsn
            tli.get_flush_lsn()?
        } else {
            // other clients shouldn't get any uncommitted WAL
            tli.get_state()?.0.commit_lsn
        }
        .to_string();

        let sysid = tli.get_state()?.1.server.system_id.to_string();
        let lsn_bytes = lsn.as_bytes();
        let tli = PG_TLI.to_string();
        let tli_bytes = tli.as_bytes();
        let sysid_bytes = sysid.as_bytes();

        pgb.write_message_noflush(&BeMessage::RowDescription(&[
            RowDescriptor {
                name: b"systemid",
                typoid: TEXT_OID,
                typlen: -1,
                ..Default::default()
            },
            RowDescriptor {
                name: b"timeline",
                typoid: INT4_OID,
                typlen: 4,
                ..Default::default()
            },
            RowDescriptor {
                name: b"xlogpos",
                typoid: TEXT_OID,
                typlen: -1,
                ..Default::default()
            },
            RowDescriptor {
                name: b"dbname",
                typoid: TEXT_OID,
                typlen: -1,
                ..Default::default()
            },
        ]))?
        .write_message_noflush(&BeMessage::DataRow(&[
            Some(sysid_bytes),
            Some(tli_bytes),
            Some(lsn_bytes),
            None,
        ]))?
        .write_message(&BeMessage::CommandComplete(b"IDENTIFY_SYSTEM"))?;
        Ok(())
    }

    /// Returns true if current connection is a replication connection, originating
    /// from a walproposer recovery function. This connection gets a special handling:
    /// safekeeper must stream all local WAL till the flush_lsn, whether committed or not.
    pub fn is_walproposer_recovery(&self) -> bool {
        self.appname == Some("wal_proposer_recovery".to_string())
    }
}

import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import EnvVar, Nothing
from .._helpers import _sling_connection_url
from .._helpers import _ensure_incremental_target_indexes


horizons_db_connection = SlingConnectionResource(
    name="HORIZONS_DB",
    type="postgres",
    connection_string=_sling_connection_url("HORIZONS_K8S_URL"),
)


horizons_replication_config = {
    "source": "HORIZONS_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "horizons.{stream_table}",
    },

    "streams": {
        "public.*": None,

        # Mutable tables are full-refresh (all are <25k rows). Horizons writes rows
        # without bumping updated_at (soft deletes, Joe fraud fields,
        # screenshot/readme URLs), mutates "append-only" tables
        # (transactions.fulfilled_at, user_sessions.expires_at), and hard-
        # deletes rows -- incremental mirrors of these tables drifted on
        # thousands of rows. Incremental also never backfills columns added
        # to a select list, and keeps stale column types (projects broke on
        # 2026-10-01 when hours_justification outgrew varchar(500)).
        "public.projects": {
            "select": [
                "-id", "-name", "-description", "-playable_url", "-code_url",
                "-image_url", "-hackatime_names", "-status", "-shipped",
                "-total_hours", "-shipped_at", "-reviewed",
                "-past_approved_hours", "-coins_earned", "-admin_feedback",
                "-hour_justification", "-reviewed_at",
                "-reviewed_by_user_id", "-fraud_flag", "-baseline_hours",
                "-hackatime_hours", "-airtable_record_id", "-bricks_earned",
                "-ysws_record_id", "-parent_project_id", "-ship_kind",
                "-blocked", "-last_shipped_hours", "-reship_update",
            ],
        },
        "public.sticker_tokens": {
            "select": [
                "id", "email", "rsvpNumber", "isUsed",
                "usedAt", "createdAt", "updatedAt",
            ],  # Excludes token
        },
        "public.submissions": {
            "select": [
                "submission_id", "project_id", "approved_hours",
                "approval_status", "reviewed_by", "reviewed_at",
                "created_at", "updated_at", "hackatime_hours",
                "airtable_rec_id", "finalized_at", "pending_send_email",
                "review_passed", "silent_reject", "claim_heartbeat_at",
                "claimed_at", "claimed_by_id",
            ],
        },
        "public.users": {
            "select": [
                "user_id", "email", "first_name", "last_name", "birthday",
                # `role` (scalar text) was dropped upstream and replaced by
                # `roles` (text[]). Sling stringifies Postgres arrays, so this
                # lands in the warehouse as text holding an array literal, e.g.
                # '{user}' -- same shape as fallout.users.roles / macondo.users.roles.
                "roles", "onboard_complete", "onboarded_at", "address_line_1",
                "address_line_2", "city", "state", "country", "zip_code",
                "airtable_rec_id", "hackatime_account", "created_at",
                "updated_at", "referral_code", "raffle_pos", "is_fraud",
                "is_sus", "slack_user_id", "hca_id", "verification_status",
            ],  # Excludes hackatime_access_token, admin_comment, banned_reason
        },
        "public.hackatime_link_otps": {
            "select": [
                "id", "user_id", "email", "expires_at",
                "is_used", "used_at", "created_at",
            ],  # Excludes otp_code
        },

        # --- Incremental: large and verified drift-free ---
        "public.submission_audit_logs": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "created_at",
        },
        "public.user_daily_activity": {
            "mode": "incremental",
            "primary_key": ["id"],
            "update_key": "updated_at",
        },

        # --- Disabled: infrastructure ---
        "public._prisma_migrations": {"disabled": True},
        "public.pg_stat_statements": {"disabled": True},
        "public.pg_stat_statements_info": {"disabled": True},
    }
}



@dg.asset(
    name="horizons_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def horizons_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Horizons DB → warehouse in a single shot."""
    context.log.info("Starting Horizons → warehouse Sling replication")
    _ensure_incremental_target_indexes(context, horizons_replication_config)

    for _ in sling.replicate(
        context=context,
        replication_config=horizons_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None

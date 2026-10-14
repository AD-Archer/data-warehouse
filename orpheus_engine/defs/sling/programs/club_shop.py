import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import EnvVar, Nothing


club_shop_db_connection = SlingConnectionResource(
    name="CLUB_SHOP_DB",
    type="postgres",
    connection_string=EnvVar("CLUB_SHOP_DATABASE_URL"),
)


club_shop_replication_config = {
    "source": "CLUB_SHOP_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "club_shop.{stream_table}",
    },

    "streams": {
        "public.__clubs_migrations": {"disabled": True},
        "public.airtable_sync_state": None,
        "public.ambassadors": None,
        "public.app_settings": None,
        "public.audit_log": None,
        "public.budget_lines": None,
        # notify_review_back / notify_workshop_decision / weekly_digest_email
        # were dropped upstream (clubs-platform migration 0092) and replaced by
        # per-user users.email_opt_outs. Nothing sensitive here, so mirror all.
        "public.club_settings": None,
        "public.clubs": None,
        "public.email_sends": None,
        "public.email_templates": None,
        "public.explore_items": None,
        "public.external_ships": None,
        "public.hcb_card_grants": None,
        "public.hcb_category_rules": None,
        "public.hcb_disbursements": None,
        "public.hcb_org_snapshots": None,
        "public.hcb_orgs": None,
        "public.hcb_sync_runs": None,
        "public.hcb_tokens": {
            "select": ["-access_token", "-refresh_token"],
        },
        "public.hcb_transaction_overrides": None,
        "public.hcb_transactions": None,
        "public.help_faqs": None,
        "public.leaders": None,
        "public.login_codes": {"disabled": True},
        "public.members": None,
        "public.order_grants": None,
        "public.order_items": None,
        "public.orders": None,
        "public.outbox": None,
        "public.review_templates": None,
        "public.role_invites": None,
        "public.sessions": {"disabled": True},
        "public.ship_drafts": None,
        "public.shipments": None,
        "public.ships": None,
        "public.shop_item_favorites": None,
        "public.shop_item_stars": None,
        "public.shop_item_suggestions": None,
        "public.shop_items": None,
        "public.support_messages": None,
        "public.support_threads": None,
        "public.sync_heartbeat": None,
        "public.token_balances": None,
        "public.token_ledger": None,
        "public.trust_ratings": None,
        "public.unified_records": None,
        "public.user_activity_days": None,
        "public.user_roles": None,
        "public.users": {
            "select": ["-hackatime_access_token"],
        },
        "public.workshop_applications": None,
        "public.workshop_club_access": None,
        "public.workshop_projects": None,
        "public.workshops": None,
    },
}



@dg.asset(
    name="club_shop_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def club_shop_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Club Shop DB → warehouse in a single shot."""
    context.log.info("Starting Club Shop → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=club_shop_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None

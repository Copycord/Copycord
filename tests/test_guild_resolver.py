from server.guild_resolver import GuildResolver


def _create_mapping(db, *, mapping_id="resolver-test", status="active"):
    db.upsert_guild_mapping(
        mapping_id=mapping_id,
        mapping_name="Resolver Test",
        original_guild_id=111,
        original_guild_name="Source Server",
        original_guild_icon_url=None,
        cloned_guild_id=222,
        cloned_guild_name="Clone Server",
    )
    db.update_mapping_status(mapping_id, status)


def test_clones_for_host_reads_sqlite_rows(db):
    _create_mapping(db)

    assert GuildResolver(db).clones_for_host(111) == {222}


def test_clones_for_host_skips_paused_sqlite_rows(db):
    _create_mapping(db, status="paused")

    assert GuildResolver(db).clones_for_host(111) == set()

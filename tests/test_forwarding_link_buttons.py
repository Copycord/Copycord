"""
Link buttons on Discord webhook forwards.

Webhooks not owned by an application may only send non-interactive
components (link buttons), and only when the url carries
with_components=true. Custom emoji are dropped because they do not render
outside the source server.

Delivery runs through the real send path against a recording session, since
the rule only accepts discord.com webhook urls.
"""
import asyncio
import os
import tempfile

_tmpdir = tempfile.mkdtemp()
os.environ.setdefault("DATA_DIR", _tmpdir)
os.environ.setdefault("DB_PATH", os.path.join(_tmpdir, "test.db"))
os.environ.setdefault("LOG_LEVEL", "WARNING")

from copycord.selfclient.enums import MessageType  # noqa: E402
from copycord.selfclient.models import Message  # noqa: E402

from copycord.client.forwarding import (  # noqa: E402
    ForwardingFilters,
    ForwardingJob,
    ForwardingManager,
    ForwardingRule,
    _sanitize_link_button_rows,
    build_http_payload,
)

HOOK = "https://discord.com/api/webhooks/111/tok"
EMOJI = {"id": "123456789012345678", "name": "kakobuy", "animated": False}


def _link(label, url, emoji=EMOJI, **extra):
    b = {"type": 2, "style": 5, "label": label, "url": url}
    if emoji is not None:
        b["emoji"] = emoji
    b.update(extra)
    return b


def _row(*buttons):
    return {"type": 1, "id": 1, "components": list(buttons)}


def _source_components():
    """The two rows of the example message, every button with a custom emoji."""
    return [
        _row(
            _link("KakoBuy", "https://www.kakobuy.com/item?id=1"),
            _link("SuperBuy", "https://www.superbuy.com/item?id=1"),
            _link("OOPBuy", "https://oopbuy.com/item?id=1"),
            _link("CSSBuy", "https://www.cssbuy.com/item?id=1"),
            _link("JoyaGoo", "https://joyagoo.com/item?id=1"),
        ),
        _row(
            _link("LitBuy", "https://litbuy.com/item?id=1"),
            _link("SugarGoo", "https://www.sugargoo.com/item?id=1"),
            _link("Raw", "https://weidian.com/item.html?itemID=1"),
            _link("QC", "https://qc.example.com/1"),
            _link("Report", "https://report.example.com/1"),
        ),
    ]


# Sanitizer


def test_link_buttons_are_kept_without_emoji():
    rows = _sanitize_link_button_rows(_source_components())
    assert len(rows) == 2
    assert [b["label"] for b in rows[0]["components"]] == [
        "KakoBuy", "SuperBuy", "OOPBuy", "CSSBuy", "JoyaGoo",
    ]
    assert [b["label"] for b in rows[1]["components"]] == [
        "LitBuy", "SugarGoo", "Raw", "QC", "Report",
    ]
    for row in rows:
        assert row == {"type": 1, "components": row["components"]}
        for b in row["components"]:
            assert set(b) == {"type", "style", "label", "url"}
            assert b["type"] == 2 and b["style"] == 5
    assert rows[0]["components"][0]["url"] == "https://www.kakobuy.com/item?id=1"


def test_extra_button_fields_are_dropped():
    rows = _sanitize_link_button_rows(
        [_row(_link("Go", "https://a.example/x", disabled=True, custom_id="nope", id=7))]
    )
    assert rows == [
        {"type": 1, "components": [
            {"type": 2, "style": 5, "label": "Go", "url": "https://a.example/x"}
        ]}
    ]


def test_interactive_buttons_and_selects_are_dropped():
    rows = _sanitize_link_button_rows(
        [
            _row(
                {"type": 2, "style": 1, "label": "Click", "custom_id": "c1"},
                {"type": 2, "style": 4, "label": "Danger", "custom_id": "c2"},
                {"type": 2, "style": 6, "sku_id": "1"},
                _link("Keep", "https://keep.example/"),
            ),
            _row({"type": 3, "custom_id": "s", "options": [{"label": "a", "value": "a"}]}),
        ]
    )
    assert rows == [
        {"type": 1, "components": [
            {"type": 2, "style": 5, "label": "Keep", "url": "https://keep.example/"}
        ]}
    ]


def test_emoji_only_button_is_labelled_with_the_host():
    rows = _sanitize_link_button_rows(
        [_row(_link("", "https://www.kakobuy.com/item?id=1"), {
            "type": 2, "style": 5, "url": "https://qc.example.com/1", "emoji": EMOJI,
        })]
    )
    labels = [b["label"] for b in rows[0]["components"]]
    assert labels == ["www.kakobuy.com", "qc.example.com"]
    assert all("emoji" not in b for b in rows[0]["components"])


def test_long_labels_are_clipped_to_80():
    rows = _sanitize_link_button_rows([_row(_link("x" * 200, "https://a.example/"))])
    assert len(rows[0]["components"][0]["label"]) == 80


def test_rows_and_buttons_are_capped_at_five():
    many = [_link(f"b{i}", f"https://a.example/{i}") for i in range(8)]
    rows = _sanitize_link_button_rows([_row(*many) for _ in range(7)])
    assert len(rows) == 5
    assert all(len(r["components"]) == 5 for r in rows)
    assert [b["label"] for b in rows[0]["components"]] == ["b0", "b1", "b2", "b3", "b4"]


def test_non_http_urls_are_dropped_and_empty_rows_go():
    rows = _sanitize_link_button_rows(
        [
            _row(
                _link("js", "javascript:alert(1)"),
                _link("discord", "discord://-/channels/1/2"),
                _link("ftp", "ftp://files.example/x"),
                _link("blank", ""),
                _link("nohost", "https://"),
                _link("long", "https://a.example/" + "x" * 600),
            ),
            _row(_link("ok", "http://plain.example/")),
        ]
    )
    assert rows == [
        {"type": 1, "components": [
            {"type": 2, "style": 5, "label": "ok", "url": "http://plain.example/"}
        ]}
    ]


def test_non_row_top_level_components_are_ignored():
    container = {
        "type": 17,
        "components": [
            {"type": 10, "content": "hi"},
            _row(_link("Inner", "https://inner.example/")),
        ],
    }
    section = {"type": 9, "accessory": _link("Acc", "https://acc.example/")}
    loose_button = _link("Loose", "https://loose.example/")
    rows = _sanitize_link_button_rows(
        [container, section, loose_button, "junk", None, _row(_link("Kept", "https://k.example/"))]
    )
    assert [b["label"] for r in rows for b in r["components"]] == ["Kept"]


def test_bad_input_gives_no_rows():
    assert _sanitize_link_button_rows(None) == []
    assert _sanitize_link_button_rows({}) == []
    assert _sanitize_link_button_rows([]) == []
    assert _sanitize_link_button_rows([{"type": 1, "components": None}]) == []


# Attributes


class _Guild:
    id = 5001


class _Channel:
    id = 4001
    name = "general"


class _Author:
    id = 6001
    name = display_name = "Tunmise"
    bot = True
    roles = []


class _Message:
    def __init__(self, components=None):
        self.id = 90001
        self.type = MessageType.default
        self._data = {"type": 0}
        if components is not None:
            self._data["components"] = components
        self.content = ""
        self.guild = _Guild()
        self.channel = _Channel()
        self.author = _Author()
        self.attachments = []
        self.embeds = []
        self.jump_url = "https://discord.com/channels/1/2/3"

    @property
    def components(self):
        return Message.components.fget(self)

    def is_system(self):
        return Message.is_system(self)


def test_attributes_carry_the_raw_components():
    comps = _source_components()
    attrs = ForwardingManager._get_message_attributes(None, _Message(comps))
    assert attrs["components"] == comps


def test_attributes_default_to_no_components():
    attrs = ForwardingManager._get_message_attributes(None, _Message())
    assert attrs["components"] == []


def test_http_provider_payload_is_unchanged():
    rule = ForwardingRule(
        rule_id="r", guild_id=5001, label="http", provider="http", enabled=True,
        config={"url": "https://example.com/hook"},
        filters=ForwardingFilters.from_dict({}),
    )
    payload = build_http_payload(rule, {"content": "hi", "components": _source_components()})
    assert "components" not in payload


# Delivery


class _Resp:
    status = 204
    headers = {}

    async def text(self):
        return ""

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class _Session:
    def __init__(self):
        self.posts = []

    def post(self, url, json=None):
        self.posts.append((url, json))
        return _Resp()


class _Db:
    def __init__(self):
        self.events = []

    def has_forwarding_event(self, **kw):
        return False

    def record_forwarding_event(self, **kw):
        self.events.append(kw)


def _send(attrs, urls):
    rule = ForwardingRule(
        rule_id="rule-wh", guild_id=5001, label="wh", provider="discord",
        enabled=True, config={"urls": urls},
        filters=ForwardingFilters.from_dict({}),
    )
    job = ForwardingJob(provider_queue="discord", rule=rule, message_id="90001", attrs=attrs)
    session = _Session()

    async def main():
        mgr = ForwardingManager(config=None, db=_Db(), ws=None)
        await mgr._send_discord_webhook(rule=rule, attrs=attrs, session=session, job=job)

    asyncio.run(main())
    return session.posts, job


def _wh_attrs(**kw):
    attrs = {
        "message_id": 90001,
        "channel_name": "general",
        "content": "",
        "embeds": [{"title": "Item", "description": "desc"}],
        "attachments": [],
    }
    attrs.update(kw)
    return attrs


def test_post_carries_components_and_the_query_param():
    posts, job = _send(_wh_attrs(components=_source_components()), [HOOK])
    assert len(posts) == 1
    url, payload = posts[0]
    assert url == HOOK + "?with_components=true"
    assert payload["components"] == _sanitize_link_button_rows(_source_components())
    assert payload["embeds"] == [{"title": "Item", "description": "desc"}]
    assert "emoji" not in repr(payload["components"])
    assert job.delivered_urls == {HOOK}


def test_existing_thread_id_is_kept():
    hook = HOOK + "?thread_id=987"
    posts, job = _send(_wh_attrs(components=_source_components()), [hook])
    assert posts[0][0] == HOOK + "?thread_id=987&with_components=true"
    assert job.delivered_urls == {hook}


def test_param_is_not_added_twice():
    hook = HOOK + "?with_components=true"
    posts, _ = _send(_wh_attrs(components=_source_components()), [hook])
    assert posts[0][0] == hook


def test_every_fanout_url_gets_the_param():
    hooks = [HOOK, "https://discord.com/api/webhooks/222/tok2?thread_id=5"]
    posts, _ = _send(_wh_attrs(components=_source_components()), hooks)
    assert sorted(u for u, _ in posts) == sorted(
        [HOOK + "?with_components=true",
         "https://discord.com/api/webhooks/222/tok2?thread_id=5&with_components=true"]
    )


def _baseline(attrs, urls):
    attrs = dict(attrs)
    attrs.pop("components", None)
    return _send(attrs, urls)[0]


def test_no_link_buttons_means_no_change():
    hook = HOOK + "?thread_id=987"
    interactive_only = [_row({"type": 2, "style": 1, "label": "Click", "custom_id": "c"})]
    for comps in ([], None, interactive_only):
        attrs = _wh_attrs(components=comps)
        posts, _ = _send(attrs, [hook])
        assert posts == _baseline(attrs, [hook])
        url, payload = posts[0]
        assert url == hook
        assert "components" not in payload


def test_buttons_alone_still_get_the_placeholder_content():
    posts, _ = _send(
        _wh_attrs(embeds=[], components=_source_components()), [HOOK]
    )
    _, payload = posts[0]
    assert payload["content"] == "New message"
    assert len(payload["components"]) == 2

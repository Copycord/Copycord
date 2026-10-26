from copycord.server.mention_strip import strip_embed_mentions, strip_mentions


def test_everyone_and_here_are_removed():
    assert strip_mentions("Hey @everyone check this out") == "Hey check this out"
    assert strip_mentions("@here new drop") == "new drop"
    assert strip_mentions("ping @everyone @here") == "ping"


def test_everyone_already_neutralized_is_removed():
    assert strip_mentions("Hey @​everyone and @​here") == "Hey and"


def test_role_names_are_removed():
    out = strip_mentions("out @Moderator now", role_names=["Moderator"])
    assert out == "out now"


def test_user_names_are_removed_longest_first():
    out = strip_mentions(
        "thanks @Bob Smith and @Bob", user_names=["Bob", "Bob Smith"]
    )
    assert out == "thanks and"


def test_name_needs_a_word_boundary():
    assert strip_mentions("hi @Bobby", user_names=["Bob"]) == "hi @Bobby"


def test_unknown_at_names_stay():
    assert strip_mentions("hi @Someone", user_names=["Bob"]) == "hi @Someone"


def test_raw_tokens_are_removed():
    assert strip_mentions("a <@123> b <@!456> c <@&789> d") == "a b c d"


def test_channel_mentions_stay():
    assert strip_mentions("see <#123> @everyone") == "see <#123>"


def test_email_and_bare_at_untouched():
    text = "mail a@b.com or bob@everyone.com, use @ here"
    assert strip_mentions(text, user_names=["b"]) == text


def test_punctuation_after_a_removed_mention():
    assert strip_mentions("Hey @everyone, look!") == "Hey, look!"


def test_empty_lines_left_behind_are_dropped():
    text = "@everyone\nfirst line\n\nsecond  line\n<@&1>"
    assert strip_mentions(text) == "first line\n\nsecond  line"


def test_only_mentions_gives_empty_string():
    assert strip_mentions("@everyone <@1> @Mod", role_names=["Mod"]) == ""


def test_none_and_empty_pass_through():
    assert strip_mentions(None) is None
    assert strip_mentions("") == ""


def test_embed_fields():
    embed = {
        "title": "@everyone news",
        "description": "for @Moderator and @Alice",
        "author": {"name": "<@1> bot"},
        "footer": {"text": "sent to @here"},
        "fields": [
            {"name": "@Alice", "value": "hi <@&2>", "inline": False},
            {"name": "plain", "value": "mail a@b.com"},
        ],
    }
    out = strip_embed_mentions(embed, user_names=["Alice"], role_names=["Moderator"])
    assert out["title"] == "news"
    assert out["description"] == "for and"
    assert out["author"]["name"] == "bot"
    assert out["footer"]["text"] == "sent to"
    assert out["fields"][0] == {"name": "​", "value": "hi", "inline": False}
    assert out["fields"][1] == {"name": "plain", "value": "mail a@b.com"}


def test_embed_without_text_parts():
    assert strip_embed_mentions({"type": "rich"}) == {"type": "rich"}

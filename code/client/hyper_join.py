import asyncio
from typing import Tuple, List
from time import perf_counter
from threading import Thread
import aiohttp
from discord.http import Route


class HyperCheck:
    def __init__(self, bot, config, logger):
        self.bot = bot
        self.config = config
        self.logger = logger

        self._bot_blacklist = (
            "722196398635745312",
            "553610702439579669",
            "1003836018237120512",
            "641385620530266125",
            "642660768755613706",
            "876527983698001991",
            "880454049370083329",
            "718493970652594217",
            "725801889874313358",
            "880082829566967848",
            "947546187878236233",
            "843107317129019412",
            "557628352828014614",
            "508391840525975553",
            "576395920787111936",
            "1012079902310277222",
            "851885408328482896",
            "980269252663713822",
            "663515546557480990",
            "955466249482150018",
            "735147814878969968",
            "991459501242847373",
            "603541055530598400",
            "294882584201003009",
            "961746288628600922",
            "718501137484873748",
            "270904126974590976",
            "368521195940741122",
            "530082442967646230",
            "675996677366218774",
            "474841349968101386",
            "490039330388180992",
            "512227974893010954",
            "717716451699589143",
            "696870234262339614",
            "789822495141658674",
            "574652751745777665",
            "534589798267224065",
            "623545336484462593",
            "620191140473470976",
            "566009524574617600",
            "704810036547026954",
            "318312854816161792",
            "957635842631950379",
            "426537812993638400",
            "700070794444669039",
            "825617171589759006",
            "710034409214181396",
        )

        self._verify_list_high = (
            "rule",
            "verify",
            "verifi",
            "unlock",
            "reglas",
            "règle",
            "Regeln",
            "regole",
            "regras",
            "regolamento",
            "regels",
            "vérifier",
            "überprüfen",
            "verificación",
            "verifiëren",
            "проверять",
            "確認する",
            "검증하다",
            "验证",
            "verificación",
            "membership",
            "vérification",
            "Überprüfung",
            "verifica",
            "проверка",
            "access",
            "验证",
            "検証",
            "التحقق",
            "vefication",
            "सत्यापन",
        )

        self._verify_list = (
            "✅",
            "✔",
            "✓",
            "🔒",
            "🔓",
            "🔑",
            "📜",
            "تحقق",
            "تایید کردن",
            "تأكيد",
            "যাচাই করা",
            "قواعد",
            "قوانين",
            "قواعد",
            "নিয়ম",
            "welcome",
            "authenticate",
            "confirm",
            "greeting",
            "click",
            "start-here",
            "invitation",
            "entree",
            "hello",
            "react",
            "правила",
            "ルール",
            "규칙",
            "规则",
        )

        self._ignore_channels = (
            "staff",
            "chat",
            "self",
            "reaction",
            "legit",
            "logs",
            "vc",
        )

    def _parse_properties(self, properties) -> Tuple[bool, bool]:
        membergate = False
        onboarding = False

        for feature in properties:
            if feature == "GUILD_ONBOARDING_EVER_ENABLED":
                onboarding = True
            if feature == "MEMBER_VERIFICATION_GATE_ENABLED":
                membergate = True

        return membergate, onboarding

    async def _parse_server_channels(self, guild_id: int) -> List[int]:
        """Parse server channels to find verification channels using discord.py"""
        guild = self.bot.get_guild(guild_id)
        if not guild:
            self.logger.error(f"Guild {guild_id} not found")
            return []

        access_channels = 0
        priority_channels = []
        good_channels = []
        see_channels = []

        for channel in guild.text_channels:

            try:
                if not channel.permissions_for(guild.me).view_channel:
                    continue
                access = True
                see_channels.append(channel.id)
            except Exception:
                continue

            if not access:
                continue

            access_channels += 1

            try:
                async for message in channel.history(limit=1):
                    break
            except Exception:
                continue

            channel_name = channel.name.lower()

            for name in self._verify_list_high:
                if name.lower() in channel_name:
                    priority_channels.append(channel.id)
                    break

            for name in self._verify_list:
                if name.lower() in channel_name:
                    good = True
                    for word in self._ignore_channels:
                        if word in channel_name:
                            good = False
                            break
                    if good:
                        good_channels.append(channel.id)
                    break

        if access_channels >= 8 or (
            priority_channels
            and getattr(self.config, "hypercheck_mode", "normal") != "boost"
        ):
            good_channels = []

        if (
            access_channels <= 4
            and getattr(self.config, "hypercheck_mode", "normal") == "boost"
        ):
            priority_channels = priority_channels + see_channels

        return good_channels + priority_channels

    async def _accept_membergate(self, guild_id: int) -> bool:
        try:
            # GET the form
            route = Route(
                "GET", "/guilds/{guild_id}/member-verification", guild_id=guild_id
            )
            try:
                data = await self.bot.http.request(route, params={"with_guild": "false"})
            except Exception as e:
                self.logger.error(f"[MemberGate] HTTP GET failed for guild {guild_id}: {e}")
                return False

            if not data or "form_fields" not in data:
                self.logger.error(f"[MemberGate] No form fields in response for guild {guild_id}")
                return False

            # Auto-fill responses
            for field in data["form_fields"]:
                field["response"] = True

            # PUT back the response
            route = Route("PUT", "/guilds/{guild_id}/requests/@me", guild_id=guild_id)
            try:
                resp = await self.bot.http.request(route, json=data)
            except Exception as e:
                self.logger.error(f"[MemberGate] HTTP PUT failed for guild {guild_id}: {e}")
                return False

            if resp is None:
                self.logger.error(f"[MemberGate] No response returned for guild {guild_id}")
                return False

            self.logger.info(f"[MemberGate] Successfully completed for guild {guild_id}")
            return True

        except Exception as e:
            self.logger.error(f"Error in membergate acceptance for guild {guild_id}: {e}")
            return False


    async def _accept_onboarding(self, guild_id: int) -> bool:
        try:
            # GET onboarding info
            route = Route("GET", "/guilds/{guild_id}/onboarding", guild_id=guild_id)
            try:
                data = await self.bot.http.request(route)
            except Exception as e:
                self.logger.error(f"[Onboarding] HTTP GET failed for guild {guild_id}: {e}")
                return False

            if not data or "prompts" not in data:
                self.logger.error(f"[Onboarding] No prompts in response for guild {guild_id}")
                return False

            # Build responses
            timestamp = int(perf_counter() * 1000)
            onboarding_responses = []
            onboarding_prompts_seen = {}
            onboarding_responses_seen = {}

            for prompt in data["prompts"]:
                onboarding_prompts_seen[str(prompt["id"])] = timestamp
                if prompt.get("options"):
                    onboarding_responses.append(prompt["options"][0]["id"])
                for response in prompt.get("options", []):
                    onboarding_responses_seen[str(response["id"])] = timestamp

            payload = {
                "onboarding_responses": onboarding_responses,
                "onboarding_prompts_seen": onboarding_prompts_seen,
                "onboarding_responses_seen": onboarding_responses_seen,
            }

            # POST the responses
            route = Route("POST", "/guilds/{guild_id}/onboarding-responses", guild_id=guild_id)
            try:
                resp = await self.bot.http.request(route, json=payload)
            except Exception as e:
                self.logger.error(f"[Onboarding] HTTP POST failed for guild {guild_id}: {e}")
                return False

            if resp is None:
                self.logger.error(f"[Onboarding] No response returned for guild {guild_id}")
                return False

            self.logger.info(f"[Onboarding] Successfully completed for guild {guild_id}")
            return True

        except Exception as e:
            self.logger.error(f"Error in onboarding acceptance for guild {guild_id}: {e}")
            return False

    async def _do_button(self, channel_id: int, guild_id: int) -> int:
        """Click buttons in a channel"""
        clicked_buttons = 0
        channel = self.bot.get_channel(channel_id)
        if not channel:
            return 0

        try:
            async for message in channel.history(limit=20):
                if message.author.id in self._bot_blacklist or not message.components:
                    continue

                for row in message.components:
                    for component in row.children:
                        if hasattr(component, "custom_id") and component.custom_id:
                            try:
                                await channel.trigger_typing()

                                self.logger.info(
                                    f"Would click button {component.custom_id} in {channel.name}"
                                )
                                clicked_buttons += 1
                                await asyncio.sleep(1)
                            except Exception as e:
                                self.logger.debug(f"Failed to click button: {e}")
                                continue

        except Exception as e:
            self.logger.error(f"Error clicking buttons in {channel_id}: {e}")

        return clicked_buttons

    async def _do_emoji(self, channel_id: int) -> int:
        """Add reactions to messages in a channel"""
        clicked_emojis = 0
        channel = self.bot.get_channel(channel_id)
        if not channel:
            return 0

        try:
            async for message in channel.history(limit=20):
                if message.reactions:
                    for reaction in message.reactions:
                        try:
                            await reaction.message.add_reaction(reaction.emoji)
                            clicked_emojis += 1
                            await asyncio.sleep(1)
                        except Exception as e:
                            self.logger.debug(f"Failed to add reaction: {e}")
                            continue

        except Exception as e:
            self.logger.error(f"Error adding reactions in {channel_id}: {e}")

        return clicked_emojis

    async def verify_in_server(
        self, guild_id: int, properties: dict
    ) -> Tuple[int, int]:
        """Main verification method adapted for discord.py"""
        verifications = []
        verify_time = perf_counter()

        membergate, onboarding = self._parse_properties(properties)

        if onboarding:
            onboarding_success = await self._accept_onboarding(guild_id)
            if onboarding_success:
                verifications.append("OnBoarding")
                self.logger.info(f"Onboarding completed for guild {guild_id}")

        if membergate:
            membergate_success = await self._accept_membergate(guild_id)
            if membergate_success:
                verifications.append("MemberGate")
                self.logger.info(f"MemberGate completed for guild {guild_id}")
            else:
                self.logger.warning(f"MemberGate failed for guild {guild_id}")

        verification_channels = await self._parse_server_channels(guild_id)

        clicked_emoji = 0
        clicked_buttons = 0

        for channel_id in verification_channels:
            clicked_emoji += await self._do_emoji(channel_id)
            clicked_buttons += await self._do_button(channel_id, guild_id)

        if clicked_emoji:
            verifications.append("Emoji")
        if clicked_buttons:
            verifications.append("Button")

        verify_duration = perf_counter() - verify_time
        if verifications:
            self.logger.info(
                f"Verified in server {guild_id}: {', '.join(verifications)} "
                f"({verify_duration:.2f}s)"
            )
        else:
            self.logger.info(f"No verifications needed for server {guild_id}")

        return clicked_emoji, clicked_buttons

"""Confirm which Polymarket outcome a Predict.fun outcome names, when the two venues spell it differently.

Predict.fun lists Polymarket's sports markets under Polymarket's own condition id,
with the same question and, word for word, the same rules -- but it names the
outcomes its own way: "HOU" where Polymarket says "Houston Dynamo", "Over 7.5"
where Polymarket says "Over". On 2026-10-05 that left 2 959 such markets (5 835
seeds) found by exact id and then dropped, because no outcome label matched.

Both venues settle through the conditional-tokens contract, and a Predict seed's
side is its outcome slot there (indexSet 1 or 2), just as Polymarket's outcome
list is in slot order. The slot alone would pair them. It is not trusted alone:
a pair that is wrong is not a worse trade, it is an unhedged bet. So the label
must independently name the outcome in that slot and not the other one, and the
caller additionally requires both outcomes of the market to be confirmed and the
rules texts to be identical. Anything that names neither outcome, or both, is
left unpaired: a missed pair costs nothing, a wrong one costs the stake.
"""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Iterable, Sequence

NAMED_OUTCOME_STRATEGY = "exact_id_named_outcome"

_OVER_UNDER_LABEL = re.compile(r"^(over|under)\s+(\d+(?:\.\d+)?)$", re.IGNORECASE)
_OVER_UNDER_LINE = re.compile(r"\b(?:o/u|over/under)\s+(\d+(?:\.\d+)?)\b", re.IGNORECASE)
_CODE = re.compile(r"[a-z0-9]{2,10}")
_BINARY_WORDS = frozenset({"yes", "no"})


def named_outcome_index(label: str, outcomes: Sequence[str], titles: Iterable[str]) -> int | None:
    """Index of the one outcome in `outcomes` that `label` names, or None when it names neither or both.

    `titles` are the market's titles on both venues; an over/under label must
    carry the same line as they do.
    """
    if len(outcomes) != 2:
        return None
    over_under = _OVER_UNDER_LABEL.match(label.strip())
    if over_under is not None:
        return _over_under_index(over_under, outcomes, titles)
    label_words = _words(label)
    if not label_words:
        return None
    outcome_words = [_words(outcome) for outcome in outcomes]
    if any(not words for words in outcome_words):
        return None
    whole_name = _hits(words == label_words for words in outcome_words)
    if whole_name:
        return whole_name[0] if len(whole_name) == 1 else None
    if len(label_words) != 1:
        return None
    code = label_words[0]
    if code in _BINARY_WORDS or not _CODE.fullmatch(code):
        return None
    # Predict.fun sometimes numbers a code that would otherwise repeat ("AUR1"
    # for Aurora), so a code ending in digits is also read without them -- but
    # only after it has been read as it is, because the digits can be the name
    # ("B04" is Bayer 04 Leverkusen).
    readable = [code]
    stripped = code.rstrip("0123456789")
    if stripped != code and len(stripped) >= 2:
        readable.append(stripped)
    # From the most to the least specific reading of a code. The first reading
    # that fits any outcome decides; if it fits both, the label is ambiguous and
    # a looser reading is not consulted. Looser readings than these -- letters
    # in order anywhere in a name -- were tried on the 2026-10-05 catalogue and
    # put "TEN" in "Texans" and "LAS" in "Maple Leafs".
    for candidate in readable:
        for reading in (_is_whole_word, _is_word_prefix, _is_initials, _is_word_segments, _is_known_code):
            hits = _hits(reading(candidate, words) for words in outcome_words)
            if hits:
                return hits[0] if len(hits) == 1 else None
    return None


def same_rules_text(left: str | None, right: str | None) -> bool:
    """True when both rules texts exist and differ at most in case and whitespace."""
    if not left or not right:
        return False
    return " ".join(left.split()).casefold() == " ".join(right.split()).casefold()


def _over_under_index(over_under: re.Match[str], outcomes: Sequence[str], titles: Iterable[str]) -> int | None:
    names = [" ".join(_words(outcome)) for outcome in outcomes]
    if sorted(names) != ["over", "under"]:
        return None
    lines = {line for title in titles for line in _OVER_UNDER_LINE.findall(title)}
    if lines != {over_under.group(2)}:
        return None
    return names.index(over_under.group(1).lower())


def _hits(matches: Iterable[bool]) -> list[int]:
    return [index for index, matched in enumerate(matches) if matched]


def _is_whole_word(code: str, words: list[str]) -> bool:
    return code in words


def _is_word_prefix(code: str, words: list[str]) -> bool:
    return len(code) >= 3 and any(word.startswith(code) for word in words)


def _is_initials(code: str, words: list[str]) -> bool:
    return len(words) >= 2 and "".join(word[0] for word in words) == code


def _is_word_segments(code: str, words: list[str]) -> bool:
    """`code` spelled by the starts of consecutive words: "aja" from "AJ Auxerre", "arkst" from "Arkansas State"."""
    return len(code) >= 3 and any(_spells(code, words[start:]) for start in range(len(words)))


def _spells(code: str, words: list[str]) -> bool:
    if not code:
        return True
    if not words:
        return False
    word = words[0]
    return any(
        code[:length] == word[:length] and _spells(code[length:], words[1:])
        for length in range(min(len(word), len(code)), 0, -1)
    )


def _is_known_code(code: str, words: list[str]) -> bool:
    return any(_contains_phrase(words, name) for name in _KNOWN_CODES.get(code, ()))


def _contains_phrase(words: list[str], phrase: tuple[str, ...]) -> bool:
    width = len(phrase)
    return any(tuple(words[index : index + width]) == phrase for index in range(len(words) - width + 1))


def _words(text: str) -> list[str]:
    folded = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii").casefold()
    return re.findall(r"[a-z0-9]+", folded)


# Codes no reading of the spelling recovers: a city where the outcome is a
# nickname ("PHI" for "76ers"), a country's ISO or FIFA code where it is not a
# prefix ("CHE", "SUI" for Switzerland). One code stands for every team or
# country that uses it -- "DET" is the Lions, Pistons, Red Wings and Tigers --
# which is harmless: the two sides of one match are never both Detroit, and if
# a code ever names both outcomes the label is rejected as ambiguous.
_CODE_TABLE = """
ARI cardinals|diamondbacks; ATL falcons|hawks|braves|atlanta united; BAL ravens|orioles; BUF bills|sabres;
CAR panthers|hurricanes; CHI bears|bulls|blackhawks|chicago fire|chile; CIN bengals|reds|cincinnati;
CLE browns|cavaliers|guardians; DAL cowboys|mavericks|stars|fc dallas; DEN broncos|nuggets|denmark;
DET lions|pistons|red wings|tigers; GB packers; GNB packers; HOU texans|rockets|astros|houston dynamo;
IND colts|pacers|india; JAX jaguars; JAC jaguars; KC chiefs|royals; KAN chiefs; LV raiders|golden knights;
LVR raiders; LAS raiders|golden knights; LAC chargers|clippers; LAR rams; LA rams|kings|la galaxy;
MIA dolphins|heat|marlins|inter miami; MIN vikings|timberwolves|wild|twins|minnesota united;
NE patriots|new england revolution; NWE patriots; NYG giants; NYJ jets;
PHI eagles|76ers|flyers|phillies|union|philippines; PIT steelers|penguins|pirates; SF 49ers|giants; SFO 49ers;
SEA seahawks|kraken|mariners|seattle sounders; TB buccaneers|lightning|rays; TAM buccaneers; TEN titans;
WAS commanders|wizards|capitals|nationals; WSH commanders|wizards|capitals|nationals;
BOS celtics|bruins|red sox; BKN nets; BRK nets; CHA hornets|chad; CHO hornets; GS warriors; GSW warriors;
LAL lakers; MEM grizzlies; MIL bucks|brewers; NOP pelicans; NY knicks|red bulls; NYK knicks; OKC thunder;
ORL magic|orlando city; PHX suns; PHO suns; POR trail blazers|timbers|portugal; SAC kings; SA spurs; SAS spurs;
TOR raptors|maple leafs|blue jays|toronto fc; UTA jazz|mammoth; UTAH jazz|mammoth; ANA ducks; CGY flames;
CBJ blue jackets; COL avalanche|rockies|colorado rapids|colombia; EDM oilers; FLA panthers; LAK kings;
MTL canadiens|montreal; NSH predators|nashville; NJ devils; NJD devils; NYI islanders; NYR rangers;
OTT senators; SJ sharks|san jose earthquakes; SJS sharks; STL blues|cardinals|st louis city; TBL lightning;
VAN canucks|vancouver whitecaps; VGK golden knights; VEG golden knights; WPG jets; AZ diamondbacks; CHC cubs;
CWS white sox; CHW white sox; LAA angels; LAD dodgers; NYM mets; NYY yankees; OAK athletics; ATH athletics;
SD padres|san diego; SDP padres; SFG giants; TEX rangers; CLB columbus crew; CLT charlotte; DC d c united;
DCU d c united; LAG la galaxy; LAFC los angeles fc; NYC new york city; RBNY red bulls; RSL real salt lake;
SKC sporting kansas city; AUS austin|australia;
ALB albania; AND andorra; ARM armenia; AUT austria; AZE azerbaijan; BLR belarus; BEL belgium; BIH bosnia;
BUL bulgaria; BGR bulgaria; CRO croatia; HRV croatia; CYP cyprus; CZE czechia|czech; ENG england; EST estonia;
FRO faroe; FIN finland; FRA france; GEO georgia; GER germany; DEU germany; GIB gibraltar; GRE greece; GRC greece;
HUN hungary; ISL iceland; IRL ireland; ISR israel; ITA italy; KAZ kazakhstan; KOS kosovo; LVA latvia;
LIE liechtenstein; LTU lithuania; LUX luxembourg; MLT malta; MDA moldova; MNE montenegro; NED netherlands;
NLD netherlands; MKD macedonia; NIR northern ireland; NOR norway; POL poland; PRT portugal; ROU romania;
RUS russia; SMR san marino; SCO scotland; SRB serbia; SVK slovakia; SVN slovenia; ESP spain; SWE sweden;
SUI switzerland; CHE switzerland; TUR turkiye|turkey; UKR ukraine; WAL wales; ARG argentina; BOL bolivia;
BRA brazil; CHL chile; ECU ecuador; PAR paraguay; PRY paraguay; PER peru; URU uruguay; URY uruguay;
VEN venezuela; USA usa|united states; MEX mexico; CAN canada; CRC costa rica; CRI costa rica; HON honduras;
HND honduras; PAN panama; JAM jamaica; SLV el salvador; GUA guatemala; GTM guatemala; HAI haiti; HTI haiti;
TRI trinidad; TTO trinidad; CUW curacao; SUR suriname; NCA nicaragua; NIC nicaragua; JPN japan; KOR korea;
PRK korea; CHN china; IRN iran; IRI iran; KSA saudi; SAU saudi; QAT qatar; UAE united arab emirates|uae;
ARE united arab emirates|uae; IRQ iraq; JOR jordan; UZB uzbekistan; OMA oman; OMN oman; BHR bahrain;
KUW kuwait; KWT kuwait; SYR syria; LIB lebanon; LBN lebanon; PLE palestine; PSE palestine; THA thailand;
VIE vietnam; VNM vietnam; IDN indonesia; INA indonesia; MAS malaysia; MYS malaysia; SIN singapore;
SGP singapore; PHL philippines; TJK tajikistan; KGZ kyrgyz; TKM turkmenistan; HKG hong kong;
NZL new zealand; MAR morocco; ALG algeria; DZA algeria; TUN tunisia; EGY egypt; SEN senegal; NGA nigeria;
GHA ghana; CMR cameroon; CIV ivoire|ivory coast; MLI mali; BFA burkina; RSA south africa; ZAF south africa;
COD congo; ANG angola; AGO angola; ZAM zambia; ZMB zambia; CPV cape verde|cabo verde; GAB gabon;
GUI guinea; GIN guinea; EQG equatorial guinea; GNQ equatorial guinea; BEN benin; TAN tanzania;
TZA tanzania; UGA uganda; KEN kenya; LBY libya; SUD sudan; SDN sudan; MOZ mozambique; NAM namibia;
ZIM zimbabwe; ZWE zimbabwe; MAD madagascar; MDG madagascar; MTN mauritania; MRT mauritania; GAM gambia;
GMB gambia; SLE sierra leone; TOG togo; TGO togo; ETH ethiopia; RWA rwanda; BOT botswana; BWA botswana
"""


def _parse_code_table(table: str) -> dict[str, tuple[tuple[str, ...], ...]]:
    codes: dict[str, list[tuple[str, ...]]] = {}
    for entry in table.split(";"):
        code, _, names = entry.strip().partition(" ")
        for name in names.split("|"):
            codes.setdefault(code.casefold(), []).append(tuple(_words(name)))
    return {code: tuple(dict.fromkeys(names)) for code, names in codes.items()}


_KNOWN_CODES = _parse_code_table(_CODE_TABLE)

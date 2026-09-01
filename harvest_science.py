#!/usr/bin/env python3
"""
harvest_science.py — the science wire: what is wrong with the published record,
who is producing the wrong parts, what catches it and how late.

After the Science section, which puts numbers to it: 2% of scientists admitting
to fabricating or falsifying data at least once, around 14% saying they know a
colleague who did, up to one in seven submissions to hundreds of journals
flagged for paper-mill signs, and screening that suggested a third of one year's
neuroscience papers and a quarter of its medicine papers were likely made up or
plagiarised. Behind a large share of that sit paper mills — the section
describes one operation producing over a hundred manuscripts a month with a
hundred staff, three hundred freelancers across six countries, and more than a
thousand agencies feeding it.

The rest follows the mechanisms it lists: plagiarism, ghost authorship, image
manipulation, undisclosed conflicts, p-hacking and salami-slicing, citation
cartels, and undisclosed AI generation. Then the two things that make it
compound — detection lagging by years while fraud is produced faster than it is
caught, and every fraudulent paper contaminating the work written on top of it.
Then peer review, predatory venues, replication, who profits from the record,
who can read it, who funds it, the incentives that reward volume, and the people
who catch it.

A discovery is not this subject. This is a feed on research integrity, not
science news, and every term carries the integrity words it must appear beside.

    python3 harvest_science.py
    python3 harvest_science.py --dry-run
"""

import argparse
import gzip
import html
import io
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

# The shared gazetteer. Placement used to be each wire's own short country
# table, which put most of every wire in a counter marked "unplaced"; this is
# the fleet's common one, and it is optional at import so a harvest still runs
# if the data file has not been fetched yet.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    import galaxy_places
    _GAZETTEER = True
except Exception as _exc:                       # noqa: BLE001
    print("  ! gazetteer unavailable (%s); falling back to the local table"
          % _exc, file=sys.stderr)
    galaxy_places = None
    _GAZETTEER = False

HERE = os.path.dirname(os.path.abspath(__file__))
SOURCES_PATH = os.path.join(HERE, "sources_science.json")
OUT_PATH = os.path.join(HERE, "wire_science.json")

RETAIN_DAYS = 45
MAX_ITEMS = 1200
WORKERS = 14         # ~330 wires now: 26 languages, each asked in its own
NOTABLE_SCORE = 3       # at or above this a story is marked as consequential

# --------------------------------------------------------------------------
# Plumbing: fetching, feed parsing, word-edge matching, fingerprints.
# --------------------------------------------------------------------------
USER_AGENT = ("Mozilla/5.0 (compatible; science-feed/1.0; "
              "+https://github.com/WelcomeToYourGalaxy/space-life-news)")

TIMEOUT = 25

SNIPPET_CHARS = 240

TAG_RE = re.compile(r"<[^>]+>")

WS_RE = re.compile(r"\s+")

PUNCT_RE = re.compile(r"[^\w\s]", re.UNICODE)

def build_gnews_url(loc):
    q = loc["query"] + " when:30d"
    return ("https://news.google.com/rss/search?q=" + urllib.parse.quote(q) +
            "&hl=" + loc["hl"] + "&gl=" + loc["gl"] + "&ceid=" + loc["ceid"])

READ_BUDGET_MIN = 40          # minutes spent reading wires

# The wall-clock budget for reading wires. Past it the remaining sources are
# recorded unreachable and the harvest finishes on what it has, because the
# wire is only written at the end of run() and a job killed by the workflow
# timeout commits nothing at all — which is how a feed gets stuck stale.
DEADLINE = None


def out_of_time():
    return DEADLINE is not None and time.monotonic() > DEADLINE


def fetch(url, tries=3):
    last = None
    for attempt in range(tries):
        if out_of_time():
            return None
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": USER_AGENT,
                "Accept": "application/rss+xml, application/atom+xml, application/xml;q=0.9, */*;q=0.8",
                "Accept-Encoding": "gzip",
                "Accept-Language": "*",
            })
            with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
                raw = resp.read()
                if resp.headers.get("Content-Encoding") == "gzip":
                    raw = gzip.GzipFile(fileobj=io.BytesIO(raw)).read()
                return raw
        except urllib.error.HTTPError as exc:
            last = exc
            # Being rate-limited or refused is an answer, not a hiccup. Trying
            # the same query twice more against the same limiter spends eighty
            # seconds of a worker slot to be told the same thing, and deepens
            # the throttle for every other query in the run.
            if exc.code in (403, 429, 451):
                time.sleep(1.5)
                break
            time.sleep(1.5 * (attempt + 1))
        except Exception as exc:                       # noqa: BLE001 — report, don't crash the run
            last = exc
            time.sleep(1.5 * (attempt + 1))
    print("  ! unreachable: %s (%s)" % (url[:90], last), file=sys.stderr)
    return None

def strip_ns(tag):
    return tag.split("}", 1)[1] if "}" in tag else tag

def text_of(el):
    return WS_RE.sub(" ", html.unescape(TAG_RE.sub(" ", el.text or ""))).strip() if el is not None else ""

def child(node, *names):
    for kid in node:
        if strip_ns(kid.tag) in names:
            return kid
    return None

def parse_date(raw):
    if not raw:
        return None
    raw = raw.strip()
    try:
        dt = parsedate_to_datetime(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:  # noqa: BLE001
        pass
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:  # noqa: BLE001
        return None



_YT_ID = re.compile(r"\s*\([A-Za-z0-9_-]{9,14}\)\s*$")
_HASHTAG = re.compile(r"#\S+")

def _is_video_result(title):
    """Google News mixes YouTube results in among the articles, and marks them
    by pasting a search query and the video id onto the end of the video title:

        <unrelated Japanese vlog title> Food Recall Salmonella Milk (JClqFGBDvh)

    The pasted query is not always the source's own query, so stripping words
    cannot reliably clear it — a football clip still arrives carrying "Food
    Recall Salmonella". The bracketed 9-14 character id is the dependable
    marker: no real headline ends that way. Hashtag pileups are the same class
    of upload-description noise.
    """
    if not title:
        return False
    if _YT_ID.search(title):
        return True
    return len(_HASHTAG.findall(title)) >= 3


# Stripping the pasted query word-by-word was tried and removed: real
# headlines end on query words all the time — "Regulator bans ad over
# misleading carbon neutral claim" lost four real words that way, and the
# shortened copy then failed to dedupe against the original. The video id
# is the only reliable marker, so detection rests on that alone.

def parse_feed(raw, src):
    try:
        root = ET.fromstring(raw)
    except ET.ParseError:
        # Some publishers serve a stray byte before the declaration.
        try:
            root = ET.fromstring(raw[raw.index(b"<"):])
        except Exception:  # noqa: BLE001
            return []

    nodes = [n for n in root.iter() if strip_ns(n.tag) == "item"]
    atom = False
    if not nodes:
        nodes = [n for n in root.iter() if strip_ns(n.tag) == "entry"]
        atom = True

    out = []
    for n in nodes:
        title = text_of(child(n, "title"))
        if atom:
            link = ""
            for kid in n:
                if strip_ns(kid.tag) == "link" and kid.get("rel", "alternate") == "alternate":
                    link = kid.get("href", "")
                    break
        else:
            link_el = child(n, "link")
            link = (link_el.text or "").strip() if link_el is not None else ""
            if not link:
                link = text_of(child(n, "guid"))
        if not title or not link:
            continue

        outlet_el = child(n, "source")
        outlet = text_of(outlet_el) if outlet_el is not None else ""
        if outlet and title.endswith(" - " + outlet):
            title = title[: -(len(outlet) + 3)].strip()
        elif not outlet and src["name"].startswith("Google News") and " - " in title:
            # Google News appends the outlet to the headline when it omits <source>.
            head, _, tail = title.rpartition(" - ")
            if head and 2 <= len(tail) <= 45:
                title, outlet = head.strip(), tail.strip()

        stamp = parse_date(text_of(child(n, "pubDate", "published", "updated", "date")))
        snippet = text_of(child(n, "description", "summary", "content"))[:SNIPPET_CHARS]

        # Google News descriptions are usually the headline with the publisher's
        # name tacked on the end. That name is not part of the story, and it was
        # being read as geography: "The Guardian Nigeria News" placed a piece
        # about UK social mobility in Nigeria. Strip the publisher before the
        # text is ever classified or placed.
        for tail in (outlet, src["name"].replace("Google News \u00b7 ", "")):
            if tail and len(tail) > 3 and snippet.endswith(tail):
                snippet = snippet[: -len(tail)].strip(" -\u2013\u2014\u00b7|,")

        # Google News surfaces YouTube videos with the search query pasted onto
        # the end of the video title, followed by the video id in brackets:
        #   "<unrelated Japanese vlog title> Food Recall Salmonella Milk (JClqFGBDvh)"
        # Left alone, the query terms are read as if the story were about them,
        # so a football clip arrives filed under a food recall. Strip the id and
        # then any trailing run of words drawn from this source's own query.
        if _is_video_result(title):
            continue
        title = _YT_ID.sub("", title)
        snippet = _YT_ID.sub("", snippet)
        if not title.strip():
            continue

        out.append({
            "t": title,
            "u": link,
            "o": outlet or src["name"].replace("Google News · ", ""),
            "g": src["lang"],
            "r": src["region"],
            "k": src.get("kind", "news"),
            "d": stamp,
            "s": snippet,
            "w": src["name"],
        })
    return out

def _compile(term):
    if any(ord(ch) > 0x24F for ch in term):        # non-Latin script
        # substring matching is already prefix-like in scripts without word
        # breaks, so a trailing * is a no-op — strip it rather than search for
        # a literal asterisk, which is what used to happen.
        return term[:-1] if term.endswith("*") else term
    if term.endswith("*"):
        return re.compile(r"(?<![a-z0-9])" + re.escape(term[:-1]) + r"[a-z0-9\-]*", re.I)
    # A plain term also matches its simple plural. Without this, "polling
    # station" misses "polling stations" and "voter roll" misses "voter rolls",
    # which is how most headlines actually write them — the term looks present
    # to a reader and is invisible to the matcher. Use a trailing * for a real
    # prefix match; this only adds the regular plural.
    return re.compile(r"(?<![a-z0-9])" + re.escape(term) + r"(?:es|s)?(?![a-z0-9])", re.I)

def _compile_all(terms):
    return [_compile(t) for t in terms]

def hit(text, compiled):
    """True when any compiled term matches."""
    for c in compiled:
        if isinstance(c, str):
            if c in text:
                return True
        elif c.search(text):
            return True
    return False

def fingerprint(title):
    norm = PUNCT_RE.sub(" ", title.lower())
    return " ".join(WS_RE.sub(" ", norm).strip().split()[:9])

def canon_url(url):
    try:
        parts = urllib.parse.urlsplit(url)
        query = urllib.parse.parse_qsl(parts.query)
        query = [(k, v) for k, v in query if not k.lower().startswith("utm_")]
        return urllib.parse.urlunsplit((parts.scheme, parts.netloc, parts.path.rstrip("/"),
                                        urllib.parse.urlencode(query), ""))
    except Exception:  # noqa: BLE001
        return url


# --------------------------------------------------------------------------
# Where the story is, in three levels: region, subregion, place. A story
# naming a place files under the subregion and region above it, so the page
# can open a region and drill into it.
# --------------------------------------------------------------------------
# region → subregion → country, with the terms that match each country.
# Matching a country implies its subregion and its region, so a story naming
# Peru files under Peru, South America and Latin America at once.
GEO3 = [
 ("africa", "Africa", [
   ("africa-e", "East Africa", [
     ("ke","Kenya",["kenya","kenyan","nairobi","ogiek","maasai","samburu","turkana"]),
     ("tz","Tanzania",["tanzania","tanzanian","ngorongoro","hadza","serengeti"]),
     ("ug","Uganda",["uganda","ugandan","batwa uganda","karamoja"]),
     ("et","Ethiopia",["ethiopia","ethiopian","omo valley","oromia"]),
     ("so","Somalia",["somalia","somali","somaliland"]),
     ("rw","Rwanda",["rwanda","rwandan"]),
     ("bi","Burundi",["burundi"]),
     ("sd","Sudan",["sudan","sudanese","darfur"]),
     ("ss","South Sudan",["south sudan","dinka","nuer"]),
     ("mg","Madagascar",["madagascar","malagasy"]),
     ("mz","Mozambique",["mozambique","cabo delgado"]),
     ("zm","Zambia",["zambia","zambian"]),
     ("zw","Zimbabwe",["zimbabwe","zimbabwean"]),
     ("mw","Malawi",["malawi"]),
   ]),
   ("africa-w", "West Africa", [
     ("ng","Nigeria",["nigeria","nigerian","ogoni","niger delta","ijaw","nafdac"]),
     ("gh","Ghana",["ghana","ghanaian"]),
     ("ci","Côte d'Ivoire",["côte d'ivoire","ivory coast","ivorian"]),
     ("sn","Senegal",["senegal","senegalese","casamance"]),
     ("ml","Mali",["mali","malian","bamako","tuareg"]),
     ("bf","Burkina Faso",["burkina faso"]),
     ("ne","Niger",["niger republic","nigerien"]),
     ("lr","Liberia",["liberia","liberian"]),
     ("sl","Sierra Leone",["sierra leone"]),
     ("gn","Guinea",["guinea conakry","guinean"]),
     ("cm","Cameroon",["cameroon","cameroonian","baka"]),
   ]),
   ("africa-c", "Central Africa", [
     ("cd","DR Congo",["democratic republic of congo","drc","congolese","kivu","batwa"]),
     ("cg","Congo-Brazzaville",["republic of congo","brazzaville"]),
     ("ga","Gabon",["gabon","gabonese"]),
     ("cf","Central African Republic",["central african republic"]),
     ("td","Chad",["chad","chadian"]),
   ]),
   ("africa-s", "Southern Africa", [
     ("za","South Africa",["south africa","south african","khoisan","khoi","xolobeni","sahpra"]),
     ("bw","Botswana",["botswana","san people","central kalahari"]),
     ("na","Namibia",["namibia","namibian","himba","ovahimba"]),
     ("ao","Angola",["angola","angolan"]),
     ("ls","Lesotho",["lesotho"]),
   ]),
   ("africa-n", "North Africa", [
     ("ma","Morocco",["morocco","moroccan","amazigh","berber","western sahara","sahrawi"]),
     ("dz","Algeria",["algeria","algerian","kabyle"]),
     ("tn","Tunisia",["tunisia"]),
     ("ly","Libya",["libya","libyan","tuareg libya"]),
     ("eg","Egypt",["egypt","egyptian","nubian"]),
   ]),
 ]),
 ("americas-n", "North America", [
   ("na-us", "United States", [
     ("us","United States",["united states", "u.s.", "usa", "american", "washington dc", "fda", "usda", "cdc", "epa", "ftc", "cms", "nih", "osha", "fsis", "congress", "white house", "alabama", "arizona", "arkansas", "california", "colorado", "connecticut", "delaware", "florida", "georgia", "idaho", "illinois", "indiana", "iowa", "kansas", "kentucky", "louisiana", "maine", "maryland", "massachusetts", "michigan", "minnesota", "mississippi", "missouri", "montana", "nebraska", "nevada", "new hampshire", "new jersey", "new mexico", "north carolina", "north dakota", "ohio", "oklahoma", "oregon", "pennsylvania", "rhode island", "south carolina", "south dakota", "tennessee", "texas", "utah", "vermont", "virginia", "west virginia", "wisconsin", "wyoming"]),
     ("us-ak","Alaska",["alaska","alaskan","inupiat","yupik","gwich'in"]),
     ("us-sw","US Southwest",["navajo","diné","hopi","apache","arizona tribe","new mexico pueblo","tohono o'odham"]),
     ("us-pl","US Plains & Midwest",["standing rock","lakota","dakota access","oglala","cheyenne river","ojibwe","anishinaabe"]),
     ("us-pnw","US Pacific Northwest",["yakama","nez perce","puyallup","lummi","columbia river treaty","klamath"]),
     ("us-e","US East & South",["cherokee","seminole","lumbee","penobscot","wampanoag","mashpee"]),
     ("us-hi","Hawai'i",["native hawaiian","kanaka maoli","mauna kea","hawaii"]),
   ]),
   ("na-ca", "Canada", [
     ("ca","Canada",["canada", "canadian", "health canada", "cfia", "ottawa", "toronto", "montreal", "vancouver", "ontario", "alberta", "manitoba", "saskatchewan", "nova scotia", "new brunswick", "newfoundland"]),
     ("ca-bc","British Columbia",["british columbia","wet'suwet'en","haida","coastal gitxsan","secwepemc"]),
     ("ca-pr","Prairies",["alberta","saskatchewan","manitoba","treaty 8","treaty 6"]),
     ("ca-on","Ontario & Quebec",["ontario first nation","quebec","grassy narrows","innu","cree quebec","atikamekw"]),
     ("ca-n","Northern Canada",["nunavut","northwest territories","yukon","inuit nunangat","dene"]),
     ("ca-at","Atlantic Canada",["mi'kmaq","nova scotia","new brunswick","newfoundland","innu labrador"]),
   ]),
   ("na-mx", "Mexico", [
     ("mx","Mexico",["mexico", "mexican", "cofepris", "mexico city"]),
     ("mx-s","Southern Mexico",["chiapas","oaxaca","zapatista","zapoteco","mixe","tren maya","yucatán","maya"]),
     ("mx-n","Northern Mexico",["yaqui","rarámuri","tarahumara","sonora","chihuahua"]),
   ]),
 ]),
 ("americas-s", "Latin America & Caribbean", [
   ("la-amz", "Amazon Basin", [
     ("br-amz","Brazilian Amazon",["yanomami","munduruku","kayapó","xingu","terra indígena","amazônia","rondônia","pará"]),
     ("pe-amz","Peruvian Amazon",["loreto","ucayali","madre de dios","awajún","shipibo","kakataibo"]),
     ("co-amz","Colombian Amazon",["amazonas colombia","putumayo","caquetá"]),
     ("ec-amz","Ecuadorian Amazon",["yasuní","waorani","sarayaku","sucumbíos","achuar"]),
     ("bo-amz","Bolivian Amazon",["tipnis","beni","chiquitano","bolivian amazon"]),
     ("ve-amz","Venezuelan Amazon",["arco minero","amazonas venezuela","pemón"]),
   ]),
   ("la-and", "Andes & Southern Cone", [
     ("cl","Chile",["chile","chilean","mapuche","araucanía","wallmapu"]),
     ("ar","Argentina",["argentina","argentine","patagonia","qom","wichí"]),
     ("pe","Peru",["peru","peruvian","quechua","aymara peru"]),
     ("bo","Bolivia",["bolivia","bolivian","aymara","quechua bolivia"]),
     ("py","Paraguay",["paraguay","ayoreo","chaco paraguayo"]),
     ("uy","Uruguay",["uruguay"]),
   ]),
   ("la-ca", "Central America", [
     ("gt","Guatemala",["guatemala","guatemalan","ixil","k'iche'","q'eqchi'"]),
     ("hn","Honduras",["honduras","garífuna","lenca","berta cáceres"]),
     ("ni","Nicaragua",["nicaragua","miskito","bosawás"]),
     ("cr","Costa Rica",["costa rica","bribri","térraba"]),
     ("pa","Panama",["panama","guna","ngäbe","emberá"]),
     ("bz","Belize",["belize","maya belize"]),
     ("sv","El Salvador",["el salvador"]),
   ]),
   ("la-car", "Caribbean & Guianas", [
     ("gy","Guyana",["guyana","wapichan","rupununi"]),
     ("sr","Suriname",["suriname","saamaka","maroon suriname","kaliña"]),
     ("gf","French Guiana",["guyane","french guiana","wayana"]),
     ("do","Caribbean islands",["dominica kalinago","caribbean indigenous","taino","haiti","jamaica","puerto rico"]),
   ]),
   ("la-br", "Brazil (other)", [
     ("br","Brazil",["brazil", "brazilian", "anvisa", "brasilia", "sao paulo", "minas gerais", "bahia", "mato grosso"]),
     ("br-ne","Brazil northeast & cerrado",["cerrado","bahia indígena","maranhão","quilombola","pataxó","guarani-kaiowá","mato grosso do sul"]),
   ]),
 ]),
 ("asia-s", "South Asia", [
   ("sa-in", "India", [
     ("in","India",["india", "indian", "fssai", "cdsco", "sebi", "new delhi", "mumbai", "maharashtra", "uttar pradesh", "tamil nadu", "karnataka", "kerala", "west bengal", "gujarat", "rajasthan", "bihar", "andhra pradesh", "telangana"]),
     ("in-c","Central India",["chhattisgarh","jharkhand","odisha","madhya pradesh","hasdeo","niyamgiri","bastar"]),
     ("in-ne","Northeast India",["assam","manipur","nagaland","mizoram","meghalaya","arunachal"]),
     ("in-s","South & West India",["kerala adivasi","tamil nadu tribal","karnataka tribal","gujarat adivasi","maharashtra adivasi"]),
     ("in-h","Himalayan India",["ladakh","uttarakhand","himachal","sikkim"]),
   ]),
   ("sa-oth", "Rest of South Asia", [
     ("bd","Bangladesh",["bangladesh","chittagong hill tracts","jumma","chakma"]),
     ("np","Nepal",["nepal","tharu","newar","chepang"]),
     ("pk","Pakistan",["pakistan","balochistan","kalash"]),
     ("lk","Sri Lanka",["sri lanka","vedda"]),
     ("bt","Bhutan",["bhutan"]),
   ]),
 ]),
 ("asia-se", "Southeast Asia", [
   ("se-mar", "Maritime Southeast Asia", [
     ("id","Indonesia",["indonesia","indonesian","masyarakat adat","papua","west papua","kalimantan","dayak","sulawesi","sumatra","mentawai"]),
     ("ph","Philippines",["philippines","filipino","lumad","igorot","mindanao","cordillera","ancestral domain"]),
     ("my","Malaysia",["malaysia","sarawak","sabah","penan","orang asli","bakun"]),
     ("tl","Timor-Leste",["timor-leste","east timor"]),
     ("pg-ind","Papua New Guinea",["papua new guinea","bougainville","porgera"]),
   ]),
   ("se-main", "Mainland Southeast Asia", [
     ("th","Thailand",["thailand","karen thailand","bangkloi","chao lay","hill tribe"]),
     ("mm","Myanmar",["myanmar","burma","karen state","kachin","chin state","rakhine"]),
     ("vn","Vietnam",["vietnam","montagnard","central highlands vietnam"]),
     ("kh","Cambodia",["cambodia","bunong","ratanakiri"]),
     ("la","Laos",["laos","hmong laos"]),
   ]),
 ]),
 ("asia-e", "East & Central Asia", [
   ("ea-e", "East Asia", [
     ("tw","Taiwan",["taiwan","原住民族","傳統領域","amis","atayal","bunun"]),
     ("jp","Japan",["japan","ainu","hokkaido","okinawa","ryukyu","pmda","mhlw"]),
     ("cn","China",["china","tibet","tibetan","xinjiang","uyghur","inner mongolia","yunnan minority","nmpa","samr","guangdong"]),
     ("kr","Korea",["korea","korean","mfds"]),
     ("mn","Mongolia",["mongolia","mongolian","dukha","tsaatan"]),
   ]),
   ("ea-c", "Central Asia & Siberia", [
     ("ru-sib","Siberia & Russian North",["siberia","evenki","nenets","khanty","yamal","sakha","chukotka","коренные малочисленные"]),
     ("kz","Kazakhstan",["kazakhstan"]),
     ("kg","Kyrgyzstan",["kyrgyzstan"]),
     ("uz","Uzbekistan",["uzbekistan"]),
   ]),
 ]),
 ("mena", "Middle East & North Africa", [
   ("me-lev", "Levant & Gulf", [
     ("il","Israel & Palestine",["bedouin","negev","naqab","palestinian land","israel","west bank"]),
     ("jo","Jordan",["jordan","bedouin jordan"]),
     ("iq","Iraq",["iraq","marsh arabs","yazidi","kurdistan iraq"]),
     ("ir","Iran",["iran","qashqai","bakhtiari","ahwazi"]),
     ("sa","Gulf states",["saudi arabia","uae","oman","qatar","kuwait","sfda"]),
     ("tr","Turkey",["turkey","türkiye","kurdish","hasankeyf","alevi"]),
   ]),
 ]),
 ("europe", "Europe", [
   ("eu-n", "Nordic & Arctic Europe", [
     ("no","Norway",["norway","norwegian","sápmi","fosen","finnmark"]),
     ("se","Sweden",["sweden","swedish","girjas","gällivare","kiruna","samer"]),
     ("fi","Finland",["finland","finnish","inari","sámi parliament"]),
     ("gl","Greenland",["greenland","kalaallit","nuuk"]),
     ("ru-eu","Russian Karelia & Kola",["kola peninsula","karelia","murmansk sami"]),
   ]),
   ("eu-o", "Rest of Europe", [
     ("be","Belgium",["belgium", "belgian", "flanders", "wallonia", "antwerp"]),
     ("it","Italy",["italy", "italian", "rome", "milan", "sicily", "lombardy"]),
     ("ch","Switzerland",["switzerland", "swiss", "swissmedic", "geneva", "zurich", "bern"]),
     ("nl","Netherlands",["netherlands", "dutch", "nvwa", "amsterdam", "the hague"]),
     ("ua","Ukraine",["ukraine","crimean tatars","krym"]),
     ("ru","Russia (European)",["russia","russian federation"]),
     ("eu","European Union",["european union","european commission","brussels","efsa","ema","echa","european food safety authority","european medicines agency"]),
     ("uk","United Kingdom",["united kingdom","britain","scotland","wales","england","u.k.","uk","mhra","food standards agency","ofcom","ofgem","ofsted","northern ireland"]),
     ("es","Spain",["spain","spanish","catalonia","andalusia"]),
     ("fr","France",["france","french","anses"]),
     ("de","Germany",["germany","german","bfr","bavaria","saxony"]),
   ]),
 ]),
 ("oceania", "Oceania", [
   ("oc-au", "Australia", [
     ("au","Australia",["australia", "australian", "tga", "fsanz", "accc", "canberra", "sydney", "melbourne", "new south wales", "queensland", "western australia", "south australia", "tasmania", "northern territory"]),
     ("au-n","Northern Australia",["northern territory","arnhem land","kimberley","juukan gorge","tiwi","gulf country"]),
     ("au-w","Western Australia",["western australia","pilbara","noongar","yindjibarndi"]),
     ("au-e","Eastern Australia",["queensland","new south wales","victoria aboriginal","wiradjuri","gunditjmara","adani","carmichael"]),
     ("au-c","Central & South Australia",["south australia","adnyamathanha","arrernte","alice springs","olympic dam"]),
   ]),
   ("oc-nz", "Aotearoa New Zealand", [
     ("nz","Aotearoa",["new zealand","aotearoa","māori","maori","iwi","waitangi","ngāi tahu","tainui"]),
   ]),
   ("oc-pac", "Pacific Islands", [
     ("fj","Fiji",["fiji","fijian","itaukei"]),
     ("nc","Kanaky New Caledonia",["new caledonia","kanaky","kanak","nouméa"]),
     ("sb","Solomon Islands",["solomon islands"]),
     ("vu","Vanuatu",["vanuatu","ni-vanuatu"]),
     ("ws","Polynesia & Micronesia",["samoa","tonga","tuvalu","kiribati","marshall islands","palau","guam","chamorro","tahiti","rapa nui","easter island"]),
   ]),
 ]),
 ("polar", "Arctic & Antarctic", [
   ("pol-arc", "Circumpolar", [
     ("arctic","Arctic Council region",["arctic council","circumpolar","inuit circumpolar","arctic indigenous"]),
   ]),
 ]),
]

# --------------------------------------------------------------------------
# Subjects
# --------------------------------------------------------------------------
# --------------------------------------------------------------------------
# Subjects — after the Law Enforcement section of Suppression.
#
# Each subject is a list of (term, context) pairs. The term must appear AND at
# least one of its context words, which is what keeps a sector from swamping
# the wire: "copper output rises" has the sector but no control, so it fails;
# "copper concession auctioned" passes. Subjects with an empty context list
# are already control language on their own.
# --------------------------------------------------------------------------
# THE SUBJECTS
#
# What is wrong with the published record, who is producing the wrong parts,
# what catches it and how late, and what the incentives reward.
#
# This is a feed on research integrity, not science news. A discovery is not
# this subject, and every term carries the integrity words it must appear
# beside so that "scientists find" coverage cannot reach the wire.
# --------------------------------------------------------------------------
TOPICS = [
    ("fabrication", "Fabricated and falsified data", [
        ("data fabricat*", []), ("fabricated data", []), ("falsified data", []),
        ("falsification", ["data", "research", "results", "finding"]),
        ("made-up", ["data", "results", "papers", "patients", "participants"]),
        ("invented", ["data", "results", "patients", "participants"]),
        ("research misconduct", []), ("scientific misconduct", []),
        ("research fraud", []), ("scientific fraud", []),
        ("admitted", ["fabricat*", "falsif*", "misconduct", "fraud"]),
        ("misconduct finding", []), ("integrity investigation", []),
        ("office of research integrity", []),
    ]),
    ("papermills", "Paper mills and papers for sale", [
        ("paper mill", []), ("papermill", []), ("paper-mill", []),
        ("authorship for sale", []), ("buy authorship", []), ("sold authorship", []),
        ("academic support agenc*", []), ("manuscript factory", []),
        ("mass-produced", ["papers", "manuscripts", "publications"]),
        ("templated", ["papers", "manuscripts", "figures"]),
        ("tortured phrase*", []), ("suspect papers", []), ("flagged for", ["paper mill", "misconduct", "integrity"]),
        ("bulk retraction", []), ("mass retraction", []),
        ("retract*", ["papers", "articles", "studies", "batch", "hundreds", "integrity screening"]),
        ("integrity screening", []), ("screening flagged", []),
    ]),
    ("plagiarism", "Plagiarism and stolen work", [
        ("plagiaris*", []), ("plagiariz*", []), ("self-plagiarism", []),
        ("duplicate publication", []), ("text recycling", []),
        ("stolen", ["manuscript", "data", "research", "results", "work"]),
        ("copied", ["text", "figures", "without attribution", "verbatim", "thesis"]),
        ("similarity", ["check", "software", "score", "detected"]),
        ("thesis", ["plagiaris*", "retracted", "revoked", "investigation"]),
        ("degree revoked", []), ("doctorate revoked", []),
    ]),
    ("authorship", "Whose name is on it", [
        ("ghost author*", []), ("guest author*", []), ("gift author*", []),
        ("honorary author*", []), ("authorship dispute", []),
        ("author*", ["sold", "bought", "added", "removed", "fabricated", "unaware", "listed without"]),
        ("corresponding author", ["fake", "unreachable", "fabricated", "unaware"]),
        ("fake affiliation", []), ("affiliation", ["bought", "fake", "rented", "undisclosed"]),
        ("hyperprolific", []), ("co-author", ["unaware", "added without", "removed", "dispute"]),
    ]),
    ("images", "Image and figure manipulation", [
        ("image manipulation", []), ("image duplication", []), ("figure manipulation", []),
        ("western blot", ["duplicat*", "manipulat*", "spliced", "reused"]),
        ("spliced", ["image", "blot", "gel", "figure"]),
        ("duplicated image*", []), ("reused image*", []),
        ("photoshop*", ["figure", "image", "blot", "data"]),
        ("image integrity", []), ("image screening", []), ("proofig", []),
    ]),
    ("stats", "What the numbers were made to say", [
        ("p-hacking", []), ("p hacking", []), ("harking", []),
        ("salami slicing", []), ("salami-slicing", []),
        ("selective reporting", []), ("outcome switching", []), ("spin", ["results", "abstract", "reporting", "trial"]),
        ("statistical error*", ["paper", "study", "published", "widespread"]),
        ("statcheck", []), ("impossible", ["statistics", "numbers", "means", "grim"]),
        ("data inconsisten*", []), ("unreliable results", []),
        ("underpowered", ["study", "trial", "sample"]),
    ]),
    ("citations", "Gaming the count", [
        ("citation cartel", []), ("citation ring", []), ("citation manipulation", []),
        ("self-citation", ["excessive", "extreme", "flagged", "rate"]),
        ("coercive citation", []), ("citation farm", []),
        ("impact factor", ["gaming", "manipulat*", "inflated", "suspended", "delisted"]),
        ("delisted", ["journal", "index", "web of science", "scopus"]),
        ("indexing", ["removed", "suspended", "delisted", "revoked"]),
        ("metrics gaming", []), ("h-index", ["gaming", "inflated", "manipulat*"]),
    ]),
    ("ai", "Machine-written and undisclosed", [
        ("undisclosed ai", []), ("ai-generated", ["paper", "manuscript", "figure", "text", "undisclosed", "review"]),
        ("chatgpt", ["paper", "manuscript", "detected", "undisclosed", "phrase", "review"]),
        ("as an ai language model", []), ("regenerate response", []),
        ("llm", ["paper", "manuscript", "peer review", "policy", "detected"]),
        ("ai detection", ["paper", "manuscript", "text", "tool"]),
        ("ai policy", ["journal", "publisher", "disclosure", "authorship"]),
        ("synthetic", ["data", "images", "papers", "results"]),
    ]),
    ("retraction", "Retraction, and how late", [
        ("retraction", []), ("retracted", ["paper", "study", "article", "journal", "author"]),
        # Publishers rarely say "retract" in a headline. Retraction Watch's own
        # front page uses pulls, removes, deletes, withdraws and issues.
        ("pull*", ["paper*", "article*", "book", "edition", "copies", "journal"]),
        ("remove*", ["paper*", "article*", "book", "edition", "from the market"]),
        ("delete*", ["paper*", "article*", "study", "journal"]),
        ("withdraw*", ["paper*", "article*", "book", "edition", "study"]),
        ("issues", ["retraction*", "correction*", "expression of concern"]),
        ("misquot*", []), ("misattribut*", ["quote", "author", "source"]),
        ("expression of concern", []), ("correction notice", []),
        ("retraction watch", []), ("retraction rate", []),
        ("years to retract", []), ("detection lag", []),
        ("still cited", ["retracted", "after retraction"]),
        ("post-publication", ["review", "scrutiny", "criticism", "concerns"]),
        ("journal investigat*", []), ("publisher investigat*", []),
    ]),
    ("contamination", "What gets built on top", [
        ("cited by", ["retracted", "fraudulent", "flawed", "hundreds"]),
        ("built on", ["retracted", "fraudulent", "flawed", "fabricated"]),
        ("meta-analys*", ["contaminated", "included retracted", "flawed", "corrupted"]),
        ("systematic review", ["contaminated", "included retracted", "flawed", "withdrawn"]),
        ("clinical guideline", ["based on", "retracted", "fraudulent", "flawed", "withdrawn"]),
        ("evidence base", ["contaminated", "corrupted", "undermined", "unreliable"]),
        ("wasted research", []), ("research waste", []),
        ("failed to replicate", ["downstream", "field", "literature"]),
    ]),
    ("peerreview", "What review catches", [
        ("peer review", ["failure", "failed", "fake", "rigged", "compromised", "reform", "slow", "unpaid"]),
        ("fake peer review", []), ("rigged review", []), ("reviewer fraud", []),
        ("fake reviewer", []), ("suggested reviewer", []),
        ("editorial board", ["resign*", "mass", "conflict", "fake", "listed without"]),
        ("editor resign*", []), ("mass resignation", []),
        ("desk reject", []), ("review time", ["days", "implausible", "too fast"]),
        ("compromised", ["review", "editorial process", "special issue"]),
        ("special issue", ["guest edited", "retracted", "compromised", "flagged",
                           "manipulation", "pulled", "articles"]),
        ("peer review manipulation", []), ("review manipulation", []),
    ]),
    ("predatory", "Predatory venues", [
        ("predatory journal", []), ("predatory publisher", []), ("predatory conference", []),
        ("hijacked journal", []), ("clone journal", []),
        ("beall", []), ("questionable journal", []),
        ("pay to publish", []), ("no peer review", ["journal", "published", "accepted"]),
        ("accepted within", ["hours", "days"]),
        ("spam invitation", ["journal", "conference", "special issue"]),
    ]),
    ("replication", "What fails to replicate", [
        ("replication crisis", []), ("replication failure", []), ("failed to replicate", []),
        ("reproducib*", ["crisis", "failure", "project", "study", "poor"]),
        ("replication study", []), ("registered report", []),
        ("preregistration", []), ("pre-registration", []),
        ("effect size", ["shrank", "smaller", "failed", "inflated"]),
        ("many labs", []), ("robustness", ["check", "failed", "reanalysis"]),
        ("reanalysis", ["overturned", "contradicted", "different result"]),
    ]),
    ("publishers", "Who profits from the record", [
        ("elsevier", []), ("springer nature", []), ("wiley", ["journal", "retract*", "paper mill", "integrity"]),
        ("taylor & francis", []), ("sage", ["journal", "retract*", "paper mill", "integrity"]),
        ("article processing charge", []), ("apc", ["cost", "rose", "waiver", "charge"]),
        ("publisher profit*", []), ("profit margin", ["publisher", "journal", "academic"]),
        ("subscription", ["journal", "library", "cancel*", "big deal", "negotiat*"]),
        ("big deal", ["cancel*", "negotiat*", "library", "contract"]),
        ("transformative agreement", []), ("read and publish", []),
    ]),
    ("access", "Who can read it", [
        ("open access", []), ("paywall", ["research", "journal", "paper", "science"]),
        ("preprint", []), ("green open access", []), ("diamond open access", []),
        ("sci-hub", []), ("shadow librar*", []),
        ("public access", ["policy", "mandate", "research", "funded"]),
        ("embargo", ["research", "paper", "publication", "months"]),
        ("data availability", ["statement", "failed", "requirement", "shared", "refused"]),
        ("access to data", []), ("access to all the data", []), ("data accountability", []),
        ("authors' access", []), ("underlying data", ["access", "refused", "unavailable", "platform"]),
        ("data sharing", ["policy", "requirement", "refused", "mandate"]),
        ("code availability", []),
    ]),
    ("funding", "Who pays for the research", [
        ("industry funding", ["research", "study", "trial", "university", "department"]),
        ("industry-funded", ["study", "trial", "research", "review"]),
        ("funder", ["influence", "policy", "mandate", "withdrew", "pressure"]),
        ("research funding", ["cut", "influence", "conditions", "political", "withdrawn"]),
        ("grant", ["fraud", "misuse", "withdrawn", "conditions", "political"]),
        ("political interference", ["research", "science", "funding", "agency"]),
        ("scientific advice", ["ignored", "suppressed", "pressure", "political"]),
        ("suppress*", ["research", "findings", "report", "study", "data"]),
        ("gagging", ["scientist*", "researcher*", "clause"]),
    ]),
    ("conflicts", "Undisclosed interests", [
        ("undisclosed conflict", []), ("conflict of interest", ["undisclosed", "failed to declare",
                                        "author", "reviewer", "editor", "panel", "guideline"]),
        ("failed to disclose", []), ("financial tie*", ["undisclosed", "author", "researcher", "industry"]),
        ("consultanc*", ["undisclosed", "researcher", "author", "paid"]),
        ("declaration", ["missing", "incomplete", "failed", "interests"]),
        ("dual affiliation", []), ("undeclared funding", []),
    ]),
    ("incentives", "Publish or perish", [
        ("publish or perish", []), ("research assessment", []),
        ("cash for publication", []), ("publication bonus", []), ("cash incentive", ["publication", "paper", "journal"]),
        ("tenure", ["publication", "metrics", "pressure", "count"]),
        ("ranking", ["university", "gaming", "manipulat*", "withdrew", "boycott"]),
        ("dora", []), ("responsible metrics", []), ("leiden manifesto", []),
        ("quantity over quality", []), ("publication pressure", []),
        ("hyperauthorship", []), ("least publishable unit", []),
    ]),
    ("sleuths", "Who catches it", [
        ("pubpeer", []), ("data sleuth*", []), ("integrity sleuth*", []),
        ("elisabeth bik", []), ("image sleuth*", []),
        ("whistleblower", ["research", "academic", "university", "lab", "retaliation"]),
        ("reported concerns", ["journal", "institution", "university", "ignored"]),
        ("legal threat", ["sleuth*", "critic", "researcher", "pubpeer"]),
        ("defamation", ["sleuth*", "critic", "researcher", "integrity"]),
        ("institution", ["failed to investigate", "cleared", "investigation", "inquiry", "misconduct"]),
    ]),
    ("reform", "What is set against it", [
        ("research integrity", ["office", "policy", "framework", "training", "reform"]),
        ("integrity office", []), ("national research integrity", []),
        ("open science", []), ("reproducibility initiative", []),
        ("registered report", []), ("open peer review", []),
        ("retraction policy", []), ("cope", ["guidance", "committee on publication ethics"]),
        ("committee on publication ethics", []),
        ("screening tool", ["integrity", "paper mill", "image", "submissions"]),
        ("reform", ["peer review", "assessment", "publishing", "incentives", "metrics"]),
    ]),
]

ANCHOR = [
    # Research-integrity language. A discovery is not this subject; what is
    # wrong with the record, who produced it, what catches it and how late, is.
    "research misconduct", "scientific misconduct", "research fraud",
    "data fabrication", "fabricated data", "falsified data", "falsification",
    "paper mill", "papermill", "plagiarism", "ghost authorship",
    "authorship for sale", "image manipulation", "image duplication",
    "p-hacking", "salami slicing", "outcome switching", "selective reporting",
    "citation cartel", "citation manipulation", "impact factor gaming",
    "retraction", "retracted", "expression of concern", "retraction watch",
    "peer review", "fake peer review", "predatory journal", "hijacked journal",
    "replication crisis", "failed to replicate", "reproducibility",
    "registered report", "preregistration", "open access", "preprint",
    "article processing charge", "transformative agreement", "sci-hub",
    "data availability", "data sharing policy", "conflict of interest",
    "undisclosed funding", "publish or perish", "research assessment",
    "pubpeer", "research integrity", "committee on publication ethics",
    "office of research integrity", "open science",
]

BLOCK = [
    # ordinary science coverage — the discovery, not the record
    "scientists discover", "researchers have found", "new study suggests",
    "could help treat", "breakthrough in", "may hold the key", "sheds light on",
    "what we know about", "explained:", "here's why", "stunning images",
    "james webb", "black hole", "dinosaur", "fossil found", "new species",
    "asteroid", "eclipse", "northern lights", "space station",
    "health tips", "best supplements", "signs you may have", "home remedy",
    # trade and business filler
    "quarterly results", "share price", "product launch", "funding round",
    "film review", "video game", "recipe", "gift guide", "coupon",
    "horoscope", "sponsored content", "partner content",
]

DECIDED = [
    "approved", "signed", "awarded", "granted", "ratified", "enacted", "passed",
    "ruling", "ruled", "struck down", "upheld", "overturned", "judgment", "judgement",
    "took effect", "came into force", "repealed", "revoked", "banned", "prohibited",
    "retracted", "withdrawn", "delisted", "suspended", "resigned", "dismissed",
    "fined", "settled", "convicted", "cleared", "degree revoked", "found guilty",
]
INSTITUTIONAL = [
    "office of research integrity", "committee on publication ethics", "cope",
    "national institutes of health", "national science foundation",
    "european research council", "wellcome", "research councils",
    "retraction watch", "pubpeer", "clarivate", "web of science", "scopus",
    "nature", "science", "the lancet", "bmj", "plos", "elife", "cell",
    "university investigation", "inquiry panel", "auditor general",
    "national audit office", "parliamentary committee", "select committee",
    "official gazette", "court filing", "inspector general",
    "peer-reviewed", "published in", "study finds", "preprint", "dataset",
    "official data", "government figures", "national statistics",
]
MEASURED = [
    "per cent", "percent", "%", "one in", "1 in", "million", "billion",
    "figures show", "fell by", "rose by", "increase of", "decrease of",
    "estimated", "median", "average", "ranked", "index", "share of", "rate of",
    "papers", "retractions", "submissions", "manuscripts", "citations",
]
PENDING = [
    "proposed", "draft policy", "consultation", "under review", "expected to",
    "due to decide", "hearing scheduled", "investigation ongoing", "deadline",
    "next month", "next year", "review scheduled", "pending", "under investigation",
    "inquiry launched", "investigation opened", "awaiting", "referred to",
]


ANCHOR_C = _compile_all(ANCHOR)
BLOCK_C = _compile_all(BLOCK)
DECIDED_C = _compile_all(DECIDED)
INSTITUTIONAL_C = _compile_all(INSTITUTIONAL)
MEASURED_C = _compile_all(MEASURED)
PENDING_C = _compile_all(PENDING)
# ------------------------------------------------------------------
# The subjects, in the languages the queries now ask in. Research
# fraud is reported everywhere and the wire was asking about it only
# in English: 60 stories in one language, against a section whose own
# example is a Guangzhou paper mill running 100 staff and 300
# freelancers across six countries.
# ------------------------------------------------------------------
LOCAL_TERMS = {
    "ai": [
        ("ai 생성 논문", None), ("ai生成論文", None),
        ("ai生成论文", None), ("articles générés par ia", None),
        ("articoli generati dall'ia", None), ("artigos gerados por ia", None),
        ("artículos generados por ia", None), ("door ai gegenereerde artikelen", None),
        ("ki-generierte", None),
    ],
    "conflicts": [
        ("açıklanmayan çıkar çatışması", None), ("conflicto de interés no declarado", None),
        ("conflit d'intérêts non déclaré", None), ("conflito de interesses não declarado", None),
        ("conflitto di interessi non dichiarato", None), ("konflik kepentingan tidak diungkapkan", None),
        ("nicht offengelegter interessenkonflikt", None), ("niet-gemelde belangenverstrengeling", None),
        ("nieujawniony konflikt interesów", None), ("odeklarerad intressekonflikt", None),
        ("μη δηλωμένη σύγκρουση", None), ("нераскрытый конфликт интересов", None),
        ("تضارب مصالح غير معلن", None), ("利益相反 未開示", None),
        ("未披露利益冲突", None), ("이해충돌 미공개", None),
    ],
    "fabrication": [
        ("dados falsificados", None), ("dati falsificati", None),
        ("datos falsificados", None), ("données falsifiées", None),
        ("förfalskade forskningsdata", None), ("gefälschte daten", None),
        ("kughushi data za utafiti", None), ("manipulación de datos", None),
        ("ngụy tạo dữ liệu", None), ("pemalsuan data", None),
        ("sfałszowane dane", None), ("veri sahteciliği", None),
        ("vervalste data", None), ("παραποίηση δεδομένων", None),
        ("фальсификация данных", None), ("фальсифікація даних", None),
        ("تزوير بيانات البحث", None), ("جعل داده‌های پژوهشی", None),
        ("शोध डेटा में हेराफेरी", None), ("গবেষণার তথ্য জালিয়াতি", None),
        ("ปลอมแปลงข้อมูลวิจัย", None), ("ねつ造", None),
        ("研究データ捏造", None), ("科研数据造假", None),
        ("论文造假", None), ("논문 조작", None),
        ("연구 데이터 조작", None),
    ],
    "images": [
        ("beeldmanipulatie", None), ("bildmanipulation", None),
        ("görsel manipülasyon", None), ("manipolazione di immagini", None),
        ("manipulación de imágenes", None), ("manipulacja obrazami", None),
        ("manipulation d'images", None), ("manipulação de imagens", None),
        ("манипуляция изображениями", None), ("图片不当处理", None),
        ("画像の不正加工", None), ("이미지 조작 논문", None),
    ],
    "incentives": [
        ("cárteles de citas", None), ("pubblica o muori", None),
        ("publicar o perecer", None), ("publicar ou perecer", None),
        ("publier ou périr", None), ("publikationsdruck", None),
        ("唯论文", None), ("引用卡特尔", None),
        ("論文数至上主義", None),
    ],
    "papermills": [
        ("artikelfabrik", None), ("fabbrica di articoli", None),
        ("fabryka artykułów", None), ("fábrica de artigos", None),
        ("fábrica de artículos", None), ("makale fabrikası", None),
        ("nhà máy bài báo", None), ("pabrik artikel", None),
        ("paper mill", None), ("papierfabrik", None),
        ("usine à articles", None), ("εργοστάσια άρθρων", None),
        ("фабрика статей", None), ("مصانع الأبحاث", None),
        ("کارخانه مقاله", None), ("पेपर मिल", None),
        ("โรงงานผลิตบทความ", None), ("ペーパーミル", None),
        ("代写代发", None), ("論文工場", None),
        ("论文工厂", None), ("논문 공장", None),
    ],
    "peerreview": [
        ("fraude en revisión por pares", None), ("fraude na revisão por pares", None),
        ("fraude à l'évaluation par les pairs", None), ("frode nella revisione", None),
        ("granskningsfusk", None), ("hakem değerlendirmesi sahteciliği", None),
        ("kecurangan telaah sejawat", None), ("oszustwo w recenzji", None),
        ("peer-review-betrug", None), ("peerreviewfraude", None),
        ("мошенничество в рецензировании", None), ("تزوير مراجعة الأقران", None),
        ("同行评审造假", None), ("査読不正", None),
        ("심사 부정", None),
    ],
    "retraction": [
        ("articles rétractés", None), ("articoli ritirati", None),
        ("artigos retratados", None), ("artikel ditarik", None),
        ("artículos retractados", None), ("bài báo bị rút", None),
        ("geri çekilen makaleler", None), ("indragna artiklar", None),
        ("ingetrokken artikelen", None), ("wycofane artykuły", None),
        ("zurückgezogene studien", None), ("ανακλήσεις άρθρων", None),
        ("відкликані статті", None), ("отозванные статьи", None),
        ("بازپس‌گیری مقالات", None), ("سحب أبحاث علمية", None),
        ("शोध पत्र वापस", None), ("গবেষণাপত্র প্রত্যাহার", None),
        ("บทความถูกถอน", None), ("論文撤回", None),
        ("论文撤稿", None), ("논문 철회", None),
    ],
}

for _tid, _label, _terms in TOPICS:
    _terms.extend(LOCAL_TERMS.get(_tid, []))

# ------------------------------------------------------------------
# Subjects this wire had a name for and never asked about.
#
# The terms below were already here and were well written; what was
# missing was any query aimed at them, so they held zero stories
# however much the world published. These are the phrases from the
# queries now added, so what is fetched can be filed.
# ------------------------------------------------------------------
FILL_TERMS = {
    "citations": [
        ("cartel de citations", None), ("cartel de citações", None),
        ("cartello di citazioni", None), ("citación coercitiva", None),
        ("citation coercitive", None), ("citazione coercitiva", None),
        ("citação coerciva", None), ("cártel de citas", None),
        ("erzwungene zitationen", None), ("manipolazione dell'impact factor", None),
        ("manipulación del factor", None), ("manipulation des impact-faktors", None),
        ("manipulation du facteur", None), ("manipulação do fator", None),
        ("zitationskartell", None), ("манипуляция импакт-фактором", None),
        ("принудительное цитирование", None), ("цитатный картель", None),
        ("インパクトファクター 操作", None), ("引用の強要", None),
        ("引用カルテル", None), ("引用卡特尔", None),
        ("强制引用", None), ("影响因子 操纵 期刊除名", None),
        ("영향력 지수 조작", None), ("인용 강요", None),
        ("인용 카르텔", None),
    ],
    "contamination": [
        ("article rétracté toujours", None), ("artigo retratado continua", None),
        ("artículo retractado sigue", None), ("contaminación de la", None),
        ("contamination de la", None), ("contaminação da literatura", None),
        ("kontamination der fachliteratur", None), ("zurückgezogene studie weiterhin", None),
        ("学术污染", None), ("撤回論文 引用され続ける", None),
        ("撤稿论文 仍被引用", None), ("研究不正 波及", None),
        ("부정 연구 파급", None), ("철회 논문 계속", None),
    ],
    "funding": [
        ("condiciones políticas a", None), ("conditions politiques au", None),
        ("condizioni politiche ai", None), ("condições políticas ao", None),
        ("estudio financiado por", None), ("estudo financiado pela", None),
        ("industriefinanzierte studie verzerrung", None), ("politische auflagen forschungsförderung", None),
        ("studio finanziato dall'industria", None), ("étude financée par", None),
        ("исследование, профинансированное отраслью,", None), ("политические условия финансирования", None),
        ("企业资助 研究 偏倚", None), ("業界資金 研究 偏り", None),
        ("研究資金 政治的条件", None), ("科研经费 政治 条件", None),
        ("업계 자금 연구", None), ("연구비 정치적 조건", None),
    ],
    "sleuths": [
        ("comentarios en pubpeer", None), ("comentários no pubpeer", None),
        ("commentaires pubpeer enquête", None), ("detectives de la", None),
        ("detetives da integridade", None), ("limiers de l'intégrité", None),
        ("pubpeer hinweise untersuchung", None), ("pubpeer 质疑 调查", None),
        ("wissenschaftsdetektive integrität", None), ("パブピア 指摘", None),
        ("学术打假", None), ("研究不正 追及", None),
        ("연구 부정 추적자", None), ("펍피어 지적", None),
    ],
    "stats": [
        ("erreurs statistiques dans", None), ("errores estadísticos en", None),
        ("errori statistici negli", None), ("erros estatísticos em", None),
        ("fatiamento de publicações", None), ("fragmentación de publicaciones", None),
        ("p 해킹", None), ("p-hacking", None),
        ("pubblicazioni a salame", None), ("pハッキング", None),
        ("p值操纵", None), ("salami-taktik publikationen", None),
        ("saucissonnage des publications", None), ("statistische fehler in", None),
        ("дробление публикаций", None), ("статистические ошибки в", None),
        ("サラミ出版", None), ("拆分发表", None),
        ("統計的誤り 論文", None), ("论文 统计错误", None),
        ("살라미 논문", None), ("통계 오류 논문", None),
    ],
}

for _tid, _label, _terms in TOPICS:
    _terms.extend(FILL_TERMS.get(_tid, []))


# --------------------------------------------------------------------------
# The same subjects in the languages this wire's own queries ask in, derived
# from those queries and filed under the subject each query's label names. The
# gate above was written in English; the queries were translated and it was
# not, so three quarters of what the wire fetched could not be recognised once
# it arrived. Generated — edit topics_multilingual.json, or delete the file to
# turn this off.
# --------------------------------------------------------------------------
_EXTRA_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                           "topics_multilingual.json")
if os.path.exists(_EXTRA_PATH):
    with open(_EXTRA_PATH, encoding="utf-8") as _fh:
        _EXTRA = json.load(_fh)
    TOPICS = [(tid, label, terms + [(t, g) for t, g in _EXTRA.get(tid, [])])
              for tid, label, terms in TOPICS]

TOPICS_C = [(tid, label, [(_compile(t), _compile_all(g) if g else None) for t, g in terms])
            for tid, label, terms in TOPICS]
GEO3_C = [(rid, rlabel, [(sid, slabel, [(pid, plabel, _compile_all(terms))
                                        for pid, plabel, terms in places])
                        for sid, slabel, places in subs])
          for rid, rlabel, subs in GEO3]


def relevant(text):
    """A subject has to claim the story.

    An anchor term alone used to be enough, with a fallback subject put on the
    result. That labels a story the wire never actually recognised, so a piece
    that merely mentions a market word arrives filed under a real subject. A
    story no subject will claim is refused and counted as refused instead."""
    if hit(text, BLOCK_C):
        return False
    return bool(topics_for(text))


def weight(text, standing, placed):
    """What the story contains, as a score and the reasons for it."""
    total, reasons = 0, []
    if hit(text, DECIDED_C):
        total += 2
        reasons.append("decided")
    if hit(text, INSTITUTIONAL_C):
        total += 2
        reasons.append("institutional")
    if hit(text, MEASURED_C):
        total += 1
        reasons.append("measured")
    if hit(text, PENDING_C):
        total += 1
        reasons.append("pending")
    if placed:
        total += 1
        reasons.append("located")
    if standing in ("official", "research"):
        total += 1
        reasons.append("primary source")
    return total, reasons


def topics_for(text):
    hits = []
    for tid, _label, terms in TOPICS_C:
        for term, guards in terms:
            if not hit(text, [term]):
                continue
            if guards and not hit(text, guards):
                continue
            hits.append(tid)
            break
    return hits


def places_for(text):
    """Returns (regions, subregions, places). Naming a place implies the
    subregion and region above it."""
    regions, subs, places = [], [], []
    for rid, _rl, sublist in GEO3_C:
        for sid, _sl, plist in sublist:
            for pid, _pl, terms in plist:
                if not hit(text, terms):
                    continue
                if pid not in places:
                    places.append(pid)
                if sid not in subs:
                    subs.append(sid)
                if rid not in regions:
                    regions.append(rid)
    return (regions or ["unlocated"], subs or ["unlocated"], places or ["unlocated"])


# ---------------------------------------------------------------- placement
# Coordinates for the gazetteer, a table of named places, and the routine that
# resolves a story to the most specific point it names.
#
# Nothing here decides what a story is ABOUT. That belongs to this feed's own
# relevant() and topics_for(), and must never be shadowed by anything arriving
# alongside the coordinates: a later definition wins in Python, and one
# careless slice once handed five feeds the conflict wire's vocabulary.

COORDS = {
 # --- regions ---
 "africa": [1.5, 20.0], "americas-n": [45.0, -100.0], "americas-s": [-12.0, -60.0],
 "asia-s": [22.0, 79.0], "asia-se": [2.0, 112.0], "asia-e": [40.0, 100.0],
 "mena": [28.0, 42.0], "europe": [52.0, 15.0], "oceania": [-25.0, 140.0], "polar": [78.0, 0.0],
 # --- subregions ---
 "africa-e": [1.0, 37.0], "africa-w": [10.0, -2.0], "africa-c": [0.0, 20.0],
 "africa-s": [-24.0, 24.0], "africa-n": [28.0, 12.0],
 "na-us": [39.0, -98.0], "na-ca": [58.0, -100.0], "na-mx": [23.0, -102.0],
 "la-amz": [-4.0, -62.0], "la-and": [-25.0, -68.0], "la-ca": [14.0, -87.0],
 "la-car": [8.0, -60.0], "la-br": [-13.0, -47.0],
 "sa-in": [22.0, 79.0], "sa-oth": [27.0, 85.0],
 "se-mar": [-2.0, 118.0], "se-main": [16.0, 101.0],
 "ea-e": [35.0, 118.0], "ea-c": [50.0, 80.0],
 "me-lev": [32.0, 40.0],
 "eu-n": [65.0, 20.0], "eu-o": [50.0, 15.0],
 "oc-au": [-25.0, 134.0], "oc-nz": [-41.0, 174.0], "oc-pac": [-15.0, 170.0],
 "pol-arc": [80.0, 0.0],
 # --- places: Africa ---
 "ke": [0.2, 37.9], "tz": [-6.4, 34.9], "ug": [1.4, 32.3], "et": [9.1, 40.5],
 "so": [5.2, 46.2], "rw": [-1.9, 29.9], "bi": [-3.4, 29.9], "sd": [15.6, 30.2],
 "ss": [7.9, 30.0], "mg": [-18.8, 46.9], "mz": [-18.7, 35.5], "zm": [-13.1, 27.8],
 "zw": [-19.0, 29.2], "mw": [-13.3, 34.3],
 "ng": [9.1, 8.7], "gh": [7.9, -1.0], "ci": [7.5, -5.5], "sn": [14.5, -14.5],
 "ml": [17.6, -4.0], "bf": [12.2, -1.6], "ne": [17.6, 8.1], "lr": [6.4, -9.4],
 "sl": [8.5, -11.8], "gn": [9.9, -9.7], "cm": [7.4, 12.4], "sahel": [15.0, 2.0], "horn": [8.0, 45.0],
 "cd": [-4.0, 21.8], "cg": [-0.2, 15.8], "ga": [-0.8, 11.6], "cf": [6.6, 20.9], "td": [15.5, 18.7],
 "za": [-30.6, 22.9], "bw": [-22.3, 24.7], "na": [-22.9, 18.5], "ao": [-11.2, 17.9], "ls": [-29.6, 28.2],
 "ma": [31.8, -7.1], "dz": [28.0, 1.7], "tn": [33.9, 9.5], "ly": [26.3, 17.2], "eg": [26.8, 30.8],
 # --- places: North America ---
 "us-ak": [64.0, -152.0], "us-sw": [34.5, -110.0], "us-pl": [44.0, -100.0],
 "us-pnw": [46.5, -121.0], "us-e": [35.5, -80.0], "us-hi": [20.8, -156.3],
 "ca-bc": [54.0, -125.0], "ca-pr": [52.0, -106.0], "ca-on": [49.0, -80.0],
 "ca-n": [64.0, -105.0], "ca-at": [46.5, -63.0],
 "mx-s": [17.0, -94.0], "mx-n": [28.5, -108.0],
 # --- places: Latin America ---
 "br-amz": [-4.5, -60.0], "pe-amz": [-6.0, -75.0], "co-amz": [-1.0, -72.0],
 "ec-amz": [-1.5, -76.5], "bo-amz": [-14.5, -65.0], "ve-amz": [5.0, -65.0],
 "cl": [-35.7, -71.5], "ar": [-38.4, -63.6], "pe": [-9.2, -75.0], "bo": [-16.3, -63.6],
 "py": [-23.4, -58.4], "uy": [-32.5, -55.8],
 "gt": [15.8, -90.2], "hn": [15.2, -86.2], "ni": [12.9, -85.2], "cr": [9.7, -83.8],
 "pa": [8.5, -80.8], "bz": [17.2, -88.5], "sv": [13.8, -88.9],
 "gy": [4.9, -58.9], "sr": [3.9, -56.0], "gf": [3.9, -53.1], "do": [18.7, -70.2],
 "br-ne": [-10.0, -45.0],
 # --- places: South Asia ---
 "in-c": [21.5, 82.0], "in-ne": [26.0, 93.0], "in-s": [13.0, 77.5], "in-h": [32.0, 78.0],
 "bd": [23.7, 90.4], "np": [28.4, 84.1], "pk": [30.4, 69.3], "lk": [7.9, 80.8], "bt": [27.5, 90.4],
 # --- places: Southeast Asia ---
 "id": [-2.5, 118.0], "ph": [12.9, 121.8], "my": [4.2, 109.5], "tl": [-8.9, 125.7], "pg-ind": [-6.3, 143.9],
 "th": [15.9, 100.99], "mm": [21.9, 95.96], "vn": [14.1, 108.3], "kh": [12.6, 104.99], "la": [19.9, 102.5],
 # --- places: East & Central Asia ---
 "tw": [23.7, 121.0], "jp": [36.2, 138.3], "cn": [35.9, 104.2], "kr": [36.5, 127.9], "mn": [46.9, 103.8],
 "ru-sib": [62.0, 105.0], "kz": [48.0, 66.9], "kg": [41.2, 74.8], "uz": [41.4, 64.6],
 # --- places: MENA ---
 "il": [31.5, 35.0], "jo": [30.6, 36.2], "iq": [33.2, 43.7], "ir": [32.4, 53.7],
 "sa": [24.0, 45.0], "tr": [39.0, 35.2],
 # --- places: Europe ---
 "no": [64.6, 12.0], "se": [62.0, 15.0], "fi": [64.0, 26.0], "gl": [71.7, -42.6], "ru-eu": [67.5, 35.0],
 "ua": [48.4, 31.2], "ru": [56.0, 40.0], "eu": [50.8, 4.4], "uk": [54.0, -2.5],
 "es": [40.2, -3.7], "fr": [46.6, 2.4], "de": [51.2, 10.4],
 # --- places: Oceania ---
 "au-n": [-15.0, 133.0], "au-w": [-25.0, 121.0], "au-e": [-30.0, 148.0], "au-c": [-29.0, 135.0],
 "nz": [-41.0, 174.0], "fj": [-17.7, 178.0], "nc": [-21.3, 165.5], "sb": [-9.6, 160.2],
 "vu": [-15.4, 166.9], "ws": [-13.8, -172.1],
 # --- polar ---
 "arctic": [80.0, 0.0],
}

PRECISE = {
 # --- Ukraine & Russia ---
 "kyiv": ("Kyiv", 50.45, 30.52), "kiev": ("Kyiv", 50.45, 30.52),
 "kharkiv": ("Kharkiv", 49.99, 36.23), "odesa": ("Odesa", 46.48, 30.73),
 "odessa": ("Odesa", 46.48, 30.73), "lviv": ("Lviv", 49.84, 24.03),
 "dnipro": ("Dnipro", 48.46, 35.05), "zaporizhzhia": ("Zaporizhzhia", 47.84, 35.14),
 "kherson": ("Kherson", 46.64, 32.61), "mykolaiv": ("Mykolaiv", 46.98, 31.99),
 "donetsk": ("Donetsk", 48.02, 37.80), "luhansk": ("Luhansk", 48.57, 39.31),
 "donbas": ("Donbas", 48.30, 38.20), "mariupol": ("Mariupol", 47.10, 37.55),
 "bakhmut": ("Bakhmut", 48.60, 38.00), "avdiivka": ("Avdiivka", 48.14, 37.75),
 "pokrovsk": ("Pokrovsk", 48.28, 37.18), "kupiansk": ("Kupiansk", 49.71, 37.62),
 "sumy": ("Sumy", 50.91, 34.80), "chernihiv": ("Chernihiv", 51.49, 31.29),
 "crimea": ("Crimea", 45.30, 34.40), "sevastopol": ("Sevastopol", 44.62, 33.53),
 "moscow": ("Moscow", 55.75, 37.62), "kremlin": ("Moscow", 55.75, 37.62),
 "belgorod": ("Belgorod", 50.60, 36.59), "kursk region": ("Kursk", 51.73, 36.19),
 "rostov": ("Rostov-on-Don", 47.24, 39.71), "novorossiysk": ("Novorossiysk", 44.72, 37.77),
 "st petersburg": ("St Petersburg", 59.94, 30.31), "vladivostok": ("Vladivostok", 43.12, 131.89),
 # --- Israel, Palestine, Lebanon, Syria ---
 "gaza city": ("Gaza City", 31.51, 34.45), "gaza strip": ("Gaza", 31.42, 34.35),
 "gaza": ("Gaza", 31.42, 34.35), "rafah": ("Rafah", 31.29, 34.25),
 "khan younis": ("Khan Younis", 31.34, 34.30), "deir al-balah": ("Deir al-Balah", 31.42, 34.35),
 "west bank": ("West Bank", 31.95, 35.30), "jenin": ("Jenin", 32.46, 35.30),
 "nablus": ("Nablus", 32.22, 35.26), "hebron": ("Hebron", 31.53, 35.10),
 "ramallah": ("Ramallah", 31.90, 35.21), "jerusalem": ("Jerusalem", 31.78, 35.22),
 "tel aviv": ("Tel Aviv", 32.09, 34.78), "haifa": ("Haifa", 32.79, 34.99),
 "golan": ("Golan Heights", 32.95, 35.75), "sderot": ("Sderot", 31.52, 34.60),
 "beirut": ("Beirut", 33.89, 35.50), "south lebanon": ("South Lebanon", 33.30, 35.40),
 "tyre": ("Tyre", 33.27, 35.20), "baalbek": ("Baalbek", 34.01, 36.21),
 "damascus": ("Damascus", 33.51, 36.29), "aleppo": ("Aleppo", 36.20, 37.13),
 "idlib": ("Idlib", 35.93, 36.63), "homs": ("Homs", 34.73, 36.71),
 "latakia": ("Latakia", 35.52, 35.79), "deir ez-zor": ("Deir ez-Zor", 35.34, 40.14),
 "hasakah": ("Hasakah", 36.50, 40.75), "rojava": ("North-east Syria", 36.40, 40.70),
 # --- Iraq, Iran, Gulf, Yemen ---
 "baghdad": ("Baghdad", 33.31, 44.36), "mosul": ("Mosul", 36.35, 43.13),
 "erbil": ("Erbil", 36.19, 44.01), "basra": ("Basra", 30.51, 47.78),
 "fallujah": ("Fallujah", 33.35, 43.78), "kirkuk": ("Kirkuk", 35.47, 44.39),
 "tehran": ("Tehran", 35.69, 51.39), "isfahan": ("Isfahan", 32.65, 51.67),
 "natanz": ("Natanz", 33.72, 51.73), "fordow": ("Fordow", 34.88, 50.99),
 "bandar abbas": ("Bandar Abbas", 27.19, 56.28), "strait of hormuz": ("Strait of Hormuz", 26.57, 56.25),
 "riyadh": ("Riyadh", 24.71, 46.68), "jeddah": ("Jeddah", 21.49, 39.19),
 "doha": ("Doha", 25.29, 51.53), "al udeid": ("Al Udeid air base", 25.12, 51.32),
 "abu dhabi": ("Abu Dhabi", 24.45, 54.38), "dubai": ("Dubai", 25.20, 55.27),
 "manama": ("Manama", 26.23, 50.59), "kuwait city": ("Kuwait City", 29.38, 47.99),
 "muscat": ("Muscat", 23.59, 58.41),
 "sanaa": ("Sanaa", 15.37, 44.19), "sana'a": ("Sanaa", 15.37, 44.19),
 "aden": ("Aden", 12.79, 45.02), "hodeidah": ("Hodeidah", 14.80, 42.95),
 "marib": ("Marib", 15.46, 45.32), "bab el-mandeb": ("Bab el-Mandeb", 12.58, 43.33),
 "red sea": ("Red Sea", 20.00, 38.00),
 # --- Turkey, Caucasus, Central Asia, Afghanistan ---
 "ankara": ("Ankara", 39.93, 32.86), "istanbul": ("Istanbul", 41.01, 28.98),
 "incirlik": ("Incirlik air base", 37.00, 35.43), "diyarbakir": ("Diyarbakır", 37.91, 40.24),
 "yerevan": ("Yerevan", 40.18, 44.51), "baku": ("Baku", 40.41, 49.87),
 "nagorno-karabakh": ("Nagorno-Karabakh", 39.82, 46.75), "karabakh": ("Nagorno-Karabakh", 39.82, 46.75),
 "tbilisi": ("Tbilisi", 41.72, 44.78), "abkhazia": ("Abkhazia", 43.00, 41.00),
 "south ossetia": ("South Ossetia", 42.35, 43.97),
 "kabul": ("Kabul", 34.53, 69.17), "kandahar": ("Kandahar", 31.61, 65.71),
 "herat": ("Herat", 34.35, 62.20), "jalalabad": ("Jalalabad", 34.43, 70.45),
 "dushanbe": ("Dushanbe", 38.56, 68.79), "tashkent": ("Tashkent", 41.30, 69.24),
 "almaty": ("Almaty", 43.24, 76.89), "astana": ("Astana", 51.17, 71.45),
 # --- South Asia ---
 "islamabad": ("Islamabad", 33.68, 73.05), "rawalpindi": ("Rawalpindi", 33.60, 73.04),
 "karachi": ("Karachi", 24.86, 67.01), "peshawar": ("Peshawar", 34.01, 71.58),
 "quetta": ("Quetta", 30.18, 66.98), "balochistan": ("Balochistan", 28.50, 65.50),
 "kashmir": ("Kashmir", 34.08, 74.80), "srinagar": ("Srinagar", 34.08, 74.80),
 "line of control": ("Line of Control", 34.20, 74.20),
 "new delhi": ("New Delhi", 28.61, 77.21), "mumbai": ("Mumbai", 19.08, 72.88),
 "manipur": ("Manipur", 24.66, 93.91), "assam": ("Assam", 26.20, 92.94),
 "dhaka": ("Dhaka", 23.81, 90.41), "chittagong hill tracts": ("Chittagong Hill Tracts", 22.60, 92.20),
 "colombo": ("Colombo", 6.93, 79.86), "kathmandu": ("Kathmandu", 27.72, 85.32),
 # --- East & Southeast Asia ---
 "beijing": ("Beijing", 39.90, 116.41), "shanghai": ("Shanghai", 31.23, 121.47),
 "taiwan strait": ("Taiwan Strait", 24.50, 119.50), "taipei": ("Taipei", 25.03, 121.57),
 "kinmen": ("Kinmen", 24.44, 118.32), "south china sea": ("South China Sea", 13.00, 114.00),
 "spratly": ("Spratly Islands", 9.50, 114.00), "paracel": ("Paracel Islands", 16.50, 112.00),
 "scarborough shoal": ("Scarborough Shoal", 15.15, 117.76),
 "senkaku": ("Senkaku Islands", 25.75, 123.48), "diaoyu": ("Senkaku Islands", 25.75, 123.48),
 "xinjiang": ("Xinjiang", 41.00, 85.00), "tibet": ("Tibet", 31.00, 88.00),
 "pyongyang": ("Pyongyang", 39.04, 125.76), "yongbyon": ("Yongbyon", 39.80, 125.75),
 "panmunjom": ("Panmunjom", 37.96, 126.68), "seoul": ("Seoul", 37.57, 126.98),
 "tokyo": ("Tokyo", 35.68, 139.69), "okinawa": ("Okinawa", 26.34, 127.80),
 "guam": ("Guam", 13.44, 144.79), "manila": ("Manila", 14.60, 120.98),
 "mindanao": ("Mindanao", 7.50, 124.50), "jakarta": ("Jakarta", -6.21, 106.85),
 "west papua": ("West Papua", -4.00, 138.00), "papua": ("Papua", -4.00, 138.00),
 "naypyidaw": ("Naypyidaw", 19.75, 96.10), "yangon": ("Yangon", 16.87, 96.20),
 "rakhine": ("Rakhine State", 20.10, 93.50), "kachin": ("Kachin State", 25.80, 97.40),
 "karen state": ("Karen State", 17.30, 97.70), "shan state": ("Shan State", 21.50, 98.00),
 "bangkok": ("Bangkok", 13.76, 100.50), "hanoi": ("Hanoi", 21.03, 105.85),
 "phnom penh": ("Phnom Penh", 11.56, 104.92),
 # --- Africa ---
 "khartoum": ("Khartoum", 15.50, 32.56), "omdurman": ("Omdurman", 15.65, 32.48),
 "port sudan": ("Port Sudan", 19.62, 37.22), "darfur": ("Darfur", 13.00, 24.00),
 "el fasher": ("El Fasher", 13.63, 25.35), "nyala": ("Nyala", 12.05, 24.88),
 "juba": ("Juba", 4.85, 31.58), "addis ababa": ("Addis Ababa", 9.03, 38.74),
 "tigray": ("Tigray", 14.00, 38.50), "amhara": ("Amhara", 11.50, 38.00),
 "mekelle": ("Mekelle", 13.50, 39.47), "asmara": ("Asmara", 15.34, 38.93),
 "mogadishu": ("Mogadishu", 2.05, 45.32), "kismayo": ("Kismayo", -0.36, 42.55),
 "puntland": ("Puntland", 8.50, 49.00), "nairobi": ("Nairobi", -1.29, 36.82),
 "kinshasa": ("Kinshasa", -4.44, 15.27), "goma": ("Goma", -1.68, 29.22),
 "north kivu": ("North Kivu", -0.80, 29.00), "south kivu": ("South Kivu", -3.00, 28.30),
 "bukavu": ("Bukavu", -2.51, 28.86), "ituri": ("Ituri", 1.80, 29.90),
 "bangui": ("Bangui", 4.39, 18.56), "n'djamena": ("N'Djamena", 12.13, 15.06),
 "bamako": ("Bamako", 12.64, -8.00), "gao": ("Gao", 16.27, -0.04),
 "timbuktu": ("Timbuktu", 16.77, -3.01), "ouagadougou": ("Ouagadougou", 12.37, -1.52),
 "niamey": ("Niamey", 13.51, 2.13), "lake chad": ("Lake Chad basin", 13.00, 14.00),
 "abuja": ("Abuja", 9.06, 7.49), "borno": ("Borno State", 11.80, 13.10),
 "maiduguri": ("Maiduguri", 11.83, 13.15), "lagos": ("Lagos", 6.52, 3.38),
 "tripoli libya": ("Tripoli", 32.89, 13.19), "benghazi": ("Benghazi", 32.12, 20.07),
 "cairo": ("Cairo", 30.04, 31.24), "sinai": ("Sinai", 29.50, 33.80),
 "cabo delgado": ("Cabo Delgado", -12.50, 39.50), "maputo": ("Maputo", -25.97, 32.57),
 "harare": ("Harare", -17.83, 31.05), "pretoria": ("Pretoria", -25.75, 28.19),
 "johannesburg": ("Johannesburg", -26.20, 28.05), "cape town": ("Cape Town", -33.92, 18.42),
 # --- Europe ---
 "brussels": ("Brussels", 50.85, 4.35), "the hague": ("The Hague", 52.08, 4.31),
 "geneva": ("Geneva", 46.20, 6.14), "vienna": ("Vienna", 48.21, 16.37),
 "london": ("London", 51.51, -0.13), "paris": ("Paris", 48.86, 2.35),
 "berlin": ("Berlin", 52.52, 13.40), "ramstein": ("Ramstein air base", 49.44, 7.60),
 "rome": ("Rome", 41.90, 12.50), "madrid": ("Madrid", 40.42, -3.70),
 "warsaw": ("Warsaw", 52.23, 21.01), "rzeszow": ("Rzeszów", 50.04, 22.00),
 "kaliningrad": ("Kaliningrad", 54.71, 20.51), "suwalki": ("Suwałki gap", 54.10, 23.00),
 "minsk": ("Minsk", 53.90, 27.57), "chisinau": ("Chișinău", 47.01, 28.86),
 "transnistria": ("Transnistria", 47.20, 29.20), "vilnius": ("Vilnius", 54.69, 25.28),
 "riga": ("Riga", 56.95, 24.11), "tallinn": ("Tallinn", 59.44, 24.75),
 "helsinki": ("Helsinki", 60.17, 24.94), "stockholm": ("Stockholm", 59.33, 18.07),
 "oslo": ("Oslo", 59.91, 10.75), "gotland": ("Gotland", 57.50, 18.50),
 "belgrade": ("Belgrade", 44.79, 20.45), "pristina": ("Pristina", 42.66, 21.16),
 "sarajevo": ("Sarajevo", 43.86, 18.41), "black sea": ("Black Sea", 43.40, 34.30),
 "baltic sea": ("Baltic Sea", 57.00, 19.00), "arctic circle": ("Arctic", 70.00, 20.00),
 # --- Americas ---
 "washington": ("Washington DC", 38.91, -77.04), "pentagon": ("The Pentagon", 38.87, -77.06),
 "white house": ("White House", 38.90, -77.04), "new york": ("New York", 40.71, -74.01),
 "guantanamo": ("Guantánamo Bay", 19.90, -75.15), "diego garcia": ("Diego Garcia", -7.31, 72.41),
 "ottawa": ("Ottawa", 45.42, -75.70), "mexico city": ("Mexico City", 19.43, -99.13),
 "bogota": ("Bogotá", 4.71, -74.07), "bogotá": ("Bogotá", 4.71, -74.07),
 "caracas": ("Caracas", 10.49, -66.88), "essequibo": ("Essequibo", 6.00, -59.00),
 "port-au-prince": ("Port-au-Prince", 18.59, -72.31), "havana": ("Havana", 23.11, -82.37),
 "brasilia": ("Brasília", -15.79, -47.88), "brasília": ("Brasília", -15.79, -47.88),
 "buenos aires": ("Buenos Aires", -34.60, -58.38), "santiago": ("Santiago", -33.45, -70.67),
 "lima": ("Lima", -12.05, -77.04), "quito": ("Quito", -0.18, -78.47),
 "guayaquil": ("Guayaquil", -2.19, -79.89), "tegucigalpa": ("Tegucigalpa", 14.07, -87.19),
 "san salvador": ("San Salvador", 13.69, -89.19), "guatemala city": ("Guatemala City", 14.63, -90.51),
 # --- Oceania ---
 "canberra": ("Canberra", -35.28, 149.13), "darwin": ("Darwin", -12.46, 130.85),
 "wellington": ("Wellington", -41.29, 174.78), "noumea": ("Nouméa", -22.28, 166.46),
 "nouméa": ("Nouméa", -22.28, 166.46), "bougainville": ("Bougainville", -6.20, 155.20),
 "port moresby": ("Port Moresby", -9.44, 147.18), "honiara": ("Honiara", -9.43, 159.95),
}

_AREA_WORDS = ("state", "region", "sea", "strait", "basin", "islands", "gap", "arctic",
               "heights", "tracts", "province", "peninsula", "shoal", "circle")
_AREA_NAMES = {"Darfur", "Tigray", "Amhara", "Donbas", "Crimea", "Kashmir", "Balochistan",
               "Xinjiang", "Tibet", "Sinai", "Papua", "West Papua", "North-east Syria",
               "Puntland", "Nagorno-Karabakh", "Abkhazia", "South Ossetia", "West Bank",
               "Gaza", "Transnistria", "Ituri", "Cabo Delgado", "Mindanao",
               "North Kivu", "South Kivu", "Golan Heights", "Senkaku Islands",
               "South Lebanon", "Line of Control", "Manipur", "Assam", "Gotland", "Okinawa",
               "Guam", "Bougainville", "Kinmen", "Essequibo"}

_AREA_NAMES = {"Darfur", "Tigray", "Amhara", "Donbas", "Crimea", "Kashmir", "Balochistan",
               "Xinjiang", "Tibet", "Sinai", "Papua", "West Papua", "North-east Syria",
               "Puntland", "Nagorno-Karabakh", "Abkhazia", "South Ossetia", "West Bank",
               "Gaza", "Transnistria", "Ituri", "Cabo Delgado", "Mindanao",
               "North Kivu", "South Kivu", "Golan Heights", "Senkaku Islands",
               "South Lebanon", "Line of Control", "Manipur", "Assam", "Gotland", "Okinawa",
               "Guam", "Bougainville", "Kinmen", "Essequibo"}


def _rank(label):
    low = label.lower()
    if label in _AREA_NAMES or any(w in low for w in _AREA_WORDS):
        return 0          # an area
    return 1              # a point: city, base, facility


# Cities where policing is reported. The inherited table is a war gazetteer
# and resolved almost nothing here: Rochdale, Chicago and São Paulo are the
# place names this subject actually produces.
PRECISE.update({
    'abidjan': ('Abidjan', 5.36, -4.01),
    'abuja': ('Abuja', 9.06, 7.5),
    'accra': ('Accra', 5.6, -0.19),
    'addis ababa': ('Addis Ababa', 9.03, 38.74),
    'alexandria': ('Alexandria', 31.2, 29.92),
    'algiers': ('Algiers', 36.75, 3.06),
    'almaty': ('Almaty', 43.24, 76.89),
    'amman': ('Amman', 31.95, 35.93),
    'amsterdam': ('Amsterdam', 52.37, 4.9),
    'ankara': ('Ankara', 39.93, 32.87),
    'atlanta': ('Atlanta', 33.75, -84.39),
    'auckland': ('Auckland', -36.85, 174.76),
    'baghdad': ('Baghdad', 33.31, 44.36),
    'baku': ('Baku', 40.41, 49.87),
    'baltimore': ('Baltimore', 39.29, -76.61),
    'bamako': ('Bamako', 12.64, -8.0),
    'bangkok': ('Bangkok', 13.76, 100.5),
    'barcelona': ('Barcelona', 41.39, 2.17),
    'beijing': ('Beijing', 39.9, 116.41),
    'beirut': ('Beirut', 33.89, 35.5),
    'belfast': ('Belfast', 54.6, -5.93),
    'belgrade': ('Belgrade', 44.79, 20.45),
    'bengaluru': ('Bengaluru', 12.97, 77.59),
    'berlin': ('Berlin', 52.52, 13.4),
    'birmingham': ('Birmingham', 52.49, -1.89),
    'bogota': ('Bogotá', 4.71, -74.07),
    'boston': ('Boston', 42.36, -71.06),
    'brasilia': ('Brasília', -15.79, -47.88),
    'brisbane': ('Brisbane', -27.47, 153.03),
    'bristol': ('Bristol', 51.45, -2.59),
    'brussels': ('Brussels', 50.85, 4.35),
    'bucharest': ('Bucharest', 44.43, 26.1),
    'budapest': ('Budapest', 47.5, 19.04),
    'buenos aires': ('Buenos Aires', -34.6, -58.38),
    'cairo': ('Cairo', 30.04, 31.24),
    'calais': ('Calais', 50.95, 1.86),
    'calgary': ('Calgary', 51.05, -114.07),
    'cali': ('Cali', 3.45, -76.53),
    'california': ('California', 36.78, -119.42),
    'cape town': ('Cape Town', -33.92, 18.42),
    'caracas': ('Caracas', 10.49, -66.88),
    'cardiff': ('Cardiff', 51.48, -3.18),
    'casablanca': ('Casablanca', 33.57, -7.59),
    'ceuta': ('Ceuta', 35.89, -5.32),
    'chennai': ('Chennai', 13.08, 80.27),
    'chicago': ('Chicago', 41.88, -87.63),
    'ciudad juarez': ('Ciudad Juárez', 31.74, -106.49),
    'cleveland': ('Cleveland', 41.5, -81.69),
    'colombo': ('Colombo', 6.93, 79.86),
    'connecticut': ('Connecticut', 41.6, -72.7),
    'copenhagen': ('Copenhagen', 55.68, 12.57),
    'dakar': ('Dakar', 14.72, -17.47),
    'dallas': ('Dallas', 32.78, -96.8),
    'dar es salaam': ('Dar es Salaam', -6.79, 39.21),
    'davao': ('Davao', 7.19, 125.46),
    'delhi': ('Delhi', 28.61, 77.21),
    'denver': ('Denver', 39.74, -104.99),
    'detroit': ('Detroit', 42.33, -83.05),
    'dhaka': ('Dhaka', 23.81, 90.41),
    'doha': ('Doha', 25.29, 51.53),
    'dubai': ('Dubai', 25.2, 55.27),
    'dublin': ('Dublin', 53.35, -6.26),
    'durban': ('Durban', -29.86, 31.02),
    'edinburgh': ('Edinburgh', 55.95, -3.19),
    'ferguson': ('Ferguson, MO', 38.74, -90.31),
    'florida': ('Florida', 27.66, -81.52),
    'frankfurt': ('Frankfurt', 50.11, 8.68),
    'glasgow': ('Glasgow', 55.86, -4.25),
    'guangzhou': ('Guangzhou', 23.13, 113.26),
    'guatemala city': ('Guatemala City', 14.63, -90.51),
    'guerrero': ('Guerrero', 17.55, -99.5),
    'hamburg': ('Hamburg', 53.55, 9.99),
    'hanoi': ('Hanoi', 21.03, 105.85),
    'harare': ('Harare', -17.83, 31.05),
    'helsinki': ('Helsinki', 60.17, 24.94),
    'ho chi minh city': ('Ho Chi Minh City', 10.82, 106.63),
    'hong kong': ('Hong Kong', 22.32, 114.17),
    'houston': ('Houston', 29.76, -95.37),
    'hyderabad': ('Hyderabad', 17.39, 78.49),
    'islamabad': ('Islamabad', 33.68, 73.05),
    'istanbul': ('Istanbul', 41.01, 28.98),
    'jakarta': ('Jakarta', -6.21, 106.85),
    'jerusalem': ('Jerusalem', 31.77, 35.21),
    'johannesburg': ('Johannesburg', -26.2, 28.05),
    'kabul': ('Kabul', 34.53, 69.17),
    'kampala': ('Kampala', 0.35, 32.58),
    'kano': ('Kano', 12.0, 8.52),
    'karachi': ('Karachi', 24.86, 67.01),
    'kathmandu': ('Kathmandu', 27.72, 85.32),
    'kentucky': ('Kentucky', 37.84, -84.27),
    'khartoum': ('Khartoum', 15.5, 32.56),
    'kigali': ('Kigali', -1.94, 30.06),
    'kingston': ('Kingston', 17.98, -76.79),
    'kinshasa': ('Kinshasa', -4.44, 15.27),
    'kolkata': ('Kolkata', 22.57, 88.36),
    'kuala lumpur': ('Kuala Lumpur', 3.14, 101.69),
    'kuwait city': ('Kuwait City', 29.38, 47.99),
    'lagos': ('Lagos', 6.52, 3.38),
    'lahore': ('Lahore', 31.55, 74.34),
    'lampedusa': ('Lampedusa', 35.5, 12.6),
    'leeds': ('Leeds', 53.8, -1.55),
    'lesvos': ('Lesvos', 39.1, 26.55),
    'lima': ('Lima', -12.05, -77.04),
    'lisbon': ('Lisbon', 38.72, -9.14),
    'liverpool': ('Liverpool', 53.41, -2.98),
    'los angeles': ('Los Angeles', 34.05, -118.24),
    'luanda': ('Luanda', -8.84, 13.23),
    'lusaka': ('Lusaka', -15.39, 28.32),
    'lyon': ('Lyon', 45.76, 4.83),
    'madrid': ('Madrid', 40.42, -3.7),
    'manchester': ('Manchester', 53.48, -2.24),
    'manila': ('Manila', 14.6, 120.98),
    'marseille': ('Marseille', 43.3, 5.37),
    'medellin': ('Medellín', 6.24, -75.58),
    'melbourne': ('Melbourne', -37.81, 144.96),
    'melilla': ('Melilla', 35.29, -2.94),
    'memphis': ('Memphis', 35.15, -90.05),
    'mexico city': ('Mexico City', 19.43, -99.13),
    'miami': ('Miami', 25.76, -80.19),
    'michoacan': ('Michoacán', 19.57, -101.71),
    'milan': ('Milan', 45.46, 9.19),
    'minneapolis': ('Minneapolis', 44.98, -93.27),
    'minsk': ('Minsk', 53.9, 27.57),
    'montevideo': ('Montevideo', -34.9, -56.16),
    'montreal': ('Montreal', 45.5, -73.57),
    'moscow': ('Moscow', 55.76, 37.62),
    'mumbai': ('Mumbai', 19.08, 72.88),
    'munich': ('Munich', 48.14, 11.58),
    'naples': ('Naples', 40.85, 14.27),
    'new delhi': ('New Delhi', 28.61, 77.21),
    'new jersey': ('New Jersey', 40.06, -74.41),
    'new orleans': ('New Orleans', 29.95, -90.07),
    'newcastle': ('Newcastle', 54.98, -1.61),
    'nottingham': ('Nottingham', 52.95, -1.15),
    'oakland': ('Oakland', 37.8, -122.27),
    'osaka': ('Osaka', 34.69, 135.5),
    'oslo': ('Oslo', 59.91, 10.75),
    'ottawa': ('Ottawa', 45.42, -75.7),
    'paris': ('Paris', 48.86, 2.35),
    'perth': ('Perth', -31.95, 115.86),
    'philadelphia': ('Philadelphia', 39.95, -75.17),
    'phnom penh': ('Phnom Penh', 11.56, 104.92),
    'phoenix': ('Phoenix', 33.45, -112.07),
    'port-au-prince': ('Port-au-Prince', 18.59, -72.31),
    'portland': ('Portland, OR', 45.52, -122.68),
    'prague': ('Prague', 50.08, 14.44),
    'pretoria': ('Pretoria', -25.75, 28.19),
    'quezon city': ('Quezon City', 14.68, 121.04),
    'quito': ('Quito', -0.18, -78.47),
    'rabat': ('Rabat', 34.02, -6.84),
    'rio de janeiro': ('Rio de Janeiro', -22.91, -43.17),
    'riyadh': ('Riyadh', 24.71, 46.68),
    'rochdale': ('Rochdale', 53.61, -2.16),
    'rome': ('Rome', 41.9, 12.5),
    'rotterdam': ('Rotterdam', 51.92, 4.48),
    'salvador': ('Salvador', -12.97, -38.5),
    'san antonio': ('San Antonio', 29.42, -98.49),
    'san diego': ('San Diego', 32.72, -117.16),
    'san francisco': ('San Francisco', 37.77, -122.42),
    'san salvador': ('San Salvador', 13.69, -89.22),
    'santiago': ('Santiago', -33.45, -70.67),
    'sao paulo': ('São Paulo', -23.55, -46.63),
    'seattle': ('Seattle', 47.61, -122.33),
    'seoul': ('Seoul', 37.57, 126.98),
    'shanghai': ('Shanghai', 31.23, 121.47),
    'sheffield': ('Sheffield', 53.38, -1.47),
    'shenzhen': ('Shenzhen', 22.54, 114.06),
    'singapore': ('Singapore', 1.35, 103.82),
    'sofia': ('Sofia', 42.7, 23.32),
    'st louis': ('St. Louis', 38.63, -90.2),
    'st petersburg': ('St Petersburg', 59.93, 30.34),
    'stockholm': ('Stockholm', 59.33, 18.07),
    'surabaya': ('Surabaya', -7.26, 112.75),
    'sydney': ('Sydney', -33.87, 151.21),
    'taipei': ('Taipei', 25.03, 121.57),
    'tashkent': ('Tashkent', 41.3, 69.24),
    'tbilisi': ('Tbilisi', 41.72, 44.79),
    'tegucigalpa': ('Tegucigalpa', 14.07, -87.19),
    'tehran': ('Tehran', 35.69, 51.39),
    'tel aviv': ('Tel Aviv', 32.09, 34.78),
    'texas': ('Texas', 31.0, -99.0),
    'tokyo': ('Tokyo', 35.68, 139.69),
    'toronto': ('Toronto', 43.65, -79.38),
    'tunis': ('Tunis', 36.81, 10.18),
    'urumqi': ('Ürümqi', 43.83, 87.62),
    'uttar pradesh': ('Uttar Pradesh', 26.85, 80.95),
    'uvalde': ('Uvalde, TX', 29.21, -99.79),
    'vancouver': ('Vancouver', 49.28, -123.12),
    'vienna': ('Vienna', 48.21, 16.37),
    'warsaw': ('Warsaw', 52.23, 21.01),
    'washington dc': ('Washington, DC', 38.91, -77.04),
    'wellington': ('Wellington', -41.29, 174.78),
    'winnipeg': ('Winnipeg', 49.9, -97.14),
    'xinjiang': ('Xinjiang', 41.75, 86.15),
    'yangon': ('Yangon', 16.87, 96.2),
    'yaounde': ('Yaoundé', 3.85, 11.5),
    'yerevan': ('Yerevan', 40.18, 44.51),
    'zagreb': ('Zagreb', 45.81, 15.98),
})


# --------------------------------------------------------------------------
# Subnational units and agencies.
#
# The city list runs out above city level and below country level, so a great
# deal of policy and enforcement reporting placed nowhere: "South Dakota State
# Fair", "FSSAI proposes warning labels", "Health Canada recalls". States,
# provinces and the agencies that stand in for their jurisdiction are added
# here. Existing entries are never overwritten — setdefault only.
# --------------------------------------------------------------------------
_AREA_ADDITIONS = {
    'accc':                    ('Australia', -35.31, 149.13),
    'alabama':                 ('Alabama', 32.81, -86.79),
    'alaska':                  ('Alaska', 61.37, -152.4),
    'alberta':                 ('Alberta', 53.93, -116.58),
    'andalusia':               ('Andalusia', 37.54, -4.73),
    'andhra pradesh':          ('Andhra Pradesh', 15.91, 79.74),
    'anses':                   ('France', 48.86, 2.35),
    'anvisa':                  ('Brazil', -15.79, -47.88),
    'arizona':                 ('Arizona', 33.73, -111.43),
    'arkansas':                ('Arkansas', 34.97, -92.37),
    'baden-wurttemberg':       ('Baden-Württemberg', 48.66, 9.35),
    'bahia':                   ('Bahia', -12.58, -41.7),
    'bavaria':                 ('Bavaria', 48.79, 11.5),
    'bavaria region':          ('Bavaria', 48.79, 11.5),
    'bfr':                     ('Germany', 52.52, 13.4),
    'bihar':                   ('Bihar', 25.1, 85.31),
    'british columbia':        ('British Columbia', 53.73, -127.65),
    'california':              ('California', 36.12, -119.68),
    'catalonia':               ('Catalonia', 41.59, 1.52),
    'cdc':                     ('United States', 33.8, -84.33),
    'cdsco':                   ('India', 28.61, 77.21),
    'cfia':                    ('Canada', 45.42, -75.7),
    'cma':                     ('United Kingdom', 51.5, -0.12),
    'cms':                     ('United States', 38.89, -77.03),
    'cofepris':                ('Mexico', 19.43, -99.13),
    'colorado':                ('Colorado', 39.06, -105.31),
    'connecticut':             ('Connecticut', 41.6, -72.76),
    'delaware':                ('Delaware', 39.32, -75.51),
    'echa':                    ('European Union', 60.17, 24.94),
    'efsa':                    ('European Union', 44.49, 11.34),
    'eiopa':                   ('European Union', 50.11, 8.68),
    'ema':                     ('European Union', 52.34, 4.91),
    'england':                 ('England', 52.36, -1.17),
    'epa':                     ('United States', 38.89, -77.03),
    'european food safety authority': ('European Union', 44.49, 11.34),
    'european medicines agency': ('European Union', 52.34, 4.91),
    'fda':                     ('United States', 38.91, -77.04),
    'flanders':                ('Flanders', 51.03, 4.1),
    'florida':                 ('Florida', 27.77, -81.69),
    'food standards agency':   ('United Kingdom', 51.5, -0.12),
    'fsanz':                   ('Australia', -35.31, 149.13),
    'fsis':                    ('United States', 38.91, -77.04),
    'fssai':                   ('India', 28.61, 77.21),
    'ftc':                     ('United States', 38.9, -77.03),
    'georgia':                 ('Georgia', 33.04, -83.64),
    'guangdong':               ('Guangdong', 23.38, 113.77),
    'gujarat':                 ('Gujarat', 22.26, 71.19),
    'hawaii':                  ('Hawaii', 21.09, -157.5),
    'health canada':           ('Canada', 45.42, -75.7),
    'hokkaido':                ('Hokkaido', 43.22, 142.86),
    'idaho':                   ('Idaho', 44.24, -114.48),
    'illinois':                ('Illinois', 40.35, -88.99),
    'indiana':                 ('Indiana', 39.85, -86.26),
    'iowa':                    ('Iowa', 42.01, -93.21),
    'kansas':                  ('Kansas', 38.53, -96.73),
    'karnataka':               ('Karnataka', 15.32, 75.71),
    'kentucky':                ('Kentucky', 37.67, -84.67),
    'kerala':                  ('Kerala', 10.85, 76.27),
    'lombardy':                ('Lombardy', 45.48, 9.85),
    'louisiana':               ('Louisiana', 31.17, -91.87),
    'maharashtra':             ('Maharashtra', 19.75, 75.71),
    'maine':                   ('Maine', 44.69, -69.38),
    'manitoba':                ('Manitoba', 53.76, -98.81),
    'maryland':                ('Maryland', 39.06, -76.8),
    'massachusetts':           ('Massachusetts', 42.23, -71.53),
    'mato grosso':             ('Mato Grosso', -12.68, -56.92),
    'mfds':                    ('South Korea', 36.48, 127.29),
    'mhlw':                    ('Japan', 35.68, 139.69),
    'mhra':                    ('United Kingdom', 51.5, -0.12),
    'michigan':                ('Michigan', 43.33, -84.54),
    'minas gerais':            ('Minas Gerais', -18.51, -44.55),
    'minnesota':               ('Minnesota', 45.69, -93.9),
    'mississippi':             ('Mississippi', 32.74, -89.68),
    'missouri':                ('Missouri', 38.46, -92.29),
    'montana':                 ('Montana', 46.92, -110.45),
    'nafdac':                  ('Nigeria', 9.06, 7.5),
    'nebraska':                ('Nebraska', 41.13, -98.27),
    'nevada':                  ('Nevada', 38.31, -117.06),
    'new brunswick':           ('New Brunswick', 46.57, -66.46),
    'new hampshire':           ('New Hampshire', 43.45, -71.56),
    'new jersey':              ('New Jersey', 40.3, -74.52),
    'new mexico':              ('New Mexico', 34.84, -106.25),
    'new south wales':         ('New South Wales', -31.25, 146.92),
    'new york state':          ('New York State', 42.17, -74.95),
    'newfoundland':            ('Newfoundland', 53.14, -57.66),
    'nice':                    ('United Kingdom', 53.48, -2.24),
    'nih':                     ('United States', 39.0, -77.1),
    'nmpa':                    ('China', 39.9, 116.4),
    'north carolina':          ('North Carolina', 35.63, -79.81),
    'north dakota':            ('North Dakota', 47.53, -99.78),
    'north rhine-westphalia':  ('North Rhine-Westphalia', 51.43, 7.66),
    'northern ireland':        ('Northern Ireland', 54.79, -6.49),
    'northern territory':      ('Northern Territory', -19.49, 132.55),
    'nova scotia':             ('Nova Scotia', 44.68, -63.74),
    'nvwa':                    ('Netherlands', 52.09, 5.12),
    'ofcom':                   ('United Kingdom', 51.5, -0.12),
    'ofgem':                   ('United Kingdom', 51.5, -0.12),
    'ofsted':                  ('United Kingdom', 51.5, -0.12),
    'ohio':                    ('Ohio', 40.39, -82.76),
    'okinawa':                 ('Okinawa', 26.34, 127.8),
    'oklahoma':                ('Oklahoma', 35.57, -96.93),
    'ontario':                 ('Ontario', 51.25, -85.32),
    'oregon':                  ('Oregon', 44.57, -122.07),
    'osha':                    ('United States', 38.89, -77.03),
    'para':                    ('Pará', -3.79, -52.48),
    'pennsylvania':            ('Pennsylvania', 40.59, -77.21),
    'pmda':                    ('Japan', 35.68, 139.69),
    'punjab':                  ('Punjab', 31.15, 75.34),
    'quebec province':         ('Quebec', 52.94, -73.55),
    'queensland':              ('Queensland', -20.92, 142.7),
    'rajasthan':               ('Rajasthan', 27.02, 74.22),
    'rbi':                     ('India', 19.06, 72.87),
    'rhode island':            ('Rhode Island', 41.68, -71.51),
    'sahpra':                  ('South Africa', -25.75, 28.19),
    'samr':                    ('China', 39.9, 116.4),
    'sao paulo state':         ('São Paulo State', -22.19, -48.79),
    'saskatchewan':            ('Saskatchewan', 52.94, -106.45),
    'saxony':                  ('Saxony', 51.1, 13.2),
    'scotland':                ('Scotland', 56.49, -4.2),
    'sebi':                    ('India', 19.06, 72.87),
    'sec':                     ('United States', 38.89, -77.03),
    'sfda':                    ('Saudi Arabia', 24.71, 46.68),
    'sicily':                  ('Sicily', 37.6, 14.02),
    'south australia':         ('South Australia', -30.0, 136.21),
    'south carolina':          ('South Carolina', 33.86, -80.95),
    'south dakota':            ('South Dakota', 44.3, -99.44),
    'swissmedic':              ('Switzerland', 46.95, 7.45),
    'tamil nadu':              ('Tamil Nadu', 11.13, 78.66),
    'tasmania':                ('Tasmania', -41.64, 146.32),
    'telangana':               ('Telangana', 18.11, 79.02),
    'tennessee':               ('Tennessee', 35.75, -86.69),
    'texas':                   ('Texas', 31.05, -97.56),
    'tga':                     ('Australia', -35.31, 149.13),
    'tibet':                   ('Tibet', 31.15, 88.78),
    'usda':                    ('United States', 38.91, -77.04),
    'utah':                    ('Utah', 40.15, -111.86),
    'uttar pradesh':           ('Uttar Pradesh', 26.85, 80.95),
    'vermont':                 ('Vermont', 44.05, -72.71),
    'victoria state':          ('Victoria', -36.86, 144.28),
    'virginia':                ('Virginia', 37.77, -78.17),
    'wales':                   ('Wales', 52.13, -3.78),
    'wallonia':                ('Wallonia', 50.44, 4.87),
    'washington state':        ('Washington State', 47.4, -121.49),
    'west bengal':             ('West Bengal', 22.99, 87.86),
    'west virginia':           ('West Virginia', 38.49, -80.95),
    'western australia':       ('Western Australia', -25.04, 122.29),
    'wisconsin':               ('Wisconsin', 44.27, -89.62),
    'wyoming':                 ('Wyoming', 42.76, -107.3),
    'xinjiang':                ('Xinjiang', 41.75, 84.9),
}
for _k, _v in _AREA_ADDITIONS.items():
    PRECISE.setdefault(_k, _v)

PRECISE_C = sorted(
    ((term, label, lat, lon, _compile(term), _rank(label))
     for term, (label, lat, lon) in PRECISE.items()),
    key=lambda row: (-row[5], -len(row[0])))   # points before areas, longest term first

LOCATIVE = [
 " in ", " im ", " en ", " au ", " aux ", " a ", " à ", " al ", " nel ", " nella ",
 " on ", " over ", " near ", " into ", " inside ", " across ", " throughout ",
 " sur ", " dans ", " van ", " naar ", " uit ", " w ", " na ", " do ", " em ", " no ",
 " v ", " в ", " на ", " у ", " до ", " στη", " στο", " την ", " στην ",
 "في ", "ب", "ל", "ב", "ที่", "ใน", "在", "で", "へ", "에서", "로",
]
_LOC_MAX = 12          # how far back to look for the marker


def _first_pos(text, compiled):
    """Where a place's terms first appear in the text, or None."""
    best = None
    for c in compiled:
        if isinstance(c, str):
            i = text.find(c)
        else:
            mo = c.search(text)
            i = mo.start() if mo else -1
        if i >= 0 and (best is None or i < best):
            best = i
    return best

def _is_scene(text, pos):
    """True when the name at this position is preceded by a locative marker."""
    if pos is None:
        return False
    window = text[max(0, pos - _LOC_MAX):pos]
    return any(mark in window for mark in LOCATIVE)

def precise_for(text):
    """A city, province, base or waterway named in the story. Checked before the
    country layer so a headline about Kharkiv is pinned on Kharkiv rather than
    the middle of Ukraine. Longest term wins."""
    for term, label, lat, lon, rx, _rk in PRECISE_C:
        if hit(text, [rx]):
            return label, [lat, lon]
    return None, None

def scene_first(text, places):
    """Reorder matched places so any marked as the scene of the story lead."""
    if len(places) < 2:
        return places
    terms = {}
    for _rid, _rl, sublist in GEO3_C:
        for _sid, _sl, plist in sublist:
            for pid, _pl, compiled in plist:
                if pid in places:
                    terms[pid] = compiled
    scene, rest = [], []
    for pid in places:
        (scene if _is_scene(text, _first_pos(text, terms.get(pid, []))) else rest).append(pid)
    return scene + rest


# --------------------------------------------------------------------------
# The gazetteer answers with a country; this wire's taxonomy is keyed on ids
# whose leading token is that country's ISO-2. Filing a placed story under its
# region is therefore a lookup, not a guess. Where a country is split across
# several places, only region and subregion are filled: which of the places a
# story belongs to is a question the country code cannot answer.
# --------------------------------------------------------------------------
ISO_REGION = {}
for _rid, _rlabel, _subs in GEO3:
    for _sid, _slabel, _places in _subs:
        for _pid, _plabel, _terms in _places:
            _iso = _pid.split("-")[0].lower()
            if len(_iso) == 2:
                ISO_REGION.setdefault(_iso, (_rid, _sid))


def file_by_country(row, cc):
    """Put a gazetteer-placed story in its region, if the wire has one."""
    if not cc:
        return
    hit = ISO_REGION.get(str(cc).lower())
    if not hit:
        return
    rid, sid = hit
    if not row.get("w") or row["w"] == ["unlocated"]:
        row["w"] = [rid]
    if not row.get("sr") or row["sr"] == ["unlocated"]:
        row["sr"] = [sid]



def country_for(raw, locale=None):
    """The ISO-2 the placement resolved to, or None."""
    if not _GAZETTEER:
        return None
    try:
        return galaxy_places.resolve_full(raw, locale)[4]
    except Exception:
        return None


def point_for(text, places, subs, regions, locale=None, raw=None):
    """The most specific point a story resolved to.

    The order is deliberate. This wire's own curated table goes first: it holds
    the places this subject actually turns up and the country list it was
    written against, and it beats a general gazetteer on its own ground. The
    shared gazetteer follows but only overrides at the settlement level, so a
    headline naming Kharkiv pins on Kharkiv rather than the middle of Ukraine,
    while a country reading from this wire's own table still wins over a
    country reading from the gazetteer. Then the bodies that stand for a
    jurisdiction without naming it — EFSA is a European story, ANVISA a
    Brazilian one. Last, and weakest, the country the source itself reports
    from.

    Returns (label_or_None, point_or_None, approx). approx is True only for
    that last case, where nothing in the story placed it and the point is the
    reporting locale rather than the scene. The page draws those hollow.
    """
    label, point = precise_for(text)
    if point:
        return label, point, False

    glabel, gpoint, grank = None, None, -1
    if _GAZETTEER:
        glabel, gpoint, grank, _approx = galaxy_places.resolve_ranked(raw or text)
        if grank == 3:
            return glabel, gpoint, False

    places = scene_first(text, places)
    for level in (places, subs, regions):
        for pid in level:
            if pid in COORDS:
                return None, COORDS[pid], False

    if gpoint:
        return glabel, gpoint, False

    if _GAZETTEER and locale:
        llabel, lpoint, _lrank, lapprox = galaxy_places.resolve_ranked("", locale)
        if lpoint:
            return llabel, lpoint, lapprox

    return None, None, False


def load_sources():
    with open(SOURCES_PATH, encoding="utf-8") as fh:
        cfg = json.load(fh)
    srcs = []
    for s in cfg.get("direct", []):
        srcs.append({"name": s["name"], "lang": s["lang"], "standing": s["standing"],
                     "region": s["standing"], "kind": s.get("kind", "news"), "url": s["url"]})
    for block, prefix in (("gnews", "Google News · "), ("events", "Events · ")):
        for loc in cfg.get(block, []):
            srcs.append({"name": prefix + loc["label"], "lang": loc["lang"],
                         "standing": loc["standing"], "region": loc["standing"],
                         "kind": "news", "url": build_gnews_url(loc), "gl": loc.get("gl"),
                         "query": loc.get("query", "")})
    return srcs, cfg


def run(dry_run=False, fixtures=None):
    global DEADLINE
    sources, cfg = load_sources()
    if not fixtures:
        DEADLINE = time.monotonic() + READ_BUDGET_MIN * 60
    print("Reading %d wires…" % len(sources))

    def read(src):
        if fixtures:
            path = os.path.join(fixtures, re.sub(r"[^\w.-]", "_", src["name"]) + ".xml")
            if not os.path.exists(path):
                return src, None
            with open(path, "rb") as fh:
                return src, fh.read()
        return src, fetch(src["url"])

    results = []
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for src, raw in pool.map(read, sources):
            results.append((src, raw))

    previous = []
    if os.path.exists(OUT_PATH):
        try:
            with open(OUT_PATH, encoding="utf-8") as fh:
                previous = json.load(fh).get("items", [])
        except Exception:  # noqa: BLE001
            previous = []

    seen_fp, seen_url, items = set(), set(), []

    def absorb(row):
        fp = fingerprint(row["t"])
        cu = canon_url(row["u"])
        if fp in seen_fp or cu in seen_url:
            return False
        seen_fp.add(fp)
        seen_url.add(cu)
        items.append(row)
        return True

    stats, ok_count, refused = [], 0, 0
    for src, raw in results:
        stat = {"name": src["name"], "lang": src["lang"], "standing": src["standing"],
                "region": src["standing"], "kept": 0, "refused": 0, "ok": False}
        if raw:
            stat["ok"] = True
            ok_count += 1
            for row in parse_feed(raw, src):
                text = (row["t"] + " " + row["s"]).lower()
                if hit(text, BLOCK_C):
                    stat["refused"] += 1
                    refused += 1
                    continue
                if not relevant(text):
                    stat["refused"] += 1
                    refused += 1
                    continue
                regions, subs, places = places_for(text)
                total, reasons = weight(text, src["standing"], regions != ["unlocated"])
                row["x"] = topics_for(text)
                row["w"] = regions
                row["sr"] = subs
                row["pl"] = places
                row["gl"] = src.get("gl")
                _raw = (row["t"] or "") + " " + (row.get("s") or "")
                row["pn"], row["ll"], row["pa"] = point_for(
                    text, places, subs, regions, src.get("gl"), _raw)
                if row["ll"]:
                    file_by_country(row, country_for(_raw, src.get("gl")))
                row["p"] = total
                row["y"] = reasons
                row["st"] = src["standing"]
                if absorb(row):
                    stat["kept"] += 1
        stats.append(stat)
        print("  %-36s %s" % (src["name"][:36],
                              "unreachable" if not raw
                              else "%d kept, %d refused" % (stat["kept"], stat["refused"])))

    fresh_urls = {canon_url(i["u"]) for i in items}
    for row in previous:
        if "x" not in row:
            continue
        # A retained story is placed again rather than carried forward with the
        # answer it happened to get the day it was first read. RETAIN_DAYS is
        # 45, so without this a change to the placement layer takes a month and
        # a half to reach the map, and a story never re-fetched keeps its first
        # answer for good. Rows already holding a point resolved from their own
        # text are left alone; only the unplaced and the source-country
        # approximations are reconsidered.
        if not row.get("ll") or row.get("pa"):
            _raw = ((row.get("t") or "") + " " + (row.get("s") or ""))
            row["pn"], row["ll"], row["pa"] = point_for(
                _raw.lower(), row.get("pl") or [], row.get("sr") or [],
                row.get("w") or [], row.get("gl"), _raw)
            if row["ll"]:
                file_by_country(row, country_for(_raw, row.get("gl")))
        absorb(row)

    cutoff = int(time.time() * 1000) - RETAIN_DAYS * 86400000
    items = [i for i in items if (i.get("d") or cutoff + 1) >= cutoff]
    items.sort(key=lambda i: i.get("d") or 0, reverse=True)
    items = items[:MAX_ITEMS]
    fresh = sum(1 for i in items if canon_url(i["u"]) in fresh_urls)

    languages = {}
    for loc in cfg.get("gnews", []):
        languages.setdefault(loc["lang"], re.sub(r"\s*·.*$|\s*\(.*$|\s+\d+$", "", loc["label"]).strip())
    languages.setdefault("en", "English")

    by_standing = {}
    for i in items:
        by_standing[i["st"]] = by_standing.get(i["st"], 0) + 1

    payload = {
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "counts": {"stories": len(items), "new_this_run": fresh,
                   "languages": len({i["g"] for i in items}),
                   "notable": sum(1 for i in items if i.get("p", 0) >= NOTABLE_SCORE),
                   "refused": refused,
                   "by_standing": by_standing,
                   "wires_ok": ok_count, "wires_total": len(sources)},
        "notable_score": NOTABLE_SCORE,
        "languages": languages,
        "standings": [
            {"id": "official", "label": "Funders, institutions & integrity offices"},
            {"id": "research", "label": "Journals, publishers & metascience"},
            {"id": "press", "label": "Press"},
            {"id": "rights", "label": "Open science & accountability groups"},
        ],
        "topics": [{"id": tid, "label": label} for tid, label, _ in TOPICS],
        "coords": COORDS,
        "geo": ([{"id": rid, "label": rlabel,
                  "subs": [{"id": sid, "label": slabel,
                            "places": [{"id": pid, "label": plabel} for pid, plabel, _t in places]}
                           for sid, slabel, places in subs]}
                 for rid, rlabel, subs in GEO3] +
                [{"id": "unlocated", "label": "No single region", "subs": []}]),
        "sources": stats,
        "items": items,
    }

    print("\n%d stories (%d new, %d consequential) · %d refused · %d languages · %d/%d wires answered"
          % (len(items), fresh, payload["counts"]["notable"], refused,
             payload["counts"]["languages"], ok_count, len(sources)))
    if by_standing:
        print("By standing: " + ", ".join("%s %d" % (k, v) for k, v in sorted(by_standing.items())))

    if dry_run:
        print("\n--dry-run: wire_science.json not written")
        return payload

    with open(OUT_PATH, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, separators=(",", ":"))
    print("Wrote %s (%.0f KB)" % (OUT_PATH, os.path.getsize(OUT_PATH) / 1024))
    return payload


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--fixtures")
    args = ap.parse_args()
    run(dry_run=args.dry_run, fixtures=args.fixtures)

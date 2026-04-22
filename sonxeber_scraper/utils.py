from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urljoin, urlparse


MONTHS = {
    "yanvar": 1,
    "fevral": 2,
    "mart": 3,
    "aprel": 4,
    "may": 5,
    "iyun": 6,
    "iyul": 7,
    "avqust": 8,
    "avgust": 8,
    "sentyabr": 9,
    "oktyabr": 10,
    "noyabr": 11,
    "dekabr": 12,
}

DATE_PATTERN = re.compile(
    r"(?P<day>\d{1,2})\s+(?P<month>[A-Za-zəöüğçşıƏÖÜĞÇŞİ]+)(?:\s+(?P<year>\d{4}))?"
)
ARTICLE_PATH_PATTERN = re.compile(r"^/(?P<article_id>\d{6})(?:/(?P<slug>[^/?#]+))?/?$")
METBUAT_ARTICLE_PATH_PATTERN = re.compile(
    r"^/news/(?P<article_id>\d+)(?:/(?P<slug>[^/?#]+?)(?:\.html)?)?/?$"
)
AZERTAG_ARTICLE_PATH_PATTERN = re.compile(r"^/az/xeber/(?P<article_id>\d+)/?$")
YENIXEBER_ARTICLE_PATH_PATTERN = re.compile(
    r"^/(?P<slug>[^/?#]+)-(?P<article_id>\d+)/?$"
)
TELEQRAF_ARTICLE_PATH_PATTERN = re.compile(
    r"^/news/(?P<category>[^/?#]+)/(?P<article_id>\d+)\.html/?$"
)
AXAR_ARTICLE_PATH_PATTERN = re.compile(
    r"^/news/(?P<category>[^/?#]+)/(?P<article_id>\d+)\.html/?$"
)
MILLI_ARTICLE_PATH_PATTERN = re.compile(
    r"^/(?P<category>[^/?#]+)/(?P<article_id>\d+)\.html/?$"
)
AZERBAIJAN_AZ_ARTICLE_PATH_PATTERN = re.compile(r"^/news/(?P<article_id>\d+)/?$")
IKISAHIL_ARTICLE_PATH_PATTERN = re.compile(
    r"^/post/(?P<article_id>\d{5,})-(?P<slug>[^/?#]+?)/?$"
)
IKISAHIL_SLUG_ONLY_ARTICLE_PATH_PATTERN = re.compile(r"^/post/(?P<slug>[^/?#]+)/?$")
AZXEBER_ARTICLE_PATH_PATTERN = re.compile(
    r"^/az/(?P<slug>[^/?#]+)/(?P<category>[^/?#]+)/?$"
)
APA_ARTICLE_PATH_PATTERN = re.compile(
    r"^/(?P<category>[^/?#]+)/(?: (?P<slug>[^/?#]+)-)?(?P<article_id>-?\d+)/?$".replace(" ", "")
)
XEBERLER_ARTICLE_PATH_PATTERN = re.compile(
    r"^/new/details/(?P<slug>[^/?#]+?)--(?P<article_id>\d+)\.htm/?$"
)
SIYASETINFO_ARTICLE_PATH_PATTERN = re.compile(r"^/(?P<article_id>\d{3,})/?$")
YENIAZERBAYCAN_ARTICLE_PATH_PATTERN = re.compile(
    r"^/(?P<category>[^/_?#]+)_e(?P<article_id>\d+)_az\.html/?$"
)
ISLAM_ARTICLE_PATH_PATTERN = re.compile(
    r"^/(?P<article_id>\d{4,})/(?P<slug>[^/?#]+)/?$"
)
ISLAMAZERI_ARTICLE_PATH_PATTERN = re.compile(r"^/(?P<slug>[^/?#]+?)\.html/?$")
ISLAMAZERI_IMAGE_ARTICLE_ID_PATTERN = re.compile(
    r"^/image/haber(?:/\d+x\d+)?/[^/?#]*-(?P<article_id>\d+)\.(?:jpe?g|png|webp)$",
    re.IGNORECASE,
)
SIA_ARTICLE_PATH_PATTERN = re.compile(
    r"^/az/news/(?P<category>[^/?#]+)/(?P<article_id>\d+)\.html/?$"
)
ONE_NEWS_ARTICLE_PATH_PATTERN = re.compile(
    r"^/az/news/(?P<article_id>\d{17})(?:-(?P<slug>[^/?#]+))?/?$"
)
IQTISADIYYAT_ARTICLE_PATH_PATTERN = re.compile(
    r"^/az/post/(?P<slug>[^/?#]+)-(?P<article_id>\d+)/?$"
)
OXU_SHORTLINK_PATTERN = re.compile(r"/(?P<article_id>\d{4,})/?$")
REPORT_SHORTLINK_PATTERN = re.compile(r"(?:^|/)(?P<article_id>\d{4,})(?:/)?$")
TIME_PATTERN = re.compile(r"(?P<hour>\d{1,2}):(?P<minute>\d{2})(?::(?P<second>\d{2}))?")
ISLAMAZERI_DATETIME_PATTERN = re.compile(
    r"(?P<month>\d{1,2})/(?P<day>\d{1,2})/(?P<year>\d{4})\s+"
    r"(?P<hour>\d{1,2}):(?P<minute>\d{2}):(?P<second>\d{2})\s*(?P<ampm>AM|PM)",
    re.IGNORECASE,
)
AZERBAIJAN_TZ = timezone(timedelta(hours=4))


def normalize_space(value: str) -> str:
    return " ".join(value.split())


def make_absolute_url(base_url: str, maybe_relative_url: str) -> str:
    if not maybe_relative_url:
        return ""
    return urljoin(base_url, maybe_relative_url)


def normalize_url(url: str) -> str:
    parsed = urlparse(url.strip())
    if not parsed.scheme:
        return url.rstrip("/")
    path = parsed.path or "/"
    if path != "/":
        path = path.rstrip("/")
    return parsed._replace(path=path, params="", query="", fragment="").geturl()


def extract_source_article_id(url: str) -> int | None:
    match = ARTICLE_PATH_PATTERN.match(urlparse(url).path)
    if not match:
        return None
    return int(match.group("article_id"))


def extract_slug(url: str) -> str:
    match = ARTICLE_PATH_PATTERN.match(urlparse(url).path)
    if not match:
        return ""
    return match.group("slug") or ""


def extract_oxu_shortlink_article_id(url: str) -> int | None:
    match = OXU_SHORTLINK_PATTERN.search(url.strip())
    if not match:
        return None
    return int(match.group("article_id"))


def extract_metbuat_article_id(url: str) -> int | None:
    match = METBUAT_ARTICLE_PATH_PATTERN.match(urlparse(url).path)
    if not match:
        return None
    return int(match.group("article_id"))


def extract_metbuat_slug(url: str) -> str:
    match = METBUAT_ARTICLE_PATH_PATTERN.match(urlparse(url).path)
    if not match:
        return ""
    slug = match.group("slug") or ""
    if slug.endswith(".html"):
        return slug[:-5]
    return slug


def extract_report_shortlink_article_id(value: str) -> int | None:
    match = REPORT_SHORTLINK_PATTERN.search(value.strip())
    if not match:
        return None
    return int(match.group("article_id"))


def extract_azertag_article_id(url: str) -> int | None:
    match = AZERTAG_ARTICLE_PATH_PATTERN.match(urlparse(url).path)
    if not match:
        return None
    return int(match.group("article_id"))


def extract_yenixeber_article_id(url: str) -> int | None:
    match = YENIXEBER_ARTICLE_PATH_PATTERN.match(urlparse(url).path)
    if not match:
        return None
    return int(match.group("article_id"))


def extract_yenixeber_slug(url: str) -> str:
    match = YENIXEBER_ARTICLE_PATH_PATTERN.match(urlparse(url).path)
    if not match:
        return ""
    return match.group("slug") or ""


def extract_teleqraf_article_id(url: str) -> int | None:
    match = TELEQRAF_ARTICLE_PATH_PATTERN.match(urlparse(url).path)
    if not match:
        return None
    return int(match.group("article_id"))


def extract_teleqraf_category_slug(url: str) -> str:
    match = TELEQRAF_ARTICLE_PATH_PATTERN.match(urlparse(url).path)
    if not match:
        return ""
    return match.group("category") or ""


def extract_axar_article_id(url: str) -> int | None:
    match = AXAR_ARTICLE_PATH_PATTERN.match(urlparse(url).path)
    if not match:
        return None
    return int(match.group("article_id"))


def extract_axar_category_slug(url: str) -> str:
    match = AXAR_ARTICLE_PATH_PATTERN.match(urlparse(url).path)
    if not match:
        return ""
    return match.group("category") or ""


def extract_milli_article_id(url: str) -> int | None:
    match = MILLI_ARTICLE_PATH_PATTERN.match(urlparse(url).path)
    if not match:
        return None

    category = (match.group("category") or "").strip().lower()
    if category == "tag":
        return None
    return int(match.group("article_id"))


def extract_milli_category_slug(url: str) -> str:
    match = MILLI_ARTICLE_PATH_PATTERN.match(urlparse(url).path)
    if not match:
        return ""

    category = (match.group("category") or "").strip()
    return "" if category.lower() == "tag" else category


def extract_azerbaijan_az_article_id(url: str) -> int | None:
    match = AZERBAIJAN_AZ_ARTICLE_PATH_PATTERN.match(urlparse(url).path)
    if not match:
        return None
    return int(match.group("article_id"))


def extract_ikisahil_article_id(url: str) -> int | None:
    match = IKISAHIL_ARTICLE_PATH_PATTERN.match(urlparse(url).path)
    if not match:
        return None
    slug = (match.group("slug") or "").strip().lower()
    if slug == "share":
        return None
    return int(match.group("article_id"))


def extract_ikisahil_slug(url: str) -> str:
    path = urlparse(url).path
    match = IKISAHIL_ARTICLE_PATH_PATTERN.match(path)
    if match:
        slug = (match.group("slug") or "").strip()
        return "" if slug.lower() == "share" else slug

    match = IKISAHIL_SLUG_ONLY_ARTICLE_PATH_PATTERN.match(path)
    if not match:
        return ""
    slug = (match.group("slug") or "").strip()
    return "" if slug.lower() == "share" else slug


def extract_azxeber_slug(url: str) -> str:
    match = AZXEBER_ARTICLE_PATH_PATTERN.match(urlparse(url).path)
    if not match:
        return ""
    return match.group("slug") or ""


def extract_azxeber_category_slug(url: str) -> str:
    match = AZXEBER_ARTICLE_PATH_PATTERN.match(urlparse(url).path)
    if not match:
        return ""
    return match.group("category") or ""


def extract_apa_article_id(url: str) -> int | None:
    match = APA_ARTICLE_PATH_PATTERN.match(urlparse(url).path)
    if not match:
        return None
    return int(match.group("article_id"))


def extract_apa_slug(url: str) -> str:
    match = APA_ARTICLE_PATH_PATTERN.match(urlparse(url).path)
    if not match:
        return ""
    return match.group("slug") or ""


def extract_apa_category_slug(url: str) -> str:
    match = APA_ARTICLE_PATH_PATTERN.match(urlparse(url).path)
    if not match:
        return ""
    return match.group("category") or ""


def extract_xeberler_article_id(url: str) -> int | None:
    match = XEBERLER_ARTICLE_PATH_PATTERN.match(urlparse(url).path)
    if not match:
        return None
    return int(match.group("article_id"))


def extract_xeberler_slug(url: str) -> str:
    match = XEBERLER_ARTICLE_PATH_PATTERN.match(urlparse(url).path)
    if not match:
        return ""
    return match.group("slug") or ""


def extract_siyasetinfo_article_id(url: str) -> int | None:
    match = SIYASETINFO_ARTICLE_PATH_PATTERN.match(urlparse(url).path)
    if not match:
        return None
    return int(match.group("article_id"))


def extract_yeniazerbaycan_article_id(url: str) -> int | None:
    match = YENIAZERBAYCAN_ARTICLE_PATH_PATTERN.match(urlparse(url).path)
    if not match:
        return None
    return int(match.group("article_id"))


def extract_yeniazerbaycan_category_slug(url: str) -> str:
    match = YENIAZERBAYCAN_ARTICLE_PATH_PATTERN.match(urlparse(url).path)
    if not match:
        return ""
    return match.group("category") or ""


def extract_islam_article_id(url: str) -> int | None:
    match = ISLAM_ARTICLE_PATH_PATTERN.match(urlparse(url).path)
    if not match:
        return None
    return int(match.group("article_id"))


def extract_islam_slug(url: str) -> str:
    match = ISLAM_ARTICLE_PATH_PATTERN.match(urlparse(url).path)
    if not match:
        return ""
    return match.group("slug") or ""


def extract_islamazeri_slug(url: str) -> str:
    match = ISLAMAZERI_ARTICLE_PATH_PATTERN.match(urlparse(url).path)
    if not match:
        return ""
    return match.group("slug") or ""


def extract_islamazeri_image_article_id(url: str) -> int | None:
    match = ISLAMAZERI_IMAGE_ARTICLE_ID_PATTERN.match(urlparse(url).path)
    if not match:
        return None
    return int(match.group("article_id"))


def extract_sia_article_id(url: str) -> int | None:
    match = SIA_ARTICLE_PATH_PATTERN.match(urlparse(url).path)
    if not match:
        return None
    return int(match.group("article_id"))


def extract_sia_category_slug(url: str) -> str:
    match = SIA_ARTICLE_PATH_PATTERN.match(urlparse(url).path)
    if not match:
        return ""
    return match.group("category") or ""


def extract_one_news_article_id(url: str) -> int | None:
    match = ONE_NEWS_ARTICLE_PATH_PATTERN.match(urlparse(url).path)
    if not match:
        return None
    return int(match.group("article_id"))


def extract_one_news_slug(url: str) -> str:
    match = ONE_NEWS_ARTICLE_PATH_PATTERN.match(urlparse(url).path)
    if not match:
        return ""
    return match.group("slug") or ""


def extract_iqtisadiyyat_article_id(url: str) -> int | None:
    match = IQTISADIYYAT_ARTICLE_PATH_PATTERN.match(urlparse(url).path)
    if not match:
        return None
    return int(match.group("article_id"))


def extract_iqtisadiyyat_slug(url: str) -> str:
    match = IQTISADIYYAT_ARTICLE_PATH_PATTERN.match(urlparse(url).path)
    if not match:
        return ""
    return match.group("slug") or ""


def parse_azerbaijani_date(raw_value: str) -> str:
    cleaned = normalize_space(raw_value.lower())
    match = DATE_PATTERN.search(cleaned)
    if not match:
        return ""

    month_name = match.group("month")
    month = MONTHS.get(month_name)
    if month is None:
        return ""

    year = int(match.group("year") or datetime.now(UTC).year)
    day = int(match.group("day"))
    try:
        return datetime(year, month, day).date().isoformat()
    except ValueError:
        return ""


def parse_azerbaijani_datetime(date_value: str, time_value: str = "") -> str:
    date_iso = parse_azerbaijani_date(date_value)
    if not date_iso:
        return ""
    if not time_value:
        return date_iso

    match = TIME_PATTERN.search(normalize_space(time_value))
    if not match:
        return date_iso

    hour = int(match.group("hour"))
    minute = int(match.group("minute"))
    second = int(match.group("second") or 0)
    try:
        date_object = datetime.fromisoformat(date_iso)
        return datetime(
            date_object.year,
            date_object.month,
            date_object.day,
            hour,
            minute,
            second,
            tzinfo=AZERBAIJAN_TZ,
        ).isoformat()
    except ValueError:
        return date_iso


def parse_rfc2822_datetime(raw_value: str) -> str:
    cleaned = normalize_space(raw_value)
    if not cleaned:
        return ""
    try:
        return parsedate_to_datetime(cleaned).isoformat()
    except (TypeError, ValueError, IndexError):
        return ""


def parse_azertag_datetime(raw_value: str) -> str:
    cleaned = normalize_space(raw_value)
    if not cleaned:
        return ""
    try:
        return datetime.strptime(cleaned, "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=AZERBAIJAN_TZ
        ).isoformat()
    except ValueError:
        return cleaned


def parse_axar_datetime(raw_value: str) -> str:
    cleaned = normalize_space(raw_value)
    if not cleaned:
        return ""
    try:
        return datetime.strptime(cleaned, "%Y.%m.%d / %H:%M").replace(
            tzinfo=AZERBAIJAN_TZ
        ).isoformat()
    except ValueError:
        return cleaned


def parse_apa_datetime(raw_value: str) -> str:
    cleaned = normalize_space(raw_value.replace("(UTC +04:00)", ""))
    if not cleaned:
        return ""

    date_match = DATE_PATTERN.search(cleaned.lower())
    time_match = TIME_PATTERN.search(cleaned)
    if not date_match:
        return ""

    date_text = " ".join(
        part for part in (date_match.group("day"), date_match.group("month"), date_match.group("year")) if part
    )
    time_text = time_match.group(0) if time_match else ""
    return parse_azerbaijani_datetime(date_text, time_text)


def parse_xeberler_datetime(raw_value: str) -> str:
    cleaned = normalize_space(raw_value)
    if not cleaned:
        return ""

    for fmt in ("%d-%m-%Y / %H:%M", "%d-%m-%Y"):
        try:
            parsed = datetime.strptime(cleaned, fmt)
            if fmt == "%d-%m-%Y":
                return parsed.date().isoformat()
            return parsed.replace(tzinfo=AZERBAIJAN_TZ).isoformat()
        except ValueError:
            continue

    try:
        return datetime.strptime(cleaned, "%Y-%m-%d+%H:%M:%S%z").isoformat()
    except ValueError:
        return ""


def parse_yeniazerbaycan_datetime(raw_value: str) -> str:
    cleaned = normalize_space(raw_value.replace("[", " ").replace("]", " "))
    if not cleaned:
        return ""

    iso_candidate = cleaned.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(iso_candidate)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=AZERBAIJAN_TZ).isoformat()
        return parsed.isoformat()
    except ValueError:
        pass

    for fmt in ("%d.%m.%Y %H:%M", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(cleaned, fmt).replace(
                tzinfo=AZERBAIJAN_TZ
            ).isoformat()
        except ValueError:
            continue

    for fmt in ("%d.%m.%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(cleaned, fmt).date().isoformat()
        except ValueError:
            continue

    return ""


def parse_islamazeri_datetime(raw_value: str) -> str:
    cleaned = normalize_space(raw_value)
    if not cleaned:
        return ""

    match = ISLAMAZERI_DATETIME_PATTERN.search(cleaned)
    if not match:
        return ""

    hour = int(match.group("hour"))
    if match.group("ampm").upper() == "PM" and hour != 12:
        hour += 12
    if match.group("ampm").upper() == "AM" and hour == 12:
        hour = 0

    try:
        parsed = datetime(
            int(match.group("year")),
            int(match.group("month")),
            int(match.group("day")),
            hour,
            int(match.group("minute")),
            int(match.group("second")),
            tzinfo=AZERBAIJAN_TZ,
        )
    except ValueError:
        return ""
    return parsed.isoformat()


def parse_one_news_datetime(raw_value: str) -> str:
    cleaned = normalize_space(raw_value)
    if not cleaned:
        return ""

    try:
        parsed = datetime.fromisoformat(cleaned)
    except ValueError:
        return ""
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=AZERBAIJAN_TZ)
    return parsed.isoformat()


def parse_iqtisadiyyat_datetime(raw_value: str) -> str:
    cleaned = normalize_space(raw_value.replace("(Azerbaijan Standard Time)", "").strip())
    if not cleaned:
        return ""

    iso_candidate = cleaned.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(iso_candidate)
    except ValueError:
        parsed = None
    if parsed is not None:
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=AZERBAIJAN_TZ)
        return parsed.isoformat()

    try:
        return datetime.strptime(cleaned, "%a %b %d %Y %H:%M:%S GMT%z").isoformat()
    except ValueError:
        return parse_rfc2822_datetime(cleaned)


def parse_iso_or_dotted_date(raw_value: str) -> str:
    cleaned = normalize_space(raw_value)
    if not cleaned:
        return ""
    for fmt in ("%Y-%m-%d", "%d.%m.%Y"):
        try:
            return datetime.strptime(cleaned, fmt).date().isoformat()
        except ValueError:
            continue
    return ""


def fix_utf8_mojibake(value: str) -> str:
    if not value:
        return ""
    try:
        repaired = value.encode("latin-1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return value
    return repaired


def extract_published_date_raw(value: str) -> str:
    if "Tarix:" in value:
        return normalize_space(value.split("Tarix:", 1)[1])
    return normalize_space(value)


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def stable_bigint_from_text(value: str) -> int:
    digest = hashlib.sha256(value.encode("utf-8")).digest()[:8]
    stable_value = int.from_bytes(digest, "big") & 0x7FFFFFFFFFFFFFFF
    return stable_value or 1


def json_dumps(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def unique_preserving_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if not value or value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def is_valid_article_url(url: str) -> bool:
    path = urlparse(url).path
    return ARTICLE_PATH_PATTERN.match(path) is not None


def is_valid_yenixeber_article_url(url: str) -> bool:
    path = urlparse(url).path
    return YENIXEBER_ARTICLE_PATH_PATTERN.match(path) is not None


def is_valid_teleqraf_article_url(url: str) -> bool:
    path = urlparse(url).path
    return TELEQRAF_ARTICLE_PATH_PATTERN.match(path) is not None


def is_valid_axar_article_url(url: str) -> bool:
    path = urlparse(url).path
    return AXAR_ARTICLE_PATH_PATTERN.match(path) is not None


def is_valid_milli_article_url(url: str) -> bool:
    return extract_milli_article_id(url) is not None


def is_valid_azerbaijan_az_article_url(url: str) -> bool:
    path = urlparse(url).path
    return AZERBAIJAN_AZ_ARTICLE_PATH_PATTERN.match(path) is not None


def is_valid_ikisahil_article_url(url: str) -> bool:
    path = urlparse(url).path
    return path.startswith("/post/") and bool(extract_ikisahil_slug(url))


def is_valid_azxeber_article_url(url: str) -> bool:
    path = urlparse(url).path
    return AZXEBER_ARTICLE_PATH_PATTERN.match(path) is not None


def is_valid_apa_article_url(url: str) -> bool:
    path = urlparse(url).path
    return APA_ARTICLE_PATH_PATTERN.match(path) is not None


def is_valid_xeberler_article_url(url: str) -> bool:
    path = urlparse(url).path
    return XEBERLER_ARTICLE_PATH_PATTERN.match(path) is not None


def is_valid_siyasetinfo_article_url(url: str) -> bool:
    path = urlparse(url).path
    return SIYASETINFO_ARTICLE_PATH_PATTERN.match(path) is not None


def is_valid_yeniazerbaycan_article_url(url: str) -> bool:
    path = urlparse(url).path
    return YENIAZERBAYCAN_ARTICLE_PATH_PATTERN.match(path) is not None


def is_valid_islam_article_url(url: str) -> bool:
    path = urlparse(url).path
    return ISLAM_ARTICLE_PATH_PATTERN.match(path) is not None


def is_valid_islamazeri_article_url(url: str) -> bool:
    path = urlparse(url).path
    return ISLAMAZERI_ARTICLE_PATH_PATTERN.match(path) is not None


def is_valid_sia_article_url(url: str) -> bool:
    path = urlparse(url).path
    return SIA_ARTICLE_PATH_PATTERN.match(path) is not None


def is_valid_one_news_article_url(url: str) -> bool:
    path = urlparse(url).path
    return ONE_NEWS_ARTICLE_PATH_PATTERN.match(path) is not None


def is_valid_iqtisadiyyat_article_url(url: str) -> bool:
    path = urlparse(url).path
    return IQTISADIYYAT_ARTICLE_PATH_PATTERN.match(path) is not None

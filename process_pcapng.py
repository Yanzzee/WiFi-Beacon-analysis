# Decode supported capture files for in-memory analysis or legacy Parquet.

import argparse
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_FLOOR
import os
import re
import shutil
import subprocess
import warnings

import pandas as pd
from dotenv import load_dotenv

from aruba_timestamp_parsing import (
    ARUBA_TIMESTAMP_CONTRACT_ID,
    ARUBA_TIMESTAMP_CONTRACT_VERSION,
    ArubaTimestampComparisonSource,
    ArubaTimestampDiagnosticStatus,
    parse_aruba_erm_timestamp,
)
from tshark_field_planner import (
    CaptureExtractionRequirements,
    TsharkExtractionPlan,
    configured_ap_display_filter,
    plan_tshark_extraction,
)
from vendor_adapter_registry import BEACON_ADAPTER_REGISTRY

load_dotenv("config.env")

PROCESSED_DATA_FOLDER = os.getenv("PROCESSED_DATA_FOLDER")
CAPTURES_FOLDER = os.getenv("CAPTURES_FOLDER")
THREAD_COUNT = os.getenv("THREAD_COUNT")
TSHARK_EXECUTABLE = os.getenv("TSHARK_PATH", "tshark")

ERM_UDP_PORT = int(os.getenv("ERM_UDP_PORT", "5555"))
ERM_TYPE = os.getenv("ERM_TYPE", "aruba_erm_type0")

SUPPORTED_ARUBA_ERM_TYPES = (
    "aruba_erm_type0",
    "aruba_erm_type3",
    "aruba_erm_type6",
)
CAPTURE_PROFILE_NATIVE = "native_80211"
CAPTURE_PROFILE_FIELD = "capture_profile"
CAPTURE_PROFILE_DETECTION_FIELD = "capture_profile_detection"
ERM_TYPE_FIELD = "erm_type"
CAPTURE_PROFILE_PROBE_LIMIT = 2000

CHUNK_SIZE = 10000
PCAPNG_MAGIC = b"\x0a\x0d\x0d\x0a"
PCAP_MAGIC_BYTES = {
    b"\xa1\xb2\xc3\xd4",
    b"\xd4\xc3\xb2\xa1",
    b"\xa1\xb2\x3c\x4d",
    b"\x4d\x3c\xb2\xa1",
}
CAPTURE_FILE_EXTENSIONS = {".pcapng", ".pcap"}
SUPPORTED_ARUBA_TSHARK_VERSION = "4.6.6"
_TSHARK_VERSION_PATTERN = re.compile(r"^TShark \(Wireshark\) (\d+\.\d+\.\d+)")

ARUBA_DISPLAY_TIMESTAMP_FIELD = "aruba_erm.time"
ARUBA_RAW_TIMESTAMP_FIELD = "@aruba_erm.time"
ARUBA_FRAME_EPOCH_FIELD = "frame.time_epoch"
ARUBA_FRAME_NUMBER_FIELD = "frame.number"
ARUBA_TSF_FIELD = "wlan.fixed.timestamp"
ARUBA_BEACON_INTERVAL_FIELD = "wlan.fixed.beacon"

CANONICAL_ARUBA_TIME_FIELD = "aruba_erm_time"
CANONICAL_FRAME_TIME_FIELD = "frame_time_epoch"
CANONICAL_TSF_FIELD = "tsf_us"
CANONICAL_BEACON_INTERVAL_FIELD = "beacon_interval_tu"
SOURCE_CAPTURE_FIELD = "source_capture"
SOURCE_FRAME_FIELD = "source_frame"
SOURCE_RANK_FIELD = "source_rank"
ARUBA_TRANSIENT_TIMESTAMP_FIELDS = (
    ARUBA_RAW_TIMESTAMP_FIELD,
    ARUBA_FRAME_EPOCH_FIELD,
    ARUBA_FRAME_NUMBER_FIELD,
)


class ArubaTimestampConversionError(RuntimeError):
    """Structured capture-level failure for the accepted Aruba contract."""

    def __init__(
        self,
        diagnostic_code: str,
        detail: str,
        *,
        source_path: str,
        row_position: int | None = None,
        frame_number: int | None = None,
    ) -> None:
        self.diagnostic_code = diagnostic_code
        self.contract_id = ARUBA_TIMESTAMP_CONTRACT_ID
        self.contract_version = ARUBA_TIMESTAMP_CONTRACT_VERSION
        self.source_path = source_path
        self.row_position = row_position
        self.frame_number = frame_number

        locator_parts = [f"source={source_path}"]
        if row_position is not None:
            locator_parts.append(f"export_row={row_position}")
        if frame_number is not None:
            locator_parts.append(f"reported_frame={frame_number}")
        locator = " ".join(locator_parts)
        super().__init__(
            "Aruba timestamp conversion failed: "
            f"diagnostic={diagnostic_code} "
            f"contract={self.contract_id} "
            f"version={self.contract_version} {locator}. {detail}"
        )


class CaptureProfileDetectionError(RuntimeError):
    """Failure to identify one supported capture payload safely."""


@dataclass(frozen=True)
class CaptureProfile:
    """One content-derived capture decoding profile."""

    name: str
    is_aruba_erm: bool
    erm_type: str | None
    detection_method: str
    plausible_beacon_count: int | None = None


@dataclass(frozen=True)
class CaptureExtractionResult:
    """Decoded beacon rows and the provenance needed by a corrected run."""

    observations: pd.DataFrame
    profile: CaptureProfile
    extraction_plan: TsharkExtractionPlan
    tshark_version: str


@dataclass(frozen=True)
class ArubaCaptureTimestampValidation:
    """Transient summary proving one result was handled per exported row."""

    row_count: int
    valid_count: int
    display_match_count: int
    frame_epoch_match_count: int
    frame_epoch_disagreement_count: int
    frame_epoch_unavailable_count: int
    missing_frame_number_count: int
    reused_frame_number_count: int


def resolve_tshark_executable() -> str:
    """Return the tshark executable path or raise if not available."""
    resolved = shutil.which(TSHARK_EXECUTABLE)
    if resolved:
        return resolved

    if os.path.isabs(TSHARK_EXECUTABLE) and os.path.exists(TSHARK_EXECUTABLE):
        return TSHARK_EXECUTABLE

    fallback = shutil.which("tshark")
    if fallback:
        return fallback

    raise FileNotFoundError(
        "The tshark executable was not found. "
        f"Tried '{TSHARK_EXECUTABLE}'. Ensure tshark is installed and on PATH, "
        "or set the TSHARK_PATH environment variable to the tshark executable."
    )


def validate_supported_aruba_tshark_version(tshark_executable: str) -> str:
    """Require the exact TShark version covered by the accepted contract."""

    completed = subprocess.run(
        [tshark_executable, "--version"],
        capture_output=True,
        text=True,
        check=False,
    )
    first_line = completed.stdout.splitlines()[0] if completed.stdout else ""
    match = _TSHARK_VERSION_PATTERN.match(first_line)
    observed_version = match.group(1) if match is not None else None
    if completed.returncode != 0 or observed_version != SUPPORTED_ARUBA_TSHARK_VERSION:
        observed = observed_version or "unrecognized"
        raise ArubaTimestampConversionError(
            ArubaTimestampDiagnosticStatus.UNSUPPORTED_PARSER_CONTRACT.value,
            (
                f"Expected TShark {SUPPORTED_ARUBA_TSHARK_VERSION}; "
                f"observed {observed}."
            ),
            source_path=tshark_executable,
        )
    return observed_version


def read_tshark_version(tshark_executable: str) -> str:
    """Return a recognizable TShark version for capture provenance."""

    completed = subprocess.run(
        [tshark_executable, "--version"],
        capture_output=True,
        text=True,
        check=False,
    )
    first_line = completed.stdout.splitlines()[0] if completed.stdout else ""
    match = _TSHARK_VERSION_PATTERN.match(first_line)
    if completed.returncode != 0 or match is None:
        raise RuntimeError(
            f"Could not determine the TShark version from {tshark_executable}."
        )
    return match.group(1)


def validate_capture_file(file_path, warn_on_legacy_extension=True) -> str:
    """Validate a pcapng or pcap capture and return its container format."""
    file_path = os.fspath(file_path)
    extension = os.path.splitext(file_path)[1].lower()

    with open(file_path, "rb") as capture_file:
        magic = capture_file.read(4)

    if magic == PCAPNG_MAGIC:
        if extension == ".pcap" and warn_on_legacy_extension:
            warnings.warn(
                f"{file_path} has a .pcap extension but pcapng magic bytes. "
                "Processing will continue, but rename the file to .pcapng when possible.",
                UserWarning,
                stacklevel=2,
            )
        return "pcapng"

    if magic in PCAP_MAGIC_BYTES:
        if extension == ".pcapng" and warn_on_legacy_extension:
            warnings.warn(
                f"{file_path} has a .pcapng extension but pcap magic bytes. "
                "Processing will continue, but rename the file to .pcap when possible.",
                UserWarning,
                stacklevel=2,
            )
        return "pcap"

    magic_hex = magic.hex(" ") if magic else "empty file"
    raise ValueError(
        f"{file_path} is not a supported pcapng or pcap capture based on its "
        f"magic bytes ({magic_hex})."
    )


def validate_pcapng_capture(file_path, warn_on_legacy_extension=True) -> None:
    """Backward-compatible alias for supported capture validation."""

    validate_capture_file(file_path, warn_on_legacy_extension)


_MAC_ADDRESS_PATTERN = re.compile(r"^(?:[0-9a-fA-F]{2}:){5}[0-9a-fA-F]{2}$")


def _probe_plausible_beacons(
    file_path: str,
    tshark_executable: str,
    *,
    erm_type: str | None,
) -> int:
    """Count plausible beacon/BSSID rows under one candidate decoder."""

    command = [
        tshark_executable,
        "-r",
        file_path,
        "-c",
        str(CAPTURE_PROFILE_PROBE_LIMIT),
    ]
    if erm_type is not None:
        command += [
            "-d",
            f"udp.port=={ERM_UDP_PORT},aruba_erm",
            "-d",
            f"aruba_erm.type,{erm_type}",
        ]
    command += [
        "-Y",
        "wlan.fc.type_subtype == 8 && wlan.bssid",
        "-T",
        "fields",
        "-e",
        "wlan.bssid",
    ]
    completed = subprocess.run(
        command,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        raise CaptureProfileDetectionError(
            f"TShark profile probe failed for {file_path} using "
            f"{erm_type or CAPTURE_PROFILE_NATIVE}: {completed.stderr.strip()}"
        )
    return sum(
        1
        for token in completed.stdout.splitlines()
        if _MAC_ADDRESS_PATTERN.fullmatch(token.strip()) is not None
    )


def detect_capture_profile(
    file_path,
    tshark_executable: str | None = None,
) -> CaptureProfile:
    """Identify native 802.11 or a supported Aruba ERM payload from content."""

    file_path = os.fspath(file_path)
    validate_capture_file(file_path)
    executable = tshark_executable or resolve_tshark_executable()

    native_count = _probe_plausible_beacons(
        file_path,
        executable,
        erm_type=None,
    )
    if native_count:
        return CaptureProfile(
            name=CAPTURE_PROFILE_NATIVE,
            is_aruba_erm=False,
            erm_type=None,
            detection_method="content_probe",
            plausible_beacon_count=native_count,
        )

    counts = {
        erm_type: _probe_plausible_beacons(
            file_path,
            executable,
            erm_type=erm_type,
        )
        for erm_type in SUPPORTED_ARUBA_ERM_TYPES
    }
    ordered = sorted(counts.items(), key=lambda item: item[1], reverse=True)
    best_type, best_count = ordered[0]
    runner_up_count = ordered[1][1]
    if best_count == 0:
        raise CaptureProfileDetectionError(
            f"No directly decoded 802.11 beacons or supported Aruba ERM "
            f"type 0/3/6 beacons were found in {file_path}."
        )
    if best_count == runner_up_count:
        raise CaptureProfileDetectionError(
            f"Aruba ERM payload type is ambiguous for {file_path}: {counts}."
        )
    return CaptureProfile(
        name=best_type,
        is_aruba_erm=True,
        erm_type=best_type,
        detection_method="content_probe",
        plausible_beacon_count=best_count,
    )

# Fields required from every Aruba ERM capture, independent of beacon vendor.
ARUBA_ERM_CAPTURE_FIELDS = (
    "aruba_erm.time",
    "wlan.ta",
    "wlan.bssid",
    "wlan.ssid",
    "wlan.qbss.scount",
    "wlan.qbss.cu",
    "wlan.qbss.adc",
    # source-native channel metadata
    "aruba_erm.channel",
    # frequency sources (band-agnostic)
    "radiotap.channel.freq",
    "wlan_radio.frequency",
    # channel sources; 6 GHz requires band/frequency context
    "wlan.ds.current_channel",
    "wlan.ext_tag.he_operation.6ghz.primary_channel",
    "wlan_radio.channel",
    # accepted timestamp contract inputs and transient diagnostics
    ARUBA_RAW_TIMESTAMP_FIELD,
    ARUBA_FRAME_EPOCH_FIELD,
    ARUBA_FRAME_NUMBER_FIELD,
    ARUBA_TSF_FIELD,
    ARUBA_BEACON_INTERVAL_FIELD,
)

# Fields required from every native 802.11 capture, independent of vendor.
NATIVE_80211_CAPTURE_FIELDS = (
    "frame.time_epoch",
    "frame.number",
    "wlan.ta",
    "wlan.bssid",
    "wlan.ssid",
    "wlan.qbss.scount",
    "wlan.qbss.cu",
    "wlan.qbss.adc",
    "radiotap.channel.freq",
    "wlan_radio.frequency",
    "wlan.ds.current_channel",
    "wlan.ext_tag.he_operation.6ghz.primary_channel",
    "wlan_radio.channel",
    "wlan.fixed.timestamp",
    "wlan.fixed.beacon",
)


def build_tshark_extraction_plan(
    profile: CaptureProfile,
    *,
    configured_ap_names: tuple[str, ...] | None = None,
) -> TsharkExtractionPlan:
    """Build one full-extraction plan from capture and adapter requirements."""

    if not isinstance(profile, CaptureProfile):
        raise TypeError("profile must be CaptureProfile")
    if profile.is_aruba_erm:
        if profile.erm_type not in SUPPORTED_ARUBA_ERM_TYPES:
            raise ValueError(
                f"Unsupported Aruba ERM type {profile.erm_type!r}; expected "
                f"one of {SUPPORTED_ARUBA_ERM_TYPES}."
            )
        requirements = CaptureExtractionRequirements(
            capture_profile=profile.name,
            fields=ARUBA_ERM_CAPTURE_FIELDS,
            decode_args=(
                "-d",
                f"udp.port=={ERM_UDP_PORT},aruba_erm",
                "-d",
                f"aruba_erm.type,{profile.erm_type}",
            ),
        )
    elif profile.name == CAPTURE_PROFILE_NATIVE and profile.erm_type is None:
        requirements = CaptureExtractionRequirements(
            capture_profile=profile.name,
            fields=NATIVE_80211_CAPTURE_FIELDS,
        )
    else:
        raise ValueError(f"Unsupported capture profile {profile.name!r}")
    if configured_ap_names is not None:
        requirements = CaptureExtractionRequirements(
            capture_profile=requirements.capture_profile,
            fields=requirements.fields,
            decode_args=requirements.decode_args,
            display_filter=configured_ap_display_filter(
                BEACON_ADAPTER_REGISTRY,
                configured_ap_names,
            ),
        )
    return plan_tshark_extraction(requirements, BEACON_ADAPTER_REGISTRY)


def build_tshark_export_command(
    tshark_executable: str,
    file_path,
    plan: TsharkExtractionPlan,
) -> list[str]:
    """Build the production command for one full beacon-field extraction."""

    if not isinstance(tshark_executable, str) or not tshark_executable.strip():
        raise ValueError("tshark_executable must be non-empty")
    if not isinstance(plan, TsharkExtractionPlan):
        raise TypeError("plan must be TsharkExtractionPlan")
    command = [
        tshark_executable,
        "-r",
        os.fspath(file_path),
        "-M",
        "100000",
        "-T",
        "fields",
        *plan.decode_args,
        "-Y",
        plan.display_filter,
        "-E",
        "header=y",
        "-E",
        "separator=,",
        "-E",
        "quote=d",
    ]
    for field in plan.fields:
        command.extend(["-e", field])
    return command

# --- helper to decode hex-encoded SSIDs ---
HEX_RE = re.compile(r"^[0-9a-fA-F]+$")

def _looks_like_hex(s: str) -> bool:
    s = s.strip()
    return len(s) > 0 and len(s) % 2 == 0 and HEX_RE.fullmatch(s) is not None

def _decode_hex_ssid(s: str) -> str:
    """Decode a hex string to UTF-8; fall back to original on error."""
    s = s.strip()
    try:
        return bytes.fromhex(s).decode("utf-8", errors="replace")
    except Exception:
        return s

def normalize_ssid_column(df: pd.DataFrame) -> None:
    """
    Inspect wlan.ssid values to see if tshark is outputting hex.
    If most non-empty values look like hex, decode them. Then
    cast to pandas string dtype.
    """
    if "wlan.ssid" not in df.columns:
        return

    s = df["wlan.ssid"].astype("string")

    # Drop NaNs/empties for detection
    non_empty = s.dropna().str.strip()
    non_empty = non_empty[non_empty != ""]
    if non_empty.empty:
        df["wlan.ssid"] = s
        return

    sample = non_empty.head(100)
    hex_count = sum(_looks_like_hex(v) for v in sample)
    frac_hex = hex_count / len(sample)

    # Heuristic: if majority look like hex, assume tshark is in hex mode
    if frac_hex > 0.7:
        decoded = s.map(
            lambda v: _decode_hex_ssid(v) if v is not None and _looks_like_hex(str(v)) else v
        )
        df["wlan.ssid"] = decoded.astype("string")
    else:
        # Already human-readable text; just enforce string dtype
        df["wlan.ssid"] = s.astype("string")


def validate_aruba_timestamp_rows(
    df: pd.DataFrame,
    *,
    source_path: str,
) -> ArubaCaptureTimestampValidation:
    """Validate every Aruba row in input order without changing the dataframe."""

    summary, _ = _validate_and_normalize_aruba_timestamp_rows(
        df,
        source_path=source_path,
    )
    return summary


def _validate_and_normalize_aruba_timestamp_rows(
    df: pd.DataFrame,
    *,
    source_path: str,
) -> tuple[ArubaCaptureTimestampValidation, pd.Series]:
    """Validate Aruba timestamps and return their canonical UTC instants."""

    missing_fields = [
        field for field in ARUBA_TRANSIENT_TIMESTAMP_FIELDS if field not in df.columns
    ]
    if df.index.name != ARUBA_DISPLAY_TIMESTAMP_FIELD:
        missing_fields.append(ARUBA_DISPLAY_TIMESTAMP_FIELD)
    if missing_fields:
        raise ArubaTimestampConversionError(
            ArubaTimestampDiagnosticStatus.UNSUPPORTED_PARSER_CONTRACT.value,
            "Missing required TShark timestamp fields: " + ", ".join(missing_fields),
            source_path=source_path,
        )

    valid_count = 0
    display_match_count = 0
    frame_epoch_match_count = 0
    frame_epoch_disagreement_count = 0
    frame_epoch_unavailable_count = 0
    missing_frame_number_count = 0
    reused_frame_number_count = 0
    previous_frame_number = None
    canonical_instants = []

    rows = zip(
        df.index,
        df[ARUBA_RAW_TIMESTAMP_FIELD].array,
        df[ARUBA_FRAME_EPOCH_FIELD].array,
        df[ARUBA_FRAME_NUMBER_FIELD].array,
    )
    for row_position, (display_value, raw_value, epoch_value, frame_value) in enumerate(
        rows,
        start=1,
    ):
        display_token = _optional_text(display_value)
        raw_token = _optional_text(raw_value)
        frame_epoch = _optional_text(epoch_value)
        frame_number = _optional_frame_number(frame_value)
        if frame_number is None:
            missing_frame_number_count += 1
        elif previous_frame_number is not None and frame_number <= previous_frame_number:
            reused_frame_number_count += 1
        if frame_number is not None:
            previous_frame_number = frame_number
        if frame_epoch is None:
            frame_epoch_unavailable_count += 1

        result = parse_aruba_erm_timestamp(
            raw_token,
            contract_id=ARUBA_TIMESTAMP_CONTRACT_ID,
            contract_version=ARUBA_TIMESTAMP_CONTRACT_VERSION,
            display_token=display_token,
            frame_time_epoch=frame_epoch,
            frame_number=frame_number,
        )
        if not result.is_valid:
            raise ArubaTimestampConversionError(
                result.diagnostic_code,
                result.diagnostic_detail,
                source_path=source_path,
                row_position=row_position,
                frame_number=frame_number,
            )
        valid_count += 1
        canonical_instants.append(result.canonical_instant)

        for diagnostic in result.comparison_diagnostics:
            if diagnostic.source is ArubaTimestampComparisonSource.DISPLAY_TOKEN:
                if (
                    diagnostic.status
                    is ArubaTimestampDiagnosticStatus.DISPLAY_TIMESTAMP_MATCH
                ):
                    display_match_count += 1
                    continue
                raise ArubaTimestampConversionError(
                    diagnostic.status.value,
                    diagnostic.detail,
                    source_path=source_path,
                    row_position=row_position,
                    frame_number=frame_number,
                )

            if diagnostic.status is ArubaTimestampDiagnosticStatus.FRAME_EPOCH_MATCH:
                frame_epoch_match_count += 1
            elif (
                diagnostic.status
                is ArubaTimestampDiagnosticStatus.FRAME_EPOCH_DISAGREEMENT
            ):
                frame_epoch_disagreement_count += 1
            else:
                frame_epoch_unavailable_count += 1

    summary = ArubaCaptureTimestampValidation(
        row_count=len(df),
        valid_count=valid_count,
        display_match_count=display_match_count,
        frame_epoch_match_count=frame_epoch_match_count,
        frame_epoch_disagreement_count=frame_epoch_disagreement_count,
        frame_epoch_unavailable_count=frame_epoch_unavailable_count,
        missing_frame_number_count=missing_frame_number_count,
        reused_frame_number_count=reused_frame_number_count,
    )
    canonical = pd.Series(
        pd.to_datetime(canonical_instants, utc=True),
        index=df.index,
        dtype="datetime64[ns, UTC]",
    )
    return summary, canonical


def _optional_text(value) -> str | None:
    if value is None or pd.isna(value):
        return None
    return str(value)


def _optional_frame_number(value) -> int | None:
    if value is None or pd.isna(value):
        return None
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return None


def _parse_frame_epoch(values: pd.Series) -> pd.Series:
    """Parse TShark epoch-second text without losing fractional precision."""

    instants = []
    for value in values.array:
        if value is None or pd.isna(value) or str(value).strip() == "":
            instants.append(pd.NaT)
            continue
        try:
            seconds = Decimal(str(value))
            if not seconds.is_finite():
                raise InvalidOperation
            nanoseconds = int(
                (seconds * Decimal(1_000_000_000)).to_integral_value(
                    rounding=ROUND_FLOOR
                )
            )
            instants.append(pd.Timestamp(nanoseconds, unit="ns", tz="UTC"))
        except (InvalidOperation, ValueError, OverflowError):
            instants.append(pd.NaT)
    return pd.Series(
        instants,
        index=values.index,
        dtype="datetime64[ns, UTC]",
    )


def _parse_nullable_unsigned(values: pd.Series, dtype: str) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    numeric = numeric.where(numeric >= 0)
    return numeric.astype(dtype)


def rank_capture_sources(paths) -> list[tuple[str, int]]:
    """Assign lower precedence ranks by normalized absolute capture path."""

    materialized = [os.fspath(path) for path in paths]
    ordered = sorted(
        materialized,
        key=lambda path: os.path.normcase(
            os.path.normpath(os.path.abspath(path))
        ),
    )
    return [(path, rank) for rank, path in enumerate(ordered)]


def _explicit_capture_profile(erm: bool, erm_type: str | None) -> CaptureProfile:
    if not erm:
        return CaptureProfile(
            name=CAPTURE_PROFILE_NATIVE,
            is_aruba_erm=False,
            erm_type=None,
            detection_method="explicit",
        )
    selected_type = erm_type or ERM_TYPE
    if selected_type not in SUPPORTED_ARUBA_ERM_TYPES:
        raise ValueError(
            f"Unsupported Aruba ERM type {selected_type!r}; expected one of "
            f"{SUPPORTED_ARUBA_ERM_TYPES}."
        )
    return CaptureProfile(
        name=selected_type,
        is_aruba_erm=True,
        erm_type=selected_type,
        detection_method="explicit",
    )


def extract_capture_beacons(
    file_path,
    erm: bool | None = None,
    source_rank=0,
    erm_type: str | None = None,
    configured_ap_names: tuple[str, ...] | None = None,
) -> CaptureExtractionResult:
    """Decode one supported capture into normalized in-memory beacon rows."""

    file_path = os.fspath(file_path)
    if (
        not isinstance(source_rank, int)
        or isinstance(source_rank, bool)
        or source_rank < 0
    ):
        raise ValueError("source_rank must be a non-negative integer")
    if erm is not None:
        validate_capture_file(file_path)

    tshark_executable = resolve_tshark_executable()
    if erm is None:
        profile = detect_capture_profile(file_path, tshark_executable)
        print(
            f"Detected {profile.name} from packet content "
            f"({profile.plausible_beacon_count} plausible beacons in probe)."
        )
    else:
        profile = _explicit_capture_profile(erm, erm_type)
    if profile.is_aruba_erm:
        tshark_version = validate_supported_aruba_tshark_version(
            tshark_executable
        )
    else:
        tshark_version = read_tshark_version(tshark_executable)
    extraction_plan = build_tshark_extraction_plan(
        profile,
        configured_ap_names=configured_ap_names,
    )
    tshark_command = build_tshark_export_command(
        tshark_executable,
        file_path,
        extraction_plan,
    )

    # Read tshark CSV directly into pandas
    with subprocess.Popen(
        tshark_command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ) as proc:
        index_col = (
            "aruba_erm.time" if profile.is_aruba_erm else "frame.time_epoch"
        )
        try:
            read_options = {
                "sep": ",",
                "quotechar": '"',
                "index_col": index_col,
            }
            if profile.is_aruba_erm:
                read_options["dtype"] = {
                    ARUBA_RAW_TIMESTAMP_FIELD: "string",
                    ARUBA_FRAME_EPOCH_FIELD: "string",
                    ARUBA_FRAME_NUMBER_FIELD: "Int64",
                    ARUBA_TSF_FIELD: "string",
                    ARUBA_BEACON_INTERVAL_FIELD: "string",
                }
            else:
                read_options["dtype"] = {
                    ARUBA_FRAME_NUMBER_FIELD: "Int64",
                    ARUBA_TSF_FIELD: "string",
                    ARUBA_BEACON_INTERVAL_FIELD: "string",
                }
            df = pd.read_csv(proc.stdout, **read_options)
        except Exception as exc:
            proc.kill()
            stderr = proc.stderr.read() if proc.stderr is not None else ""
            raise RuntimeError(
                f"Failed to parse tshark output for {file_path}: {exc}. "
                f"tshark stderr:\n{stderr}"
            ) from exc

        proc.stdout.close()
        proc.wait()
        if proc.returncode != 0:
            stderr = proc.stderr.read() if proc.stderr is not None else ""
            raise RuntimeError(
                f"tshark failed processing {file_path} with exit code {proc.returncode}. "
                f"Command: {' '.join(tshark_command)}\n"
                f"stderr:\n{stderr}"
            )

    if profile.is_aruba_erm:
        raw_present = df[ARUBA_RAW_TIMESTAMP_FIELD].astype("string").str.strip().ne(
            ""
        ).fillna(False)
        canonical_aruba_time = pd.Series(
            pd.NaT,
            index=df.index,
            dtype="datetime64[ns, UTC]",
        )
        if raw_present.any():
            _, parsed_aruba_time = _validate_and_normalize_aruba_timestamp_rows(
                df.loc[raw_present],
                source_path=file_path,
            )
            canonical_aruba_time.loc[raw_present] = parsed_aruba_time
        df[CANONICAL_ARUBA_TIME_FIELD] = canonical_aruba_time
        df[CANONICAL_FRAME_TIME_FIELD] = _parse_frame_epoch(
            df[ARUBA_FRAME_EPOCH_FIELD]
        )
        missing_observation_time = (
            df[CANONICAL_ARUBA_TIME_FIELD].isna()
            & df[CANONICAL_FRAME_TIME_FIELD].isna()
        )
        if missing_observation_time.any():
            first_position = int(missing_observation_time.to_numpy().nonzero()[0][0])
            frame_number = _optional_frame_number(
                df.iloc[first_position][ARUBA_FRAME_NUMBER_FIELD]
            )
            raise ArubaTimestampConversionError(
                "source_timestamp_unavailable",
                "Neither Aruba ERM time nor frame.time_epoch is usable.",
                source_path=file_path,
                row_position=first_position + 1,
                frame_number=frame_number,
            )
        df[CANONICAL_TSF_FIELD] = _parse_nullable_unsigned(
            df[ARUBA_TSF_FIELD],
            "UInt64",
        )
        df[CANONICAL_BEACON_INTERVAL_FIELD] = _parse_nullable_unsigned(
            df[ARUBA_BEACON_INTERVAL_FIELD],
            "UInt32",
        )
        df[SOURCE_CAPTURE_FIELD] = pd.Series(
            os.path.basename(file_path),
            index=df.index,
            dtype="string",
        )
        df[SOURCE_FRAME_FIELD] = pd.to_numeric(
            df[ARUBA_FRAME_NUMBER_FIELD], errors="coerce"
        ).astype("Int64")
        df[SOURCE_RANK_FIELD] = pd.Series(
            source_rank,
            index=df.index,
            dtype="Int64",
        )
        df.drop(columns=list(ARUBA_TRANSIENT_TIMESTAMP_FIELDS), inplace=True)
    else:
        frame_epoch_values = pd.Series(
            df.index.astype("string"),
            index=df.index,
            dtype="string",
        )
        df[CANONICAL_ARUBA_TIME_FIELD] = pd.Series(
            pd.NaT,
            index=df.index,
            dtype="datetime64[ns, UTC]",
        )
        df[CANONICAL_FRAME_TIME_FIELD] = _parse_frame_epoch(frame_epoch_values)
        if df[CANONICAL_FRAME_TIME_FIELD].isna().any():
            raise RuntimeError(
                f"Native capture {file_path} contains an unusable frame.time_epoch."
            )
        df[CANONICAL_TSF_FIELD] = _parse_nullable_unsigned(
            df[ARUBA_TSF_FIELD],
            "UInt64",
        )
        df[CANONICAL_BEACON_INTERVAL_FIELD] = _parse_nullable_unsigned(
            df[ARUBA_BEACON_INTERVAL_FIELD],
            "UInt32",
        )
        df[SOURCE_CAPTURE_FIELD] = pd.Series(
            os.path.basename(file_path),
            index=df.index,
            dtype="string",
        )
        df[SOURCE_FRAME_FIELD] = pd.to_numeric(
            df[ARUBA_FRAME_NUMBER_FIELD], errors="coerce"
        ).astype("Int64")
        df[SOURCE_RANK_FIELD] = pd.Series(
            source_rank,
            index=df.index,
            dtype="Int64",
        )
        df.drop(columns=[ARUBA_FRAME_NUMBER_FIELD], inplace=True)

    df[CAPTURE_PROFILE_FIELD] = pd.Series(
        profile.name,
        index=df.index,
        dtype="string",
    )
    df[CAPTURE_PROFILE_DETECTION_FIELD] = pd.Series(
        profile.detection_method,
        index=df.index,
        dtype="string",
    )
    df[ERM_TYPE_FIELD] = pd.Series(
        profile.erm_type,
        index=df.index,
        dtype="string",
    )

    # Build a single frequency_mhz column (numeric) from available sources
    freq = pd.to_numeric(df.get("radiotap.channel.freq"), errors="coerce")
    if "wlan_radio.frequency" in df.columns:
        freq = freq.fillna(pd.to_numeric(df["wlan_radio.frequency"], errors="coerce"))
    df["frequency_mhz"] = freq  # leave NaN if not available

    # --- normalize SSID output from tshark (hex vs text) ---
    normalize_ssid_column(df)

    # --- force SSID to pandas string dtype ---
    if "wlan.ssid" in df.columns:
        df["wlan.ssid"] = df["wlan.ssid"].astype("string")
    if "wlan.vs.aruba.ap_name" in df.columns:
        df["wlan.vs.aruba.ap_name"] = df["wlan.vs.aruba.ap_name"].astype("string")

    # Convert QBSS metrics to numeric (int)
    for col in ["wlan.qbss.scount", "wlan.qbss.cu", "wlan.qbss.adc"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # Convert radio frequency/channel evidence to nullable integers.
    for col in [
        "aruba_erm.channel",
        "radiotap.channel.freq",
        "wlan_radio.frequency",
        "frequency_mhz",
        "wlan.ds.current_channel",
        "wlan.ext_tag.he_operation.6ghz.primary_channel",
        "wlan_radio.channel",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # Convert QBSS metrics to numeric
    for col in ["wlan.qbss.scount", "wlan.qbss.cu", "wlan.qbss.adc"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return CaptureExtractionResult(
        observations=df,
        profile=profile,
        extraction_plan=extraction_plan,
        tshark_version=tshark_version,
    )


def process_pcapng(
    file_path,
    output_dir,
    erm: bool | None = None,
    source_rank=0,
    erm_type: str | None = None,
):
    """Preserve the legacy one-capture Parquet conversion entry point."""

    file_path = os.fspath(file_path)
    output_dir = os.fspath(output_dir)
    output_file = os.path.join(
        output_dir,
        f"{os.path.basename(file_path)}.parquet",
    )
    os.makedirs(output_dir, exist_ok=True)

    print(f"Processing {file_path}...")
    extraction = extract_capture_beacons(
        file_path,
        erm=erm,
        source_rank=source_rank,
        erm_type=erm_type,
    )
    extraction.observations.to_parquet(output_file, engine="pyarrow")
    print(f"Finished processing {file_path}")
    return output_file


def discover_pcapng_files(pcapng_dir):
    pcapng_dir = os.fspath(pcapng_dir)
    pcapng_files = [
        os.path.join(pcapng_dir, f)
        for f in os.listdir(pcapng_dir)
        if os.path.splitext(f)[1].lower() in CAPTURE_FILE_EXTENSIONS
    ]
    for file in pcapng_files:
        validate_capture_file(file, warn_on_legacy_extension=False)

    pcapng_files.sort()
    return pcapng_files


def process_all_pcapngs(
    pcapng_dir,
    output_dir,
    num_processes,
    erm: bool | None = None,
    erm_type: str | None = None,
):
    os.makedirs(output_dir, exist_ok=True)
    pcapng_files = discover_pcapng_files(pcapng_dir)
    ranked_files = rank_capture_sources(pcapng_files)
    print(f"Found {len(pcapng_files)} capture files. Starting processing...")

    with ProcessPoolExecutor(max_workers=num_processes) as executor:
        futures = [
            executor.submit(
                process_pcapng,
                file,
                output_dir,
                erm,
                source_rank,
                erm_type,
            )
            for file, source_rank in ranked_files
        ]
        for fut in futures:
            fut.result()
    print("All files processed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process pcapng/pcap files and save as Parquet."
    )
    parser.add_argument("-i", "--pcapng_dir", type=str, default=CAPTURES_FOLDER)
    parser.add_argument("-o", "--output_dir", type=str, default=PROCESSED_DATA_FOLDER)
    parser.add_argument("-n", "--num_processes", type=int, default=THREAD_COUNT)
    parser.add_argument(
        "--no-erm",
        action="store_true",
        help=(
            "Convert native/non-Aruba captures without Aruba ERM fields. "
            "The current analysis pipeline still requires Aruba-shaped grouped data."
        ),
    )
    parser.add_argument(
        "--erm-type",
        choices=SUPPORTED_ARUBA_ERM_TYPES,
        help=(
            "Explicitly decode every input as this Aruba ERM type. "
            "The default detects each file from its packet content."
        ),
    )
    args = parser.parse_args()

    if args.no_erm and args.erm_type:
        parser.error("--no-erm and --erm-type cannot be used together")
    explicit_erm = True if args.erm_type else (False if args.no_erm else None)
    process_all_pcapngs(
        args.pcapng_dir,
        args.output_dir,
        args.num_processes,
        erm=explicit_erm,
        erm_type=args.erm_type,
    )

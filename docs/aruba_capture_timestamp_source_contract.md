# Aruba ERM capture timestamp source contract

- Status: Accepted
- Decision: Accepted exactly within the source and tool boundary below
- Date decided: 2026-08-06
- Owner/reviewer: Project owner
- Contract name: `beacon.source-parser/aruba-erm-time/raw-seconds-microseconds`
- Contract version: `1`
- Evidence date: 2026-08-05
- Roadmap item:
  [PR-0020 capture readiness](post_refactor_roadmap.md)
- Current implementation: active in `beacon_analysis.py run` through the
  Aruba adapter and timestamp parser
- Owning decision: [ADR-0001](decisions/0001-canonical-internal-timestamp-representation.md)

> Current status (2026-10-02): the parser contract remains active, while the
> later adapter and fallback behavior is governed by ADR-0008. Sections below
> that discuss preserving a former Parquet/output shape describe the original
> implementation constraint and are not a second supported workflow.

## Scope and approval state

This document accepts the source-specific lexical contract for the existing
Aruba ERM type-0 capture path. On 2026-08-06, the project owner accepted the
contract ID, version, source authority, grammar, byte layout, precision,
comparison roles, failure policy, and exact support boundary recorded here.
That acceptance authorizes only the capture-side PR-0020 slice described in
this document.

The contract applies only to:

- pcapng captures with Ethernet encapsulation;
- Aruba ERM traffic decoded explicitly from UDP port `5555` as
  `aruba_erm_type0`;
- the built-in Aruba dissector in TShark/Wireshark `4.6.6`; and
- the exact raw `@aruba_erm.time` representation and validation rules below.

It does not establish native 802.11, `frame.time_epoch`, another Aruba ERM
type, another vendor, another TShark version, or a general timestamp-string
contract.

## Retained original-input evidence

The three intentionally untracked T4 captures remain in
`Data/captures_test`. They are not added to Git. All were written by Dumpcap
`4.6.0` on Windows 11, contain one Ethernet interface captured with
`udp port 5555`, and declare microsecond timestamp precision.

| Retained capture | Capture packets and decoded beacon rows | First relevant Aruba display value | Last relevant Aruba display value | Duplicate Aruba rows |
|---|---:|---|---|---:|
| `Beacon_Capture_20251110090000_00012.pcapng` | 1,070,997 | `2025-11-10T09:00:00.212540000-0700` | `2025-11-10T10:00:00.228386000-0700` | 209 |
| `Beacon_Capture_20251110100000_00013.pcapng` | 1,021,926 | `2025-11-10T10:00:00.248122000-0700` | `2025-11-10T11:00:00.058248000-0700` | 181 |
| `Beacon_Capture_20251110110000_00014.pcapng` | 902,021 | `2025-11-10T11:00:00.077444000-0700` | `2025-11-10T12:00:00.155767000-0700` | 69 |

The beacon display filter selected every captured row. On all 2,994,944 rows,
`frame.number`, `frame.time_epoch`, `aruba_erm.time`, `wlan.ta`, and
`wlan.bssid` were populated. No address value is published by this review.

The matching known-good processed Parquets remain in
`Data/processed_data_test/original`. Their row counts match the captures, and
their `aruba_erm.time` index is Arrow `string`, not a parsed timestamp. The
known-good tokens and duplicate counts match the fresh TShark export.

## Tool and environment evidence

The inspection used:

- TShark/Wireshark `4.6.6` (`v4.6.6-0-g3a22c3ef473d`), 64-bit Windows build;
- the built-in Aruba dissector; TShark reported no loaded plugins;
- Windows 11 25H2, build 26200, x64;
- `en-US` process culture and TShark `LC_TYPE=English_United States.utf8`;
- decimal separator `.` and Windows `Mountain Standard Time`;
- unset `LANG`, `LC_ALL`, `LC_TIME`, and `TZ` for the default run; and
- explicit decode-as and field-output options rather than profile columns.

The full scan could read the user's TShark preferences. A restricted default-
preference run produced the same first value. The proposed adapter must still
record the TShark version and exact command, validate the field type/shape,
and fail if the declared contract is not observed; this review does not claim
independence from every possible preference.

TShark `-G fields` identifies `aruba_erm.time` as `FT_ABSOLUTE_TIME` with the
label `Packet Capture Timestamp`. The official
[Aruba ERM field reference](https://www.wireshark.org/docs/dfref/a/aruba_erm.html)
documents the field as a date-and-time value. The
[TShark manual](https://www.wireshark.org/docs/man-pages/tshark.html)
documents the `fields` output and `-E` options. The official
[Wireshark 4.4 release notes](https://www.wireshark.org/docs/relnotes/wireshark-4.4.0.html)
introduced raw byte addressing in custom TShark `-e` field expressions. It is
present in the inspected 4.6.6 build; this contract does not claim it for an
untested version.

## Exact inspection commands

The current production export is equivalent to the following option shape.
It omits a display filter and relies on the documented default
`occurrence=a`:

```powershell
tshark -r <capture> -M 100000 `
  -T fields `
  -d "udp.port==5555,aruba_erm" `
  -d "aruba_erm.type,aruba_erm_type0" `
  -E "header=y" -E "separator=," -E "quote=d" `
  -e aruba_erm.time -e wlan.ta -e wlan.bssid `
  -e wlan.vs.aruba.ap_name -e wlan.ssid `
  -e wlan.qbss.scount -e wlan.qbss.cu -e wlan.qbss.adc `
  -e radiotap.channel.freq -e wlan_radio.frequency
```

The focused displayed-field comparison used this exact requested order and
made the occurrence setting explicit:

```powershell
tshark -r <capture> -M 100000 `
  -d "udp.port==5555,aruba_erm" `
  -d "aruba_erm.type,aruba_erm_type0" `
  -Y "wlan.fc.type_subtype == 8" `
  -T fields `
  -E "header=y" -E "separator=," -E "quote=d" `
  -E "occurrence=a" `
  -e frame.number -e frame.time_epoch -e aruba_erm.time `
  -e wlan.ta -e wlan.bssid
```

The raw-value comparison replaced the last three address/epoch fields with:

```powershell
-e frame.number -e aruba_erm.time -e "@aruba_erm.time"
```

CSV fields are double-quoted by `quote=d`; quotes are CSV framing and are not
part of the decoded source token. The separator is comma, the header is
present, all occurrences are requested, and the default aggregator is comma.

## Observed displayed grammar

After CSV decoding, every retained `aruba_erm.time` display value matched:

```text
YYYY-MM-DDTHH:MM:SS.fffffffff+HHMM
YYYY-MM-DDTHH:MM:SS.fffffffff-HHMM
```

The exact retained regular expression is:

```text
^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{9}[+-]\d{4}$
```

Observed properties:

- separator `T`, not a space;
- exactly nine fractional digits;
- exactly four numeric offset digits with no colon;
- no timezone abbreviation;
- no leading or trailing whitespace;
- one format and one `-0700` offset across the retained default exports;
- every fractional value ended in `000`, proving microsecond source
  precision despite the nine-digit display width; and
- outer double quotes came only from the requested CSV output mode.

Changing `LANG`/`LC_*` among the inspected `C`, German, and French settings
did not change the first token. Changing the TShark process `TZ` did:

| Process `TZ` | Same decoded Aruba instant rendered as |
|---|---|
| default / `MST7MDT` | `2025-11-10T09:00:00.212540000-0700` |
| `UTC0` | `2025-11-10T16:00:00.212540000+0000` |
| `EST5EDT` | `2025-11-10T11:00:00.212540000-0500` |
| `PST8PDT` | `2025-11-10T08:00:00.212540000-0800` |

The `-t ad`, `-t ud`, and `-t e` options did not change this field's rendered
value. Therefore the displayed numeric offset is sufficient and authoritative
for the instant represented by that token, but it is TShark rendering
evidence, not evidence of the Aruba AP's named timezone or physical location.
It must not be relabeled with `PLOT_TIMEZONE` or another configured zone.

The current pandas format
`%b %e, %Y %H:%M:%S.%f %Z` does not match the observed grammar. The retained
Parquets prove that current processing leaves the index as strings; it is not
being parsed by the declared format or by a successful fallback inference.

## Stable raw grammar

TShark `4.6.6` also permits the raw field expression
`@aruba_erm.time`. Every retained value matched exactly eight colon-separated,
lower-case hexadecimal bytes:

```text
^(?:[0-9a-f]{2}:){7}[0-9a-f]{2}$
```

For every retained row:

1. bytes 0-3, interpreted as an unsigned big-endian integer, equal Unix
   seconds for the displayed Aruba instant;
2. bytes 4-7, interpreted as an unsigned big-endian integer, equal the
   microsecond fraction in the range 0 through 999,999; and
3. `seconds * 1_000_000_000 + microseconds * 1_000` exactly equals the
   displayed offset-bearing instant.

There were zero absent values, grammar failures, or raw/display mismatches
across 2,994,944 rows. The raw eight bytes remained identical under all
tested `TZ` settings while the display string changed.

## Aruba and outer-frame timestamp comparison

`frame.time_epoch` was populated on every retained row and displayed exactly
nine fractional digits, with the final three always zero. The pcapng
interface declares microsecond precision, so both clocks have effective
microsecond precision in this evidence.

The signed difference below is `Aruba instant - frame.time_epoch`:

| Capture hour | Minimum | Median | 95th percentile | Maximum | Exact matches |
|---|---:|---:|---:|---:|---:|
| 09:00 | 456,486 us | 525,846 us | 550,775 us | 964,201 us | 0 |
| 10:00 | 368,886 us | 564,144 us | 593,491 us | 965,276 us | 0 |
| 11:00 | 520,514 us | 602,181 us | 627,048 us | 905,664 us | 0 |

The Aruba value was numerically later on every row. The evidence does not
establish whether the difference is transport delay, clock offset, or both.
It does establish that the two fields do not identify the same instant and
that `frame.time_epoch` cannot be an equivalence oracle, conflict fallback,
or replacement for the Aruba observation time.

No fatal agreement threshold is selected. A future adapter may retain the
signed comparison as a diagnostic measurement, but it must not reinterpret
the difference without a separate reviewed decision and broader evidence.

## Frame-number evidence

The current `-M 100000` option performs a TShark session auto-reset. The
exported `frame.number` resets to `1` after each 100,000-row session:

```text
99998, 99999, 100000, 1, 2, 3
```

Each retained capture therefore had only 100,000 distinct reported frame
numbers. One reported number occurred up to 11, 11, and 10 times in the three
captures respectively. Capture identity plus `frame.number` is not sufficient
to distinguish observations under the active command.

Duplicate Aruba timestamps also occurred on distinct reported frame numbers:
209, 181, and 69 rows respectively. Timestamp is therefore not a row
identifier either.

The next timestamp slice may retain reported frame number and export position
transiently for diagnostics and order preservation, but neither may be labeled
a stable pcap-frame identity. A missing frame number reduces provenance only;
it does not change the valid Aruba instant. Durable capture/source-record
identity remains an ADR-0002 physical-provenance decision.

## Parser options

| Option | Assessment |
|---|---|
| A - exact observed Aruba parser | Recommended using the raw `@aruba_erm.time` expression. It is source-specific, exact, locale/timezone invariant in the retained evidence, microsecond-precise, deterministic, and rejects all other shapes. The displayed token remains legacy/audit evidence. |
| B - small enumerated grammar set | Safe only if each additional observed form receives a separate parser name/version. No second required form is established, so accepting more now would broaden support without evidence. |
| C - pandas/dateutil inference | Rejected. It is broader than the observed contract, can vary by dependency/version, hides lexical diagnostics, and would repeat the current format-dependent behavior. |
| D - prefer `frame.time_epoch` | Rejected. It differs on every retained row by 0.369-0.965 seconds, represents the outer capture clock, and would change observation instants. Native downstream support remains unapproved. |

## Accepted source contract

The accepted parser is
`beacon.source-parser/aruba-erm-time/raw-seconds-microseconds` version `1`.

It must:

1. accept only the raw field expression `@aruba_erm.time` from the declared
   TShark/Aruba type-0 command;
2. accept only eight lower-case colon-separated hexadecimal bytes;
3. interpret bytes 0-3 as unsigned big-endian Unix seconds and bytes 4-7 as
   unsigned big-endian microseconds;
4. reject a microsecond word above 999,999;
5. construct one UTC-aware pandas `Timestamp` at exact microsecond precision,
   represented in canonical nanoseconds with the final three digits zero;
6. construct `TimestampInput` with the raw token as both `value` and
   `raw_source_token`, and supply the named, versioned parser through
   `SourceParserContract`;
7. call `normalize_timestamp()` or `normalize_timestamps()` and require a
   `valid` canonical UTC result before the row may enter timestamp-dependent
   processing;
8. retain the displayed `aruba_erm.time` token separately for legacy output
   and validate that it identifies the same instant when present;
9. retain reported `frame.number`, `frame.time_epoch`, TShark version, decode
   options, and capture reference as provenance when available, without
   claiming a stable source-record identity;
10. preserve input row order, duplicates, and one normalization result per
    input; and
11. perform no sorting, filtering, dropping, deduplication, timezone
    attachment, `NaT` coercion, or fallback.

The raw epoch value is already an unambiguous instant. No machine, dataset,
schedule, `TIMEZONE`, or `PLOT_TIMEZONE` evidence is supplied to
`timestamp_normalization.py`, and the TShark-rendered offset is not recorded as
an Aruba named-zone claim.

## Failure and diagnostic contract

| Condition | Adapter diagnostic | Timestamp handling |
|---|---|---|
| Supported raw token | `aruba_timestamp_valid` | Normalize to UTC and retain source precision/token. |
| Raw field absent | `aruba_timestamp_absent` | Fatal for the row/capture slice; do not create a valid canonical instant. |
| Raw lexical shape malformed | `aruba_timestamp_malformed` | Fatal structured rejection; no inference or `NaT`. |
| Microsecond word above 999,999 | `aruba_timestamp_fraction_out_of_range` | Fatal structured rejection. |
| Seconds cannot be represented canonically | `aruba_timestamp_seconds_out_of_range` | Fatal structured rejection. |
| Displayed Aruba token is malformed | `aruba_timestamp_render_malformed` | Fatal tool-contract violation; raw remains authoritative but legacy output cannot be validated. |
| Displayed Aruba token identifies another instant | `aruba_timestamp_render_conflict` | Fatal tool-contract violation; raw/display conflict is not repaired. |
| Unsupported TShark version, field type, raw width, or decode contract | `unsupported_aruba_timestamp_contract` | Stop before writing a supported output. |
| `frame.time_epoch` absent or malformed | `aruba_frame_epoch_unavailable` | Comparison unavailable; no fallback and no effect on a valid Aruba instant. |
| Aruba/epoch difference observed | `aruba_frame_epoch_delta_observed` | Record signed delta only; not fatal under this contract. |
| `frame.number` absent | `aruba_frame_number_unavailable` | Provenance warning only; timestamp normalization may proceed. |
| `frame.number` reused after `-M` reset | `aruba_frame_number_reused` | Preserve reported value and row order; do not call it identity. |
| Unexpected parser/normalizer invariant fails | `aruba_timestamp_internal_error` | Fatal structured rejection; do not write supported output. |

Adapter-specific diagnostics remain adjacent to the source parser. The shared
normalizer retains its existing stable statuses and API.

## Persistence and compatibility

For the first capture integration slice, raw token, parser identity, reported
frame number, epoch comparison, and normalization result may remain transient.
Persisting them would require the unresolved physical provenance/schema
decision.

The existing displayed `aruba_erm.time` index, Arrow type, row values, order,
duplicates, filenames, and known-good Parquet fingerprints must remain
unchanged. The raw field expression and canonical UTC value may be used
internally and then omitted from the legacy artifact. No grouping cleanup,
analysis filtering, metric, native path, or display behavior changes in this
slice.

## Implementation record — 2026-08-06

`aruba_timestamp_parsing.py` implements the immutable, source-specific parser,
evidence, result, and comparison-diagnostic types. Each raw token is supplied
as both `TimestampInput.value` and `TimestampInput.raw_source_token` with the
accepted `SourceParserContract`; `normalize_timestamps()` produces the
canonical UTC result. Exact component parsing is used for the display token,
and `Decimal` arithmetic is used for the diagnostic frame-epoch difference.

`process_pcapng.py` requires the evidenced TShark 4.6.6 version for Aruba
conversion, requests raw/display/frame evidence, validates every row before
writing, and raises `ArubaTimestampConversionError` with safe export-row and
reported-frame locators on fatal source-contract failures. It drops raw,
frame-epoch, and frame-number evidence before Parquet persistence. The legacy
display-token index, physical Arrow fields, row count/order/duplicates,
non-timestamp values, filename, and downstream behavior remain unchanged.

Focused coverage is implemented in `tests/test_aruba_timestamp_parsing.py`
and `tests/test_process_pcapng_windows.py`. No committed binary fixture or
checkpoint was added or changed.

## ADR assessment and acceptance record

No new ADR is required for this exact contract. It applies ADR-0001's accepted
rule that an unambiguous source value may use an explicitly named/versioned
parser and does not introduce an ambiguity, fallback, interval, metric,
schema, or public-support policy.

The project-owner acceptance on 2026-08-06 satisfies the focused decision gate
without changing PR-0020's overall `In progress` status. Implementation and
validation remain separately recorded so accepting the contract alone cannot
imply that runtime work passed.

If a future proposal supports multiple unproven grammars or TShark versions,
adds another vendor-specific timestamp ahead of frame time, or claims a stable
source-frame identity, it requires a separate high-level owner decision.
Revised
[ADR-0008](decisions/0008-aruba-capture-timestamp-authority-and-fallback.md)
keeps valid raw Aruba time first and uses valid `frame.time_epoch` when Aruba
time is unavailable or unusable. It explicitly excludes clock-domain,
discontinuity, and correction machinery.

## Implemented tests

Focused tests must cover:

- the exact lower-case eight-byte grammar and rejection of uppercase,
  missing separators, wrong width, whitespace, quotes passed through as token
  content, and trailing characters;
- Unix seconds and exact microsecond-to-nanosecond conversion;
- zero and 999,999 microseconds plus out-of-range rejection;
- absent/malformed/out-of-range values and stable adapter diagnostics;
- rejection of the displayed ISO form by the raw parser;
- exact audit-display grammar and explicit offset conversion for the observed
  `+0000`, `-0500`, `-0700`, and `-0800` renderings without making that
  rendered form canonical source authority;
- raw/display agreement and fatal conflict;
- deterministic reruns and independence from process `TZ`/locale;
- raw-token and parser name/version retention;
- reported frame-number retention, absence, and reuse;
- duplicate instants on distinct rows;
- Aruba/epoch delta recording, absence, malformed comparison, and proof that
  epoch never becomes a fallback;
- one result per input, unchanged row order, duplicates, and non-mutation;
- unchanged legacy displayed token, Parquet schema, filenames, and rows; and
- rejection of unapproved TShark/field/source contracts.

The commands and results are recorded in the
[capture integration slice](post_refactor_roadmap.md#pr-0020-capture-readiness).

## Evidence limits and deferred work

- Only TShark/Wireshark `4.6.6`, Dumpcap `4.6.0`, Aruba ERM type 0, UDP port
  `5555`, Ethernet pcapng, and the three retained November 2025 captures are
  covered.
- No capture spans a timezone-offset transition, and no other Aruba
  deployment or dissector version is represented.
- The known-good processed outputs did not record the TShark version that
  originally generated them, although their tokens match the fresh `4.6.6`
  export exactly.
- The cause and acceptable range of the Aruba/outer-frame clock difference
  remain unknown; no warning/failure threshold is approved.
- `frame.number` is not stable under the active session-reset option, and no
  durable capture/source-record identifier is selected.
- Persistent provenance, manifest layout, canonical artifact placement,
  cleanup migration, native support, and interval/display changes remain
  deferred.

## Addendum — simplified use by ADR-0008 and ADR-0009

Revised ADR-0008 and ADR-0009 use this contract's canonical normalized raw
`@aruba_erm.time` as preferred observation time `O`. Valid normalized
`frame.time_epoch` is used when Aruba time is unavailable or unusable. `O`
admits the beacon to its scheduled class and anchors any reliable per-BSSID/
session TSF mapping.

The table above remains the strict type-0 Aruba source-parser contract: asking
that parser to accept a missing raw Aruba value still fails. At the enclosing
capture-conversion boundary, PR-0037's 2026-09-14 type-6 Aruba evidence now
implements the later ADR-0008 precedence explicitly. Rows with a valid raw
Aruba value are parsed by this strict contract; a row without that value uses
valid `frame.time_epoch`; a row with neither source fails with
`source_timestamp_unavailable`. This does not pretend that frame time is an
Aruba timestamp or extend this raw parser contract to ERM type 6.

The owner-supplied type-6 radiotap capture contains no usable raw or rendered
`aruba_erm.time`, while its frame epoch, frequency, DS channel, radio-header
channel, and HE 6 GHz primary channel are available. The two additional GUI
captures establish that types 0 and 3 retain raw/rendered Aruba time; type 3
also exposes its ERM radio header. Capture conversion now detects types 0, 3,
and 6 per file from decoded beacon/BSSID evidence and retains the chosen
profile. Type 3 is preferred for current CU analysis; see
[`capture_input_profiles.md`](capture_input_profiles.md). This broadens the
capture boundary without changing this document's strict raw Aruba-time parser
grammar.

`wlan.fixed.timestamp` is evaluated separately for each BSSID/class session
and may control bin placement through `B=min(O-F)` and `T=B+F`, as well as
assist sequence ordering, duplicate recognition, and skipped-interval
diagnostics. Missing, frozen, reversing, insufficient, or grossly inconsistent
TSF uses `O` for the complete BSSID/session without discarding otherwise valid
beacons.

## Addendum — retained TSF/duplicate characterization (2026-09-01/02)

The controlled TShark `4.6.6`
[retained-capture characterization](adr0009_retained_capture_characterization.md)
reproduced 2,455,176 beacon rows. It found 22 cross-endpoint duplicate keys
with no QBSS payload conflict and found frozen `F=56` in 99.2445% of the
production LSB-2102 AP rows. Those production rows use Aruba observation time
normally; they do not require a class-level timing fallback.

The narrow per-BSSID/session `O-F` lower envelope was accepted for TSF-derived
bin placement on 2026-09-09. Shared BSSID timing pools, clock-change screens,
drift analysis, segmentation, and clock normalization remain retired. They are
not runtime-profile or release gates.

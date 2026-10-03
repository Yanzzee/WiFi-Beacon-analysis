"""Shared 2.4/5/6 GHz band and primary-channel evidence classification."""

from __future__ import annotations

import pandas as pd


def _column_or_index(frame: pd.DataFrame, *names: str) -> pd.Series:
    for name in names:
        if name in frame.columns:
            return frame[name]
        if frame.index.name == name:
            return pd.Series(frame.index, index=frame.index, name=name)
    return pd.Series(pd.NA, index=frame.index)


def _nullable_integer(values: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    return numeric.where(numeric >= 0).astype("Int64")


def _first_integer(*values: pd.Series) -> pd.Series:
    result = pd.Series(pd.NA, index=values[0].index, dtype="Int64")
    for value in values:
        result = result.fillna(_nullable_integer(value))
    return result


def classify_frequency_band(frequency_mhz: pd.Series) -> pd.Series:
    """Return nullable ``2.4``, ``5``, or ``6`` labels from MHz evidence."""

    numeric = _nullable_integer(frequency_mhz)
    band = pd.Series(pd.NA, index=numeric.index, dtype="string")
    band.loc[numeric.between(2400, 2500)] = "2.4"
    band.loc[numeric.between(4900, 5924)] = "5"
    band.loc[numeric.between(5925, 7125)] = "6"
    return band


def attach_band_channel_evidence(result: pd.DataFrame) -> pd.DataFrame:
    """Attach shared band/channel columns to an adapter-normalized frame.

    The caller supplies a private working frame; this function updates and
    returns that frame to avoid another full-table copy.
    """

    supplied_frequency = _nullable_integer(
        _column_or_index(result, "frequency_mhz")
    )
    radiotap_frequency = _nullable_integer(
        _column_or_index(result, "radiotap.channel.freq")
    )
    radio_frequency = _nullable_integer(
        _column_or_index(result, "wlan_radio.frequency")
    )
    result["frequency_mhz"] = _first_integer(
        supplied_frequency,
        radiotap_frequency,
        radio_frequency,
    )

    ds_channel = _nullable_integer(
        _column_or_index(result, "wlan.ds.current_channel")
    )
    he_6ghz_channel = _nullable_integer(
        _column_or_index(
            result,
            "wlan.ext_tag.he_operation.6ghz.primary_channel",
        )
    )
    radio_channel = _nullable_integer(
        _column_or_index(result, "wlan_radio.channel")
    )

    band = classify_frequency_band(result["frequency_mhz"])
    band_method = pd.Series(pd.NA, index=result.index, dtype="string")
    band_method.loc[band.notna()] = "frequency_mhz"
    no_band = band.isna()
    band.loc[no_band & he_6ghz_channel.notna()] = "6"
    band_method.loc[no_band & he_6ghz_channel.notna()] = "he_6ghz_operation"
    no_band = band.isna()
    band.loc[no_band & ds_channel.between(1, 14)] = "2.4"
    band.loc[no_band & ds_channel.gt(14)] = "5"
    band_method.loc[no_band & ds_channel.notna()] = "ds_parameter_set"

    channel = pd.Series(pd.NA, index=result.index, dtype="Int64")
    channel_method = pd.Series(pd.NA, index=result.index, dtype="string")
    use_he = band.eq("6") & he_6ghz_channel.notna()
    channel.loc[use_he] = he_6ghz_channel.loc[use_he]
    channel_method.loc[use_he] = "he_6ghz_operation"
    use_ds = channel.isna() & ds_channel.notna()
    channel.loc[use_ds] = ds_channel.loc[use_ds]
    channel_method.loc[use_ds] = "ds_parameter_set"
    use_radio = channel.isna() & radio_channel.notna()
    channel.loc[use_radio] = radio_channel.loc[use_radio]
    channel_method.loc[use_radio] = "radio_header"

    frequency_conflict = pd.concat(
        [supplied_frequency, radiotap_frequency, radio_frequency], axis=1
    ).nunique(axis=1, dropna=True).gt(1)
    channel_conflict = pd.concat(
        [ds_channel, he_6ghz_channel, radio_channel], axis=1
    ).nunique(axis=1, dropna=True).gt(1)
    he_band_conflict = he_6ghz_channel.notna() & band.notna() & ~band.eq("6")
    conflict = frequency_conflict | channel_conflict | he_band_conflict

    status = pd.Series("complete", index=result.index, dtype="string")
    status.loc[band.isna() | channel.isna()] = "incomplete"
    status.loc[conflict] = "conflict"
    result["band"] = band
    result["channel"] = channel
    result["band_method"] = band_method
    result["channel_method"] = channel_method
    result["band_channel_status"] = status
    return result

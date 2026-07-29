"""Room-level station-count enrichment for class beacons."""

import numpy as np
from tqdm import tqdm


def enrich_room_station_counts(
    all_class_beacons_df,
    sample_aps_df,
):
    hostname_to_location = sample_aps_df.set_index("Hostname")[
        "Building-Room"
    ].to_dict()
    location_to_hostnames = (
        sample_aps_df.groupby("Building-Room")["Hostname"]
        .apply(list)
        .to_dict()
    )
    print("Calculating station count for each location")
    print("Step 1 done")

    df = all_class_beacons_df.sort_values(
        "aruba_erm.time"
    ).reset_index(drop=True)
    df["room_scount"] = 0
    df["location"] = df["wlan.vs.aruba.ap_name"].map(
        hostname_to_location
    )

    print("Step 2 done")

    host_time_arrays = {}
    host_ta_arrays = {}
    host_scount_arrays = {}

    for hostname in tqdm(
        df["wlan.vs.aruba.ap_name"].unique(),
        desc="Preprocessing APs for faster lookup",
    ):
        host_df = df[
            df["wlan.vs.aruba.ap_name"] == hostname
        ].copy()
        host_df = host_df.sort_values(
            "aruba_erm.time"
        ).reset_index(drop=True)

        times = host_df["aruba_erm.time"].astype("int64").values
        tas = host_df["wlan.ta"].values
        scounts = host_df["wlan.qbss.scount"].values

        assert len(times) == len(tas) == len(scounts), (
            f"Array mismatch for AP {hostname}"
        )

        host_time_arrays[hostname] = times
        host_ta_arrays[hostname] = tas
        host_scount_arrays[hostname] = scounts

    print("Step 3 done")

    results = np.zeros(len(df), dtype=int)
    five_seconds_ns = (
        np.timedelta64(5, "s")
        .astype("timedelta64[ns]")
        .astype("int64")
    )

    for i in tqdm(
        range(len(df)),
        desc="Computing room_scount for each beacon",
    ):
        row = df.iloc[i]
        current_time = row["aruba_erm.time"]
        current_hostname = row["wlan.vs.aruba.ap_name"]
        current_ta = row["wlan.ta"]
        location = hostname_to_location.get(current_hostname)

        if not location:
            continue

        room_hostnames = location_to_hostnames[location]

        seen_ta = {current_ta}
        room_scount = row["wlan.qbss.scount"]
        current_time_ns = current_time.value

        ta_to_best_idx = {}

        for other_hostname in room_hostnames:
            if other_hostname not in host_time_arrays:
                continue

            times = host_time_arrays[other_hostname]
            scounts = host_scount_arrays[other_hostname]
            tas = host_ta_arrays[other_hostname]

            left_bound = current_time_ns - five_seconds_ns
            right_bound = current_time_ns + five_seconds_ns

            left_idx = np.searchsorted(times, left_bound, side="left")
            right_idx = np.searchsorted(
                times,
                right_bound,
                side="right",
            )

            matching_indices = range(left_idx, right_idx)

            for j in matching_indices:
                ta = tas[j]
                time_diff = np.abs(times[j] - current_time_ns)

                if ta not in ta_to_best_idx or time_diff < np.abs(
                    host_time_arrays[ta_to_best_idx[ta][0]][
                        ta_to_best_idx[ta][1]
                    ]
                    - current_time_ns
                ):
                    ta_to_best_idx[ta] = (other_hostname, j)

        for ta, (host, j) in ta_to_best_idx.items():
            if ta not in seen_ta:
                seen_ta.add(ta)
                room_scount += host_scount_arrays[host][j]

        results[i] = int(room_scount)

    df["room_scount"] = results
    return df

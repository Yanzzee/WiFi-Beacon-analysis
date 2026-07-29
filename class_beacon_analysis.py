"""Data preparation and reporting for all-class-beacon analysis."""

import numpy as np
from scipy.stats import linregress

from analysis_statistics import compute_pdf_cdf


def filter_configured_ssids(all_class_beacons_df, configured_ssids):
    print(all_class_beacons_df["wlan.ssid"].unique())
    filtered_df = all_class_beacons_df[
        all_class_beacons_df["wlan.ssid"].isin(configured_ssids)
    ]
    print(filtered_df["wlan.ssid"].unique())
    return filtered_df


def report_class_beacon_aggregates(
    all_class_beacons_df,
    log_and_print,
):
    total_data_points = len(all_class_beacons_df)
    greater_than_191_count = (
        all_class_beacons_df["wlan.qbss.cu"] > 191
    ).sum()
    percentage_greater_than_191 = (
        greater_than_191_count / total_data_points
    ) * 100
    log_and_print(
        "percent of beacons with very high utilization (> 75% cu): "
        f"{percentage_greater_than_191:.4f} %"
    )

    greater_than_127_count = (
        all_class_beacons_df["wlan.qbss.cu"] > 127
    ).sum()
    percentage_greater_than_127 = (
        greater_than_127_count / total_data_points
    ) * 100
    log_and_print(
        "percent of beacons with high utilization (> 50% cu): "
        f"{percentage_greater_than_127:.4f} %"
    )
    log_and_print(
        "median number of associated devices per radio: "
        f"{all_class_beacons_df['total_scount'].median()}"
    )
    log_and_print(
        "average number of associated devices per radio: "
        f"{all_class_beacons_df['total_scount'].mean():.2f}"
    )
    log_and_print(
        "median channel utilization qbss: "
        f"{all_class_beacons_df['wlan.qbss.cu'].median()}"
    )
    log_and_print(
        "average channel utilization qbss: "
        f"{all_class_beacons_df['wlan.qbss.cu'].mean():.2f}"
    )


def prepare_channel_utilization_histogram(all_class_beacons_df):
    cu_percent = all_class_beacons_df["wlan.qbss.cu"] * (100 / 255)
    bins = np.linspace(0, 100, 256)
    return cu_percent, bins


def prepare_channel_utilization_distribution(all_class_beacons_df):
    cu_data = (
        all_class_beacons_df["wlan.qbss.cu"].dropna()
        * (100 / 255)
    )
    bin_centers, pdf, cdf = compute_pdf_cdf(cu_data, bins=256)
    return cu_data, bin_centers, pdf, cdf


def report_channel_utilization_cdf(cu_data, log_and_print):
    cdf_at_50 = (cu_data <= 50).mean()
    log_and_print(
        f"CDF at wlan.qbss.cu = 50%: {cdf_at_50:.4f}"
    )
    log_and_print(
        f"{(cdf_at_50 * 100):.2f} % of beacons during class show "
        "channel utilization under 50%"
    )

    cdf_at_75 = (cu_data <= 75).mean()
    log_and_print(
        f"CDF at wlan.qbss.cu = 75%: {cdf_at_75:.4f}"
    )
    log_and_print(
        f"{(cdf_at_75 * 100):.2f} % of beacons during class show "
        "channel utilization under 75%"
    )
    return cdf_at_50, cdf_at_75


def calculate_station_count_bins(all_class_beacons_df):
    return max(all_class_beacons_df["total_scount"]) + 1


def report_class_beacon_correlations(
    all_class_beacons_df,
    log_and_print,
):
    correlation = all_class_beacons_df["total_scount"].corr(
        all_class_beacons_df["wlan.qbss.cu"]
    )
    log_and_print(
        "The correlation between total_scount and wlan.qbss.cu is: "
        f"{correlation:.4f}"
    )

    correlation_adc = all_class_beacons_df["wlan.qbss.adc"].corr(
        all_class_beacons_df["wlan.qbss.cu"]
    )
    log_and_print(
        "The correlation between wlan.qbss.adc and wlan.qbss.cu is: "
        f"{correlation_adc:.4f}"
    )

    correlation_adc_sc = all_class_beacons_df["total_scount"].corr(
        all_class_beacons_df["wlan.qbss.adc"]
    )
    log_and_print(
        "The correlation between total_scount and wlan.qbss.adc is: "
        f"{correlation_adc_sc:.4f}"
    )
    return correlation, correlation_adc, correlation_adc_sc


def report_class_beacon_linear_regression(
    all_class_beacons_df,
    log_and_print,
):
    x = all_class_beacons_df["total_scount"]
    y = all_class_beacons_df["wlan.qbss.cu"]
    slope, intercept, r_value, p_value, std_err = linregress(x, y)
    log_and_print(
        "Linear regression values of total_scount to wlan.qbss.cu:"
    )
    log_and_print(f"Slope: {slope:.3f}")
    log_and_print(f"Intercept: {intercept:.3f}")
    log_and_print(f"R-squared: {r_value**2:.3f}")
    log_and_print(f"P-value: {p_value:.3g}")
    return slope, intercept, r_value, p_value, std_err

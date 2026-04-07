"""
Week 3 — CloudWatch Row Count Anomaly Monitor
Runs as a Lambda (or standalone script) after each pipeline run.
Compares today's row count to 7-day rolling average.
Triggers Slack alert if anomaly detected.
"""

import os
import json
import boto3
import logging
from datetime import datetime, timedelta

logger = logging.getLogger()
logger.setLevel(logging.INFO)

SLACK_WEBHOOK = os.environ.get("SLACK_WEBHOOK_URL", "")
NAMESPACE = "ECommPipeline"
ANOMALY_THRESHOLD_PCT = 20  # alert if today is ±20% from rolling avg


def get_metric_stats(cw, metric_name, days=7):
    """Fetch last N days of a CloudWatch metric."""
    end = datetime.utcnow()
    start = end - timedelta(days=days)
    response = cw.get_metric_statistics(
        Namespace=NAMESPACE,
        MetricName=metric_name,
        Dimensions=[{"Name": "env", "Value": "prod"}],
        StartTime=start,
        EndTime=end,
        Period=86400,  # daily
        Statistics=["Sum"],
    )
    datapoints = sorted(response["Datapoints"], key=lambda x: x["Timestamp"])
    return [dp["Sum"] for dp in datapoints]


def detect_anomaly(metric_name, cw):
    history = get_metric_stats(cw, metric_name, days=8)
    if len(history) < 2:
        logger.info(f"{metric_name}: not enough history, skipping")
        return None

    today = history[-1]
    baseline = history[:-1]
    rolling_avg = sum(baseline) / len(baseline)

    if rolling_avg == 0:
        return None

    deviation_pct = abs(today - rolling_avg) / rolling_avg * 100

    result = {
        "metric": metric_name,
        "today": int(today),
        "rolling_avg": round(rolling_avg, 0),
        "deviation_pct": round(deviation_pct, 1),
        "is_anomaly": deviation_pct > ANOMALY_THRESHOLD_PCT,
        "direction": "spike" if today > rolling_avg else "drop",
    }
    logger.info(f"Anomaly check {metric_name}: {result}")
    return result


def send_slack_alert(anomalies):
    if not SLACK_WEBHOOK or not anomalies:
        return

    import urllib.request
    lines = []
    for a in anomalies:
        emoji = ":warning:" if a["direction"] == "drop" else ":chart_with_upwards_trend:"
        lines.append(
            f"{emoji} *{a['metric']}*: today={a['today']:,} vs avg={int(a['rolling_avg']):,} "
            f"({'+' if a['direction'] == 'spike' else '-'}{a['deviation_pct']}%)"
        )

    payload = {
        "text": ":rotating_light: *Row Count Anomaly Detected — ecomm-pipeline*",
        "blocks": [{
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": ":rotating_light: *Row Count Anomaly — ecomm-pipeline*\n" + "\n".join(lines),
            }
        }]
    }

    req = urllib.request.Request(
        SLACK_WEBHOOK,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        logger.info(f"Slack response: {resp.status}")


def lambda_handler(event, context):
    """Lambda entrypoint — also callable as a script."""
    cw = boto3.client("cloudwatch", region_name="ap-south-1")

    tables = ["orders_row_count", "payments_row_count", "products_row_count"]
    anomalies = []

    for table in tables:
        result = detect_anomaly(table, cw)
        if result and result["is_anomaly"]:
            anomalies.append(result)

    if anomalies:
        logger.warning(f"Anomalies detected: {anomalies}")
        send_slack_alert(anomalies)
    else:
        logger.info("All row counts within normal range.")

    return {
        "statusCode": 200,
        "anomalies": len(anomalies),
        "details": anomalies,
    }


if __name__ == "__main__":
    result = lambda_handler({}, None)
    print(json.dumps(result, indent=2))

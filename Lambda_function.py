import json, boto3, time, decimal, os

sns = boto3.client('sns')
ddb = boto3.resource('dynamodb').Table('forest_readings')
TOPIC_ARN = os.getenv('TOPIC_ARN')

def score(t, h, smoke, flame):
    s = 0
    s += max(0, (t-38))*1.2
    s += max(0, (35-h))*2.0
    s += max(0, (smoke-300))*0.8
    s += 150 if flame else 0
    return min(1, s/400)

def lambda_handler(event, context):
    data = event if isinstance(event, dict) else {}
    t = float(data.get('temperature',0))
    h = float(data.get('humidity',0))
    smoke = int(data.get('smoke',0))
    flame = int(data.get('flame',0))
    node = data.get('nodeId','Unknown')
    ts = int(data.get('ts', time.time()*1000))

    r = score(t,h,smoke,flame)
    label = "SAFE"
    if r>0.8: label="DANGER"
    elif r>0.5: label="WARNING"

    ddb.put_item(Item={
        "nodeId":node, "ts":decimal.Decimal(ts),
        "temperature":decimal.Decimal(str(t)),
        "humidity":decimal.Decimal(str(h)),
        "smoke":decimal.Decimal(smoke),
        "flame":flame,
        "risk":decimal.Decimal(str(r)),
        "label":label
    })

    if label=="DANGER":
        msg=f"🔥 Forest Alert: Node {node} T={t}°C H={h}% Smoke={smoke} Risk={r:.2f}"
        sns.publish(TopicArn=TOPIC_ARN, Message=msg)

    return {"label":label,"risk":r}
#AWS

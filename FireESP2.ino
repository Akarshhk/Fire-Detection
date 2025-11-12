void setup() {
  // put your setup code here, to run once:

}

void loop() {
  // put your main code here, to run repeatedly:

}
/*******************************************************
 * ESP32 -> AWS IoT Core telemetry publisher
 * Sensors (optional): DHT11/22, MQ-2 (analog), LDR (analog), digital alert
 * Topics: publish to forest/telemetry/ForestNode1
 *******************************************************/

#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>

// ====== OPTIONAL DHT ======
#define USE_DHT true        // set to false if you don't have a real DHT
#if USE_DHT
  #include <DHT.h>
  #define DHT_PIN 4         // GPIO for DHT data
  #define DHT_TYPE DHT22    // DHT11 or DHT22
  DHT dht(DHT_PIN, DHT_TYPE);
#endif

// ====== SENSOR PINS (change as needed) ======
#define LED_PIN         2          // Onboard LED for blink
#define GAS_ADC_PIN     34         // MQ-2 analog (ADC1 only, 32-39)
#define LDR_ADC_PIN     35         // LDR analog
#define ALERT_DIGITAL   27         // Digital alert (flame/rain/etc). Tie HIGH/LOW

// ====== WIFI CONFIG ======
const char* WIFI_SSID = "YOUR_WIFI_SSID";
const char* WIFI_PASS = "YOUR_WIFI_PASSWORD";

// ====== AWS IOT CONFIG ======
const char* AWS_IOT_ENDPOINT = "xxxxxxxxxxxx-ats.iot.ap-south-1.amazonaws.com"; // Settings -> Device data endpoint

// Thing details
const char* MQTT_CLIENT_ID = "ForestNode1";            // Must be unique
const char* MQTT_PUB_TOPIC = "forest/telemetry/ForestNode1";

// ====== CERTIFICATES & KEYS (paste your actual PEM contents) ======
static const char rootCACert[] PROGMEM = R"EOF(
-----BEGIN CERTIFICATE-----
PASTE_AMAZON_ROOT_CA1_PEM_HERE
-----END CERTIFICATE-----
)EOF";

static const char deviceCert[] PROGMEM = R"KEY(
-----BEGIN CERTIFICATE-----
PASTE_YOUR_DEVICE_CERT_PEM_HERE
-----END CERTIFICATE-----
)KEY";

static const char privateKey[] PROGMEM = R"KEY(
-----BEGIN RSA PRIVATE KEY-----
PASTE_YOUR_PRIVATE_KEY_PEM_HERE
-----END RSA PRIVATE KEY-----
)KEY";

// ====== GLOBALS ======
WiFiClientSecure net;
PubSubClient mqtt(net);
unsigned long lastPublishMs = 0;
const unsigned long PUBLISH_INTERVAL_MS = 10 * 1000;  // 10s

// ====== HELPERS ======
void blink(uint8_t times, uint16_t onMs = 80, uint16_t offMs = 80) {
  for (uint8_t i = 0; i < times; i++) {
    digitalWrite(LED_PIN, HIGH);
    delay(onMs);
    digitalWrite(LED_PIN, LOW);
    delay(offMs);
  }
}

void connectWiFi() {
  Serial.print(F("WiFi: connecting to "));
  Serial.println(WIFI_SSID);
  WiFi.mode(WIFI_STA);
  WiFi.begin(WIFI_SSID, WIFI_PASS);

  uint8_t dots = 0;
  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
    Serial.print('.');
    if (++dots % 4 == 0) blink(1);
  }
  Serial.println();
  Serial.print(F("WiFi connected, IP: "));
  Serial.println(WiFi.localIP());
}

void connectMQTT() {
  // TLS
  net.setCACert(rootCACert);
  net.setCertificate(deviceCert);
  net.setPrivateKey(privateKey);

  mqtt.setServer(AWS_IOT_ENDPOINT, 8883);

  Serial.print(F("MQTT: connecting as "));
  Serial.println(MQTT_CLIENT_ID);

  while (!mqtt.connected()) {
    if (mqtt.connect(MQTT_CLIENT_ID)) {
      Serial.println(F("MQTT connected."));
      blink(2, 120, 120);
    } else {
      Serial.print(F("MQTT failed, state="));
      Serial.println(mqtt.state());
      blink(3, 60, 60);
      delay(1500);
    }
  }
}

float safeReadDHTTemp() {
  #if USE_DHT
    float t = dht.readTemperature(); // Celsius
    if (isnan(t)) return NAN;
    return t;
  #else
    return NAN;
  #endif
}

float safeReadDHTHum() {
  #if USE_DHT
    float h = dht.readHumidity();
    if (isnan(h)) return NAN;
    return h;
  #else
    return NAN;
  #endif
}

int readAnalogSafe(int pin) {
  // ADC1 pins only (32..39) while WiFi is on
  return analogRead(pin);
}

void publishTelemetry() {
  // Read sensors
  float temperatureC = safeReadDHTTemp();   // NAN if not available
  float humidityPct  = safeReadDHTHum();    // NAN if not available
  int gasRaw         = readAnalogSafe(GAS_ADC_PIN); // 0..4095
  int ldrRaw         = readAnalogSafe(LDR_ADC_PIN); // 0..4095
  int alertState     = digitalRead(ALERT_DIGITAL);  // 0/1

  // Build JSON
  StaticJsonDocument<512> doc;
  doc["deviceId"]    = MQTT_CLIENT_ID;
  doc["ts"]          = (uint32_t) (millis() / 1000);
  if (!isnan(temperatureC)) doc["temp_c"] = temperatureC;
  if (!isnan(humidityPct))  doc["hum_pct"] = humidityPct;
  doc["gas_raw"]     = gasRaw;
  doc["light_raw"]   = ldrRaw;
  doc["alert_dig"]   = alertState;

  // Simple derived flags
  doc["flags"]["gas_high"]   = (gasRaw > 2500);
  doc["flags"]["very_hot"]   = (!isnan(temperatureC) && temperatureC > 45.0);
  doc["flags"]["very_dry"]   = (!isnan(humidityPct)  && humidityPct < 20.0);

  char payload[512];
  size_t n = serializeJson(doc, payload, sizeof(payload));

  // Publish
  boolean ok = mqtt.publish(MQTT_PUB_TOPIC, payload, n);
  Serial.print(F("Publish -> "));
  Serial.print(MQTT_PUB_TOPIC);
  Serial.print(F(" | "));
  Serial.println(ok ? F("OK") : F("FAILED"));
  Serial.println(payload);

  blink(1, 50, 50);
}

void setup() {
  Serial.begin(115200);
  delay(200);

  pinMode(LED_PIN, OUTPUT);
  pinMode(ALERT_DIGITAL, INPUT);   // set to INPUT_PULLUP if your module is open-drain
  digitalWrite(LED_PIN, LOW);

  #if USE_DHT
    dht.begin();
  #endif

  // Better ADC stability
  analogReadResolution(12); // 0..4095
  analogSetAttenuation(ADC_11db); // wider range

  connectWiFi();
  connectMQTT();
}

void loop() {
  if (WiFi.status() != WL_CONNECTED) {
    connectWiFi();
  }
  if (!mqtt.connected()) {
    connectMQTT();
  }
  mqtt.loop();

  unsigned long now = millis();
  if (now - lastPublishMs >= PUBLISH_INTERVAL_MS) {
    lastPublishMs = now;
    publishTelemetry();
  }
}

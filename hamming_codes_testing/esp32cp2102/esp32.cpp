#include <Wire.h>
#include <Adafruit_INA219.h>
#include <mm.h>
#include <io.hpp>
#include <hamm.h>
#include <Arduino.h>

IO io;
Adafruit_INA219 ina219;

void setup() {
  ll_init();
  io.Setup(9600);

  Wire.begin(21,22);
  if (!ina219.begin()) {
    while (1) {
      io.Send("INA219 not found!");
      delay(10);
  }
  } else {
    io.Send("INA219 OK");
  }
}

typedef struct {
  unsigned long duration;
  double        energy;
} test_result_t;

static int _default_copy_test(int count, test_result_t* out) {
  unsigned char chunk[512]       = { 0xFF };
  unsigned char destination[512] = { 0x00 };

  unsigned long startTime = millis();
  unsigned long prevMicros = micros();

  for (int i = 0; i < count; i++) {
    str_memcpy(destination, chunk, 512);

    float V = ina219.getBusVoltage_V();
    float I = ina219.getCurrent_mA() / 1000.0;
    unsigned long nowMicros = micros();
    float dt = (nowMicros - prevMicros) / 1e6;
    prevMicros = nowMicros;
    out->energy += V * I * dt * 1000.0;
  }

  out->duration = millis() - startTime;
  return 1;
}

static int _decoding_copy_test(int m, int count, test_result_t* out) {
  unsigned char chunk[512] = { 0xFF };
  unsigned char destination[calculate_decoded_size(sizeof(chunk), m)];

  unsigned long startTime = millis();
  unsigned long prevMicros = micros();

  for (int i = 0; i < count; i++) {
    decode_hamming_array(chunk, sizeof(chunk), destination, m);

    float V = ina219.getBusVoltage_V();
    float I = ina219.getCurrent_mA() / 1000.0;
    unsigned long nowMicros = micros();
    float dt = (nowMicros - prevMicros) / 1e6;
    prevMicros = nowMicros;
    out->energy += V * I * dt * 1000.0;
  }

  out->duration = millis() - startTime;
  return 1;
}

static unsigned long _encoding_copy_test(int m, int count, test_result_t* out) {
  unsigned char chunk[512] = { 0xFF };
  unsigned char destination[calculate_encoded_size(sizeof(chunk), m)];

  unsigned long startTime = millis();
  unsigned long prevMicros = micros();

  for (int i = 0; i < count; i++) {
    encode_hamming_array(chunk, sizeof(chunk), destination, m);

    float V = ina219.getBusVoltage_V();
    float I = ina219.getCurrent_mA() / 1000.0;
    unsigned long nowMicros = micros();
    float dt = (nowMicros - prevMicros) / 1e6;
    prevMicros = nowMicros;
    out->energy += V * I * dt * 1000.0;
  }

  out->duration = millis() - startTime;
  return 1;
}


static int _menu_shown = 0;

void loop() {
  if (!_menu_shown) {
    io.Send("1<count> - copy");
    io.Send("2<count> - decoding with all parity bits count (2 - 9)");
    io.Send("3<count> - encoding with all parity bits count (2 - 9)");
    _menu_shown = 1;
  }

  String line = io.Handle();
  if (!line.length()) return;
  const char* cmd = line.c_str();
  if (!cmd) return;

  int baseCmd = *cmd - '0';
  int count   = str_atoi(cmd + 1);
  test_result_t res;

  switch (baseCmd) {
    case 1:
      res.energy = 0;
      _default_copy_test(count, &res);
      io.Send("\n[BASE] Duration=" + String(res.duration / count, 3) + "ms Energy=" + String(res.energy / count, 3) + "mJ");
      break;

    case 2:
      for (int m = 2; m <= 9; m++) {
        res.energy = 0;
        _decoding_copy_test(m, count, &res);
        io.Send("[DECODING] m=" + String(m) + ": Duration=" + String(res.duration / count, 3) + "ms, Energy=" + String(res.energy / count, 3) + "mJ");
      }
      break;

    case 3:
      for (int m = 2; m <= 9; m++) {
        res.energy = 0;
        _encoding_copy_test(m, count, &res);
        io.Send("[ENCODING] m=" + String(m) + ": Duration=" + String(res.duration / count, 3) + "ms, Energy=" + String(res.energy / count, 3) + "mJ");
      }
      break;
  }

  _menu_shown = 0;
}

#include <Arduino.h>
#include <stdarg.h>
#include "nifat32/nifat32.h"

extern "C" {
  static int _esp32cp2102_fprintf(const char* fmt, ...) {
      va_list args;
      va_start(args, fmt);
      int len = vsnprintf(NULL, 0, fmt, args);
      va_end(args);

      char* buffer = (char*)malloc(len + 1);
      if (!buffer) return -1;

      va_start(args, fmt);
      vsnprintf(buffer, len + 1, fmt, args);
      va_end(args);

      Serial.print(buffer);
      free(buffer);

      return len;
  }

  static int _esp32cp2102_vfprintf(const char* fmt, va_list args) {
    va_list args_copy;
    va_copy(args_copy, args);

    int len = vsnprintf(NULL, 0, fmt, args_copy);
    va_end(args_copy);

    char* buffer = (char*)malloc(len + 1);
    if (!buffer) return -1;

    va_copy(args_copy, args);
    vsnprintf(buffer, len + 1, fmt, args_copy);
    va_end(args_copy);

    Serial.print(buffer);
    free(buffer);

    return len;
  }
}

void setup() {
    Serial.begin(115200);
    delay(2000);
    Serial.println("Setup started\n");
    
    if (!LOG_setup(_esp32cp2102_fprintf, _esp32cp2102_vfprintf)) {
        Serial.println("LOG_setup() error!\n");
        return;        
    }
    
    print_info("Logging setup complete!");
}

void loop() { /* Ignored */ }

#include <SPI.h>

const uint8_t SD_CS = 5;
SPISettings sdSpiSettings(250000, MSBFIRST, SPI_MODE0);

bool wait_card_busy() {
    auto start_time = millis();
    for (;;) {
        auto r_byte = SPI.transfer(0xFF);
        if (r_byte == 0xFF) {
            return true;
        }

        if (millis() - start_time > 300) {
            return false;
        }
    }
}

void send_card_command(uint8_t cmd, uint32_t arg, uint8_t crc) {
    wait_card_busy();
    SPI.beginTransaction(sdSpiSettings);
    SPI.transfer(0x40 | cmd);
    SPI.transfer(arg >> 24);
    SPI.transfer(arg >> 16);
    SPI.transfer(arg >> 8);
    SPI.transfer(arg);
    SPI.transfer(crc);
    SPI.endTransaction();
}

uint8_t read_card_response() {
    SPI.beginTransaction(sdSpiSettings);
    for (uint8_t i = 0; i < 10; i++) {
        uint8_t response = SPI.transfer(0xFF);
        if (response != 0xFF) {
            SPI.endTransaction();
            return response;
        }
    }

    SPI.endTransaction();
    return 0xFF;
}

bool init_sd_card() {
    delay(300);
    pinMode(SD_CS, OUTPUT);
    digitalWrite(SD_CS, HIGH);
    
    SPI.begin();
    delay(100);

    digitalWrite(SD_CS, HIGH);
    SPI.beginTransaction(sdSpiSettings);
    for (uint8_t i = 0; i < 10; i++) SPI.transfer(0xFF);
    SPI.endTransaction();
    
    delay(10);

    uint8_t response = 0xFF;
    digitalWrite(SD_CS, LOW);

    send_card_command(0, 0, 0x95);
    if (read_card_response() != 0x01) {
        digitalWrite(SD_CS, HIGH);
        return false;
    }

    send_card_command(8, 0x01AA, 0x87);
    if (read_card_response() != 0x01) {
        digitalWrite(SD_CS, HIGH);
        return false;
    }

    uint8_t retries = 3;
    for (uint8_t i = 0; i < retries; i++) {
        send_card_command(55, 0, 0x01);
        if (read_card_response() != 0x01) {
            digitalWrite(SD_CS, HIGH);
            return false;
        }

        send_card_command(41, 0x40000000, 0x01);
        if (read_card_response() == 0x00) break;
    }

    digitalWrite(SD_CS, HIGH);
    SPI.transfer(0xFF);
    return true;
}

bool read_card_block(uint32_t block_addr, uint8_t* buffer, uint16_t buffer_size) {
    digitalWrite(SD_CS, LOW);

    send_card_command(17, block_addr, 0x01);
    if (read_card_response() != 0x00) {
        digitalWrite(SD_CS, HIGH);
        Serial.println("read_card_response() != 0x00");
        return false;
    }

    SPI.beginTransaction(sdSpiSettings);

    while (SPI.transfer(0xFF) != 0xFE);
    for (uint16_t i = 0; i < 512; i++) {
        uint8_t r_byte = SPI.transfer(0xFF);
        if (i < buffer_size) {
            buffer[i] = r_byte;
        }
    }

    SPI.transfer(0xFF);
    SPI.transfer(0xFF);

    SPI.endTransaction();

    digitalWrite(SD_CS, HIGH);
    SPI.transfer(0xFF);

    return true;
}

bool write_card_block(uint32_t block_addr, const void* buffer, uint16_t buffer_size) {
    digitalWrite(SD_CS, LOW);

    send_card_command(24, block_addr, 0x01);
    if (read_card_response() != 0x00) {
        digitalWrite(SD_CS, HIGH);
        Serial.println("read_card_response() != 0x00");
        return false;
    }

    SPI.beginTransaction(sdSpiSettings);
    SPI.transfer(0xFE);

    for (uint16_t i = 0; i < 512; i++) {
        if (i < buffer_size) SPI.transfer(((uint8_t*)buffer)[i]);
        else SPI.transfer(0x00);
    }

    SPI.transfer(0xFF);
    SPI.transfer(0xFF);

    uint8_t response = SPI.transfer(0xFF);

    if ((response & 0x1F) != 0x05) {
        SPI.endTransaction();
        digitalWrite(SD_CS, HIGH);
        Serial.println("(response & 0x1F) != 0x05");
        return false;
    }

    while (SPI.transfer(0xFF) == 0x00);
    SPI.endTransaction();

    digitalWrite(SD_CS, HIGH);
    SPI.transfer(0xFF);
    return true;
}

void setup() {
    Serial.begin(115200);
    delay(400);

    SPI.begin();

    pinMode(SD_CS, OUTPUT);
    digitalWrite(SD_CS, HIGH);
    Serial.println("\nInitializing SD card...");

    if (!init_sd_card()) Serial.println("SD card initialization FAILED!");
    else Serial.println("SD card initialized successfully");
    
    char data[] = "Hello world!";
    if (!write_card_block(0, data, sizeof(data))) Serial.println("SD card write error!");

    char buff[128] = { 0 };
    if (!read_card_block(0, (unsigned char*)buff, sizeof(data))) Serial.println("SD card read error!");
    Serial.println(buff);
}

void loop() {}
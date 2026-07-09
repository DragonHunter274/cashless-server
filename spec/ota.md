# OTA Update Setup Guide

This project implements secure Over-The-Air (OTA) firmware updates using esp32FOTA with RSA signature verification.

## Overview

The OTA system allows you to remotely update the ESP32 firmware over HTTP/HTTPS without physical access to the device. All firmware binaries must be signed with your private key, and the ESP32 verifies the signature before applying any updates.

## Security Features

- **RSA 4096-bit Signature Verification**: All firmware must be signed with the private key
- **Automatic Signature Checking**: ESP32 rejects unsigned or incorrectly signed firmware
- **Secure by Default**: Security cannot be disabled in the production build
- **Version Management**: Semantic versioning prevents downgrade attacks

## Initial Setup

### 1. Key Pair (Already Generated)

The project includes:
- `priv_key.pem` - RSA private key (4096-bit) - **KEEP SECRET!**
- `rsa_key.pub` - RSA public key (embedded in firmware)
- `include/pub_key.h` - Public key in C header format

**⚠️ IMPORTANT: Never commit `priv_key.pem` to version control!**

### 2. Configure OTA Manifest URL

Edit your `include/secrets.h` file:

```cpp
#define OTA_MANIFEST_URL "http://yourserver.com/firmware/manifest.json"
```

This URL should point to the `manifest.json` file on your web server.

### 3. Update Firmware Version

Edit `include/OTA.h` to set the current firmware version:

```cpp
#define FIRMWARE_VERSION_MAJOR 1
#define FIRMWARE_VERSION_MINOR 0
#define FIRMWARE_VERSION_PATCH 0
```

The device will only update to **higher** version numbers.

## Building and Signing Firmware

### Automatic Signing (Recommended)

Firmware signing is **automatically performed** after every build via PlatformIO's post-build script:

```bash
pio run
```

This will:
1. Build the firmware binary
2. **Automatically sign** the firmware with your private key
3. Verify the signature
4. Create `firmware_release/firmware.img` with embedded signature
5. Generate `firmware_release/manifest.json`

The firmware version is automatically extracted from `include/OTA.h`.

**Note**: Automatic signing only works if `priv_key.pem` exists. If the key is missing, the build will succeed but signing will be skipped with a warning.

### Manual Signing (Alternative)

If you need to manually sign a firmware binary, use the standalone script:

```bash
./sign_firmware.sh [version]
```

Example:
```bash
./sign_firmware.sh 1.0.1
```

This script will:
1. Read the firmware from `.pio/build/esp32-s3-devkitc-1/firmware.bin`
2. Generate an RSA signature using your private key
3. Verify the signature
4. Create signed `firmware.img` with embedded signature
5. Generate `manifest.json` file
6. Place everything in the `firmware_release/` directory

### Upload to Web Server

Upload the contents of `firmware_release/` to your web server:

```
http://yourserver.com/firmware/
├── manifest.json
└── firmware.img (contains embedded signature + firmware)
```

## Manifest File Format

The `manifest.json` file tells the ESP32 about available firmware:

```json
{
  "type": "esp32-fota-http",
  "version": "1.0.1",
  "bin": "firmware.img"
}
```

- **type**: Always "esp32-fota-http"
- **version**: Semantic version (must be higher than current)
- **bin**: Filename or URL of signed firmware image (relative to manifest or absolute)

**Note**: The signature is embedded at the beginning of the `firmware.img` file in the format: `[512-byte RSA signature][firmware binary]`

## How OTA Updates Work

1. **Periodic Checks**: The ESP32 checks for updates every hour (configurable in `OTA.h`)
2. **Version Comparison**: Downloads `manifest.json` and compares versions
3. **Download**: If newer version is available, downloads `firmware.img`
4. **Signature Extraction**: Extracts the 512-byte signature from the beginning of the file
5. **Signature Verification**: Verifies the firmware signature using the embedded public key
6. **Flash Update**: If signature is valid, flashes the firmware portion (after signature)
7. **Reboot**: Automatically reboots with the new firmware

## Configuration Options

### Update Check Interval

Edit `include/OTA.h`:

```cpp
#define OTA_CHECK_INTERVAL_MS 3600000  // 1 hour in milliseconds
```

### Firmware Name

Edit `include/OTA.h`:

```cpp
#define FIRMWARE_NAME "mdb-cashless"
```

## Monitoring

The OTA system logs all activities using the FastSyslog system:

- Update checks
- Version comparisons
- Download progress
- Signature verification results
- Update success/failure

Monitor your syslog server for OTA-related messages.

## Troubleshooting

### Automatic Signing Not Working

1. **Check if private key exists**: Ensure `priv_key.pem` is in the project root
2. **Check build output**: Look for the "POST-BUILD: Firmware Signing" section in build logs
3. **OpenSSL not found**: Make sure `openssl` is installed and in your PATH
4. **Permission error**: Ensure `scripts/post_build_sign.py` is readable

If signing fails, the build will still succeed but you'll see a warning. Check the build output for details.

### Update Not Detected

1. Check that the version in `manifest.json` is **higher** than the current firmware version
2. Verify the manifest URL is correct and accessible from the ESP32
3. Check network connectivity
4. Review the version set in `include/OTA.h` vs what's in your uploaded `manifest.json`

### Signature Verification Failed

1. Ensure the firmware was signed with the correct private key
2. Verify the public key in `include/pub_key.h` matches `rsa_key.pub`
3. Check that `firmware.img` wasn't corrupted during transfer to the web server
4. Run `./verify_firmware.sh firmware_release/firmware.img` to test locally

### Download Fails

1. Check that the ESP32 can reach the web server
2. Verify file URLs in manifest.json are correct
3. Ensure the web server allows HTTP range requests
4. Check firewall rules

## Signature Verification

### Automatic Verification (Recommended)

Use the provided verification script:

```bash
./verify_firmware.sh firmware_release/firmware.img
```

This will extract and verify the embedded signature automatically.

### Manual Verification

To manually verify a signed firmware with embedded signature:

```bash
# Extract the signature (first 512 bytes)
dd if=firmware_release/firmware.img of=firmware_release/extracted.sig bs=1 count=512

# Extract the firmware (everything after the signature)
dd if=firmware_release/firmware.img of=firmware_release/extracted.bin bs=1 skip=512

# Verify the signature
openssl dgst -sha256 -verify rsa_key.pub \
  -signature firmware_release/extracted.sig \
  firmware_release/extracted.bin
```

Should output: `Verified OK`

**Note**: Both `sign_firmware.sh` and `verify_firmware.sh` perform signature verification to ensure integrity before deployment.

## Regenerating Keys (If Needed)

If you need to generate new RSA keys:

```bash
# Generate new 4096-bit RSA private key
openssl genrsa -out priv_key.pem 4096

# Extract public key
openssl rsa -in priv_key.pem -pubout -out rsa_key.pub

# Convert public key to C header format
echo 'inline const char* pub_key = R"ROOT_CA(' > include/pub_key.h
cat rsa_key.pub >> include/pub_key.h
echo ')ROOT_CA";' >> include/pub_key.h
```

**⚠️ WARNING: Changing keys requires reflashing ALL devices with the new public key!**

## Security Best Practices

1. **Protect Private Key**: Store `priv_key.pem` securely, never commit to git
2. **HTTPS**: Use HTTPS for your firmware server (configure root_ca in `OTA.cpp`)
3. **Access Control**: Restrict write access to your firmware server
4. **Version Control**: Always increment version numbers, never reuse
5. **Testing**: Test updates on a development device before production rollout
6. **Backups**: Keep backups of working firmware versions

## Advanced: Using HTTPS Server

To use HTTPS instead of HTTP, you need to provide the root CA certificate:

1. Get your server's root CA certificate
2. Edit `src/OTA.cpp` and add the certificate:

```cpp
const char* root_ca = R"ROOT_CA(
-----BEGIN CERTIFICATE-----
[Your root CA certificate here]
-----END CERTIFICATE-----
)ROOT_CA";
```

3. Uncomment the root_ca configuration in `setupOTA()`:

```cpp
CryptoMemAsset *MyRootCA = new CryptoMemAsset("Root CA", root_ca, strlen(root_ca) + 1);
cfg.root_ca = MyRootCA;
```

## Task Architecture

The OTA system runs as a low-priority FreeRTOS task (`ota_task`) on Core 0, ensuring it doesn't interfere with the critical MDB communication on Core 1.

## Quick Reference

| File | Purpose |
|------|---------|
| `include/OTA.h` | OTA configuration and version |
| `src/OTA.cpp` | OTA implementation |
| `include/pub_key.h` | RSA public key (embedded) |
| `priv_key.pem` | RSA private key (signing) - **KEEP SECRET!** |
| `rsa_key.pub` | RSA public key (verification) |
| `scripts/post_build_sign.py` | PlatformIO post-build script (automatic signing) |
| `sign_firmware.sh` | Manual firmware signing script |
| `verify_firmware.sh` | Signature verification script |
| `firmware_release/` | Signed firmware output directory |
| `firmware_release/firmware.img` | Signed firmware with embedded signature |
| `firmware_release/manifest.json` | Version manifest for OTA |

## Support

For issues or questions, check:
- Serial output during OTA updates
- Syslog messages for detailed logging
- esp32FOTA library documentation: https://github.com/chrisjoyce911/esp32FOTA

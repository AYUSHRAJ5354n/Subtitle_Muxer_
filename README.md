# 🎬 Subtitle Muxer Bot

Welcome to **Subtitle_Muxer_Bot** — your all-in-one, powerful automation bot for video and subtitle processing! Built with modern Python and leveraging FFmpeg, this bot makes video file handling, subtitle muxing, and media management easy, efficient, and scalable.

---

## 🚀 Features at a Glance

### 🛠️ **Media Processing Automation**
- **Soft & Hard Subtitle Muxing:** Automatically add subtitles to any video (burn-in or attach as selectable).
- **Unlimited File Size:** Designed for streaming and handling massive files without memory bottlenecks.
- **Thumbnail & Screenshot Generation:** Instantly create video previews for quick referencing.

### ⚡ **Performance & Resource Optimization**
- **Asynchronous Processing:** High-performance file and video handling, even for concurrent operations.
- **System Resource Monitoring:** Dynamically adapts to memory, disk, and CPU availability.
- **Smart FFmpeg Tuning:** Automatically selects FFmpeg settings for best speed/quality based on your system.

### 📊 **Rich Progress & Feedback**
- **Animated Progress Bars:** Beautiful, real-time feedback (with speed and ETA) for all tasks.
- **Detailed Status Updates:** In-app messages show ongoing actions, speeds, and completion estimates.

### 🔒 **Robust Validation & Reliability**
- **File & Subtitle Validation:** Checks video/subtitle format, size, and compatibility before processing.
- **Legacy Compatibility:** Classic APIs are maintained for easy upgrades and integration.

### 🧹 **Automatic Cleanup**
- **Periodic Temp File Cleanup:** Keeps your storage clean by removing unused files automatically.

### 🐳 **Docker Ready**
- **Easy Containerization:** One-command Docker build — deploy anywhere, instantly.

---

## 🌟 Why Choose Subtitle_Muxer_Bot?

- **All-in-one:** Video info, subtitles, thumbnails — all automated.
- **Modern Python:** Asynchronous, fast, and cloud/deployment friendly.
- **Zero config:** FFmpeg auto-detected, everything managed by the bot.
- **Customizable:** Easily extend or automate in your own workflows.

---

## 📦 Getting Started

### Requirements
- Python 3.11+  
- FFmpeg & FFprobe in your PATH  
- Docker (optional, easy deployment)

### Quick Start

1. **Clone & Install**
    ```bash
    git clone https://github.com/ChronosBots/Subtitle_Muxer_Bot.git
    cd Subtitle_Muxer_Bot
    pip install -r requirements.txt
    ```

2. **Run**
    ```bash
    python bot.py
    ```

3. **(Optional) Docker**
    ```bash
    docker build -t subtitle_muxer_bot .
    docker run -it --rm subtitle_muxer_bot
    ```

---

## 🧩 Key Components

- `bot.py` — Main bot logic, user interaction, task orchestration.
- `utils.py` — Advanced streaming file manager, video processor, system monitor, and more.

---

## 🤝 Contribute & Support

We welcome contributions! Open issues, fork & PR, or just star ⭐️ the repo if you find it useful.

**License:** [MIT](LICENSE)  
**Maintained by:** [ChronosBots](https://github.com/ChronosBots)

---
```
**Subtitle_Muxer_Bot** — *Your ultimate solution for video & subtitle automation!*
```

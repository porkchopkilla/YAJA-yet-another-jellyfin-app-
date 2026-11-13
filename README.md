PSA - THIS IS EXPERIMENTAL SOFTWARE , USE AT YOUR OWN RISK. WHILE I HAVE NOT HAD ISSUES USING 90DNS AND BEING CONNECTED TO WIFI, THERE ARE OTHERS WHO ARE NOT SO LUCKY AND ARE EXPERIENCING SUPER NAGS, ACCIDENTAL UPDATES, ETC (NOT WITH MY SOFTWARE JUST IN GENERAL) - SO PLEASE BE AWARE OF THE DANGERS WITH CONNECTING TO THE INTERNET. THIS SOFTWARE REQUIRES A INTERNET CONNECTION. I AM NOT RESPONSIBLE FOR ANY ACCIDENTAL UPDATES OR SUPER NAGS. 


Download the yaja.civapp from the releases tab and upload with FTP to your Civ Homebrew launcher

## Features

### A familiar, controller-first interface
- Apple TV-style layout: a tab bar for **Home**, each of your libraries, **Live TV** and **Settings**, switched with **L / R**
- **Continue Watching** and **Next Up** rows with wide artwork on a Jellyfin blue-to-purple gradient, scrolling up to 24 items
- Library grids sorted **A-Z** the way Jellyfin does it (numbers in natural order, leading "The" ignored), loading more as you scroll
- TV shows in two panes: **seasons on the left, episodes on the right**, over the show's backdrop, with thumbnails, runtimes, descriptions, progress bars and watched markers
- Full-screen details pages showing each movie's backdrop or each episode's own still, with **Resume**, **Play from the beginning** and **All episodes**
- Focus returns to where you were when you go back
- A splash screen while your libraries and artwork load behind it

### Playback
- On-screen media bar: **Pause/Play**, **skip 10 s**, **Subtitles**, **Audio track** and **Quality**
- Subtitles burned in by the server; switching subtitles, audio or quality picks up at the same moment
- Skip 10 seconds with left/right or 1 minute with L/R, with repeated presses combined into one jump
- Resume where you left off; progress and watched status sync with your Jellyfin server
- **Auto-play** the next episode with an "Up next" countdown, across seasons (can be turned off)
- Keeps the screen from dimming while a video plays, where the console supports it (not yet confirmed on hardware)

### Live TV (not yet confirmed on hardware)
- A channel guide with a 2-hour window, channel logos and numbers, a "now" marker, and details of the highlighted program
- Up/down change channel at the same time slot; left/right move through programs, and the guide slides forward as you go
- Tune what's on with **A**, then change channel with **L / R**
- Guide times follow your server's clock, with a time zone setting if the console's is wrong
- The tuner is released when you stop or change channel

### Connecting
- At home over HTTP (`http://192.168.1.20:8096`) or remotely over **HTTPS** (`https://jellyfin.example.com`, with or without a path such as `/jellyfin`)
- Server names resolved through public DNS, falling back to the console's resolver
- HTTPS through the console's built-in TLS, with certificate checking (can be turned off for self-signed certificates)
- **Test connection** checks each step (name lookup, connection, secure handshake, Jellyfin's reply) and shows which one fails
- A warning if you enter an unencrypted address outside your home network
- Sign in with a username and password or **Quick Connect**; your session is remembered, but your password is never stored
- An on-screen keyboard with a symbols page, `http://` and `https://` keys, and room for 128-character addresses

### Settings and troubleshooting
- Streaming quality presets from 480p 2 Mbps to 1080p 12 Mbps
- Live stats overlay during playback (**X**): frame rate, frame spacing, buffer, dropped or late frames and more
- **30-second playback reports** (**Y**) with plain-language findings, saved as text files you can download over CivHomebrew's FTP
- A Diagnostics log and an About & credits screen

### Installing
- A `.civapp` for **CivHomebrew**, or a single `.js` file for **Gezine's Remote JS Loader**
- Updates install over the previous version and keep your settings

### Known limitations
- Every video is transcoded by the server (no direct play), so your account needs transcoding permission
- Maximum 1920x1080; no IPv6
- Requires the Civilization VII 1.0.0 setup that Moonlight for CivSwitch uses

# py-scripts

## 1. Для деплоя нужно создать беслатный аккаунт на AWS и создать EC2 инстанс (аккуратно с настройкой чтобы не влезть в платный тир)
https://aws.amazon.com/free/

——
Step-by-Step Guide to Deploy and Schedule a Python Script on AWS EC2
Step 1: Create an AWS EC2 Instance
1. Log in to AWS Console
    * Go to AWS Console
    * Navigate to EC2 under the Compute section.
2. Launch a New Instance
    * Click Launch Instance.
    * Choose Amazon Linux 2 or Ubuntu 22.04 as the OS.
    * Select an instance type. t2.micro (free tier) should be enough unless your script requires more memory/CPU.
    * Click Next: Configure Instance Details.
3. Configure Instance Details
    * Keep defaults.
    * Click Next: Add Storage, then Next: Add Tags.
    * Add a tag (optional):
        * Key: Name
        * Value: Python-Script-Runner
4. Configure Security Group
    * Add a rule to allow SSH access:
        * Type: SSH
        * Protocol: TCP
        * Port: 22
        * Source: My IP (for security)
5. Launch the Instance
    * Click Review and Launch.
    * Create a new key pair (or use an existing one) and download the .pem file (important for SSH access).
    * Click Launch Instance.

Step 2: Connect to the EC2 Instance
1. Open a terminal and navigate to your .pem file location.
2. Change permissions:chmod 400 your-key.pem
3. 
4. Connect to the instance:ssh -i your-key.pem ec2-user@your-instance-public-ip
5. Replace your-instance-public-ip with the actual IP found in EC2 → Instances.

Step 3: Install Dependencies
1. Update packages:sudo yum update -y   # For Amazon Linux
2. sudo apt update -y   # For Ubuntu
3. 
4. Install Python & Pip:sudo yum install python3 -y  # For Amazon Linux
5. sudo apt install python3 -y  # For Ubuntu
6. 
7. Install pip (if not installed):sudo yum install python3-pip -y
8. sudo apt install python3-pip -y
9. 

Step 4: Upload Your Python Script
1. From Local Machine to EC2:Open your terminal and use scp:scp -i your-key.pem your_script.py ec2-user@your-instance-public-ip:~
2. This will copy your_script.py to your EC2 home directory.
3. SSH into EC2 and verify:ls
4. 

Step 5: Install Python Libraries
1. SSH into EC2.
2. Install required Python libraries (modify based on your script):pip3 install requests beautifulsoup4 airtable-python-wrapper
3. 

Step 6: Test the Script Manually
Run:
python3 your_script.py
Ensure it works without errors.

Step 7: Automate with Cron Job
1. Edit the crontab:crontab -e
2. 
3. Add the following line to run the script twice a day (e.g., at 6 AM and 6 PM UTC):0 6,18 * * * /usr/bin/python3 /home/ec2-user/your_script.py >> /home/ec2-user/script.log 2>&1
4. 
5. Save and exit (Ctrl+X, then Y, then Enter).

Step 8: Keep the Script Running
If the script needs to run for an hour, consider using nohup:
nohup python3 your_script.py > output.log 2>&1 &
This ensures it runs even if you disconnect.

Monitoring & Costs
* Check Logs:tail -f /home/ec2-user/script.log
* 
* Estimate Cost:
    * t2.micro is free tier (750 hours/month).
    * If running long, it may cost $5–$10 per month.
    * Check AWS Billing Dashboard.

Next Steps
* Secure your instance (disable SSH root access).
* Add error handling in your script.
* Consider Amazon CloudWatch for monitoring.
Let me know if you need help with any step! 🚀


## 2. Подключение к серверу через SSH

```
chmod 400 python-script-key.pem
ssh -i python-script-key.pem ec2-user@server-ip
```

Если не подключается, то нужно проверить настройки безопасности в AWS. - разрешить доступ для ИП текущего компьютера (Security Groups)


## 3. Копирование файлов на сервер

```
scp -i python-script-key.pem FILE_TO_COPY.py ec2-user@serverip:~/folder_to_copy

mkdir myproject
cd myproject
```

## 4. Инициализация виртуального окружения и установка зависимостей

```
python3 -m venv myenv
source myenv/bin/activate

pip install -r requirements.txt
```


## 5. Запуск скрипта

```
python ./script.py
```

## Telegram bot интеграция

2. 🤖 Create Telegram Bot & Channel
Search in Telegram: @BotFather

Send: /newbot and follow instructions

Save the bot token it gives you (e.g., 123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11)

Create a new Telegram channel

Add your bot as an admin of that channel (for sending messages only)

Send any test message to the channel from your personal Telegram account

Get the channel ID:

Visit this URL (replacing BOT_TOKEN):
https://api.telegram.org/bot<BOT_TOKEN>/getUpdates

You'll see something like:

json
Copy
Edit
"chat": {
  "id": -1001234567890,
  "title": "YourChannel",
  "type": "channel"
}
Save the chat.id (e.g., -1001234567890)


----------

# Daemon run

Great! Based on your setup, we’ll adjust the **`systemd` service** to:

* Use your **Python virtual environment** (`myenv`)
* Run your script located in: `/home/ec2-user/fedsfm_parser`
* Activate the environment before running `bot_server.py`

---

### ✅ Step-by-Step Adjusted Configuration

---

### **1. Create `systemd` service file**

```bash
sudo nano /etc/systemd/system/telegrambot.service
```

---

### **2. Paste the following config**:

```ini
[Unit]
Description=Telegram Bot Service
After=network.target

[Service]
User=ec2-user
WorkingDirectory=/home/ec2-user/fedsfm_parser
ExecStart=/home/ec2-user/fedsfm_parser/myenv/bin/python bot_server.py
Restart=always
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
```

✅ This:

* Uses the Python from your virtual environment (`myenv/bin/python`)
* Starts the bot script directly
* Restarts the bot on crash
* Ensures logs are flushed (with `PYTHONUNBUFFERED=1`)

---

### **3. Reload and start the service**

```bash
sudo systemctl daemon-reexec
sudo systemctl daemon-reload
sudo systemctl enable telegrambot
sudo systemctl start telegrambot
```

#### Stop

```bash
sudo systemctl stop telegrambot
```

---

### **4. Check if it works**

```bash
sudo systemctl status telegrambot
```

If something fails:

```bash
journalctl -u telegrambot -n 50 --no-pager
```

---

### ✅ Optional: View live logs

```bash
journalctl -u telegrambot -f
```

---

This setup makes your Telegram bot:

* Always running
* Auto-starting on reboot
* Using your virtualenv
* Isolated and production-friendly

Let me know if you want to log output to a file or use `tmux` during development.



-----------

To run your `main.py` script **every 2 hours from 8:00 to 20:00 Moscow time (Mon–Fri)** — regardless of your server's actual timezone — the best approach is to use **`systemd` + `systemd timer`**, paired with **`systemd-timezone-aware logic in Python`** or by explicitly setting the timezone for scheduling.

---

### ✅ Here's what you need to do:

---

## ⚙️ 1. **Set up a `systemd` service for `main.py`**

Create a service to run `main.py` **on demand** (not a daemon, just one-shot):

```bash
sudo nano /etc/systemd/system/runmain.service
```

```ini
[Unit]
Description=Run main.py one-shot task

[Service]
User=ec2-user
WorkingDirectory=/home/ec2-user/fedsfm_parser
ExecStart=/home/ec2-user/fedsfm_parser/myenv/bin/python main.py
Environment=PYTHONUNBUFFERED=1
```

> ✅ This only runs `main.py` when triggered by a timer.

---

## 🕒 2. **Create a `systemd timer`**

Create the timer that runs the service every 2 hours from **8:00 to 20:00** on **Monday to Friday**:

```bash
sudo nano /etc/systemd/system/runmain.timer
```

```ini
[Unit]
Description=Run main.py every 2 hours from 8:00 to 20:00 MSK (Mon–Fri)

[Timer]
OnCalendar=Mon..Fri 08:00
OnCalendar=Mon..Fri 10:00
OnCalendar=Mon..Fri 12:00
OnCalendar=Mon..Fri 14:00
OnCalendar=Mon..Fri 16:00
OnCalendar=Mon..Fri 18:00
OnCalendar=Mon..Fri 20:00
Persistent=true

[Install]
WantedBy=timers.target
```

> 🔁 `Persistent=true` ensures missed runs (e.g., after reboot) are triggered on boot.

---

## 🌍 3. **Ensure the schedule runs in Moscow time**

A `systemd` timer always uses **server’s local time**, so:

* Either change your server timezone to **Moscow time**:

```bash
sudo timedatectl set-timezone Europe/Moscow
```

* **Or keep the server’s timezone unchanged** and handle the logic inside `main.py`:

```python
# Inside main.py
from datetime import datetime
import pytz

moscow = pytz.timezone("Europe/Moscow")
now = datetime.now(moscow)

if not (8 <= now.hour <= 20 and now.weekday() < 5):
    print("Not within allowed Moscow time window. Exiting.")
    exit()
```

This ensures the script only runs during your required window regardless of server time.

---

## ✅ 4. **Enable and start the timer**

```bash
sudo systemctl daemon-reload
sudo systemctl enable runmain.timer
sudo systemctl start runmain.timer
```

You can check timer status with:

```bash
systemctl list-timers --all
```

---

## 🧪 5. (Optional) Test your setup

To simulate a manual run:

```bash
sudo systemctl start runmain.service
```

To check logs:

```bash
journalctl -u runmain.service --no-pager
```

---

### ✅ Summary

| Task                           | Done by                                      |
| ------------------------------ | -------------------------------------------- |
| Run every 2h from 8–20 Mon–Fri | `systemd.timer`                              |
| Run `main.py` on-demand        | `runmain.service`                            |
| Ensure Moscow hours            | Either via `timedatectl` or inside `main.py` |

Let me know if you'd like it to log to a file instead of system journal or if you want to get Telegram notifications on success/failure!

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


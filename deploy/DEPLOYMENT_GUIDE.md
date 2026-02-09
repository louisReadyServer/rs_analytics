# RS Analytics - VPS Deployment Guide

## Your VPS Details

| Field | Value |
|-------|-------|
| IP Address | `103.14.79.195` |
| Username | `root` |
| Password | `4Bj8TaZq` |
| Console URL | https://novnc101.readyservervps.com/v10008314-N6HJ0Q7E |

> ⚠️ **SECURITY WARNING**: Change your password after deployment!

---

## Quick Start (3 Steps)

### Step 1: Connect to VPS & Run Setup

Open **PowerShell** or **Command Prompt** and run:

```bash
ssh root@103.14.79.195
```

Enter password: `4Bj8TaZq`

Once connected, run:

```bash
# Update system and install dependencies
apt update && apt upgrade -y
apt install -y python3 python3-pip python3-venv git nginx ufw

# Create app user and directory
useradd -m -s /bin/bash rsanalytics
mkdir -p /home/rsanalytics/rs_analytics
chown -R rsanalytics:rsanalytics /home/rsanalytics/rs_analytics

# Configure firewall
ufw allow 22/tcp
ufw allow 80/tcp
ufw allow 443/tcp
ufw --force enable
```

### Step 2: Upload Your Files

**Option A: Using SCP (from your local machine)**

Open a NEW PowerShell window (don't close the SSH session) and run:

```powershell
cd "c:\Users\lowai\OneDrive\Desktop\RS_Analytics\rs_analytics"

scp -r app etl scripts requirements.txt .env.example deploy root@103.14.79.195:/home/rsanalytics/rs_analytics/
```

**Option B: Using WinSCP (easier)**

1. Download WinSCP: https://winscp.net/download
2. Connect with:
   - Host: `103.14.79.195`
   - User: `root`  
   - Password: `4Bj8TaZq`
3. Navigate to `/home/rsanalytics/rs_analytics/`
4. Upload these folders from your project:
   - `app/`
   - `etl/`
   - `scripts/`
   - `deploy/`
   - `requirements.txt`
   - `.env.example`

### Step 3: Complete Setup on VPS

Back in your SSH session:

```bash
# Switch to app directory
cd /home/rsanalytics/rs_analytics

# Create directories
mkdir -p data logs secrets

# Set permissions
chown -R rsanalytics:rsanalytics /home/rsanalytics/rs_analytics

# Switch to rsanalytics user
su - rsanalytics
cd rs_analytics

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install --upgrade pip
pip install -r requirements.txt

# Create .env file
cp .env.example .env
nano .env  # Edit with your API credentials
```

---

## Configure the Service

Back as root (type `exit` to leave rsanalytics user):

```bash
# Create systemd service
cat > /etc/systemd/system/rsanalytics.service << 'EOF'
[Unit]
Description=RS Analytics Streamlit Dashboard
After=network.target

[Service]
Type=simple
User=rsanalytics
Group=rsanalytics
WorkingDirectory=/home/rsanalytics/rs_analytics
Environment="PATH=/home/rsanalytics/rs_analytics/venv/bin:/usr/bin"
ExecStart=/home/rsanalytics/rs_analytics/venv/bin/streamlit run app/main.py --server.port 8501 --server.address 127.0.0.1 --server.headless true
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# Configure Nginx
cat > /etc/nginx/sites-available/rsanalytics << 'EOF'
server {
    listen 80;
    server_name _;

    location / {
        proxy_pass http://127.0.0.1:8501;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_read_timeout 86400;
        proxy_buffering off;
    }

    location /_stcore/stream {
        proxy_pass http://127.0.0.1:8501/_stcore/stream;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_read_timeout 86400;
    }
}
EOF

# Enable Nginx site
ln -sf /etc/nginx/sites-available/rsanalytics /etc/nginx/sites-enabled/
rm -f /etc/nginx/sites-enabled/default
nginx -t

# Start everything
systemctl daemon-reload
systemctl enable rsanalytics
systemctl start rsanalytics
systemctl restart nginx
```

---

## Access Your Dashboard

Open in browser: **http://103.14.79.195**

---

## Useful Commands

| Action | Command |
|--------|---------|
| Check app status | `systemctl status rsanalytics` |
| View live logs | `journalctl -u rsanalytics -f` |
| Restart app | `systemctl restart rsanalytics` |
| Stop app | `systemctl stop rsanalytics` |
| Restart Nginx | `systemctl restart nginx` |
| Edit environment | `nano /home/rsanalytics/rs_analytics/.env` |

---

## Run ETL Manually

```bash
su - rsanalytics
cd rs_analytics
source venv/bin/activate

# Test connections
python scripts/test_connections_unified.py --all

# Run full data pull
python scripts/run_etl_unified.py --source all --lifetime
```

---

## Troubleshooting

### App won't start
```bash
# Check logs
journalctl -u rsanalytics -n 50

# Test manually
su - rsanalytics
cd rs_analytics
source venv/bin/activate
streamlit run app/main.py
```

### Can't connect to website
```bash
# Check if app is running
systemctl status rsanalytics

# Check if Nginx is running
systemctl status nginx

# Check firewall
ufw status
```

### Permission errors
```bash
chown -R rsanalytics:rsanalytics /home/rsanalytics/rs_analytics
```

---

## Security Recommendations

1. **Change VPS password immediately:**
   ```bash
   passwd root
   ```

2. **Set up SSH key authentication** (disable password login)

3. **Add HTTPS with Let's Encrypt:**
   ```bash
   apt install certbot python3-certbot-nginx
   certbot --nginx -d yourdomain.com
   ```

4. **Add authentication to Streamlit** (optional - add to .streamlit/config.toml)

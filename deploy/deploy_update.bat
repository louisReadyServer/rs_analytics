@echo off
REM ==============================================
REM RS Analytics - Quick Update Deployment Script
REM ==============================================
REM This uploads ONLY the updated files to VPS
REM ==============================================

echo ==========================================
echo RS Analytics - Deploy Updates
echo ==========================================
echo.

REM VPS Details
set VPS_IP=103.14.79.195
set VPS_USER=root
set VPS_DEST=/home/rsanalytics/rs_analytics

echo This will upload app updates to VPS
echo.
echo VPS IP: %VPS_IP%
echo Destination: %VPS_DEST%
echo.

echo [Step 1] Uploading updated files...
echo.

echo Running SCP commands...
echo.

REM Upload entire app directory (includes all updates)
scp -r app %VPS_USER%@%VPS_IP%:%VPS_DEST%/

REM Upload documentation
scp -r docs %VPS_USER%@%VPS_IP%:%VPS_DEST%/

echo.
echo ==========================================
echo Upload Complete!
echo ==========================================
echo.
echo Next steps:
echo 1. SSH into your VPS: ssh %VPS_USER%@%VPS_IP%
echo 2. Restart the service: systemctl restart rsanalytics
echo 3. Check status: systemctl status rsanalytics
echo 4. View logs: journalctl -u rsanalytics -f
echo 5. Access dashboard: http://%VPS_IP%
echo.

pause

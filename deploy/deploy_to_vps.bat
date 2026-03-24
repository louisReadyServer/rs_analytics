@echo off
REM ==============================================
REM RS Analytics - Unified VPS Deployment Script
REM ==============================================
REM Usage:
REM   deploy_to_vps.bat          - Update only (app + docs)
REM   deploy_to_vps.bat full     - Full project upload
REM ==============================================

echo ==========================================
echo RS Analytics - Deploy to VPS
echo ==========================================
echo.

REM ── VPS Connection ───────────────────────────
REM Set these via environment variables or edit here.
REM NEVER commit real credentials to version control.
if "%VPS_IP%"=="" set VPS_IP=YOUR_VPS_IP
if "%VPS_USER%"=="" set VPS_USER=root
if "%VPS_DEST%"=="" set VPS_DEST=/home/rsanalytics/rs_analytics

echo VPS IP:   %VPS_IP%
echo User:     %VPS_USER%
echo Dest:     %VPS_DEST%
echo.

if "%VPS_IP%"=="YOUR_VPS_IP" (
    echo ERROR: Set VPS_IP environment variable first.
    echo   set VPS_IP=1.2.3.4
    echo   deploy_to_vps.bat
    pause
    exit /b 1
)

REM ── Mode selection ────────────────────────────
if /I "%1"=="full" goto :full_upload

REM ── Quick update (default) ────────────────────
echo [Mode] Quick update - uploading app + docs
echo.

scp -r app %VPS_USER%@%VPS_IP%:%VPS_DEST%/
scp -r docs %VPS_USER%@%VPS_IP%:%VPS_DEST%/

goto :done

:full_upload
echo [Mode] Full upload - uploading entire project
echo.

scp -r app etl scripts scheduler analysis data logs secrets requirements.txt .env.example .gitignore deploy %VPS_USER%@%VPS_IP%:%VPS_DEST%/

:done
echo.
echo ==========================================
echo Upload Complete!
echo ==========================================
echo.
echo Next steps:
echo   1. ssh %VPS_USER%@%VPS_IP%
echo   2. systemctl restart rsanalytics
echo   3. systemctl status rsanalytics
echo   4. journalctl -u rsanalytics -f
echo.

pause

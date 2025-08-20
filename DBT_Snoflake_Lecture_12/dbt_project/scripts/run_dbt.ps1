# ================== Настройки (исправь под себя) ==================
$ProjectDir = "C:\Users\Yoga\DBT_Snoflake_Lecture_12\dbt_project"  # путь к твоему проекту
$Venv       = Join-Path $ProjectDir ".venv\Scripts\Activate.ps1"   # активация venv
$DbtCmd     = "dbt build --threads 4"                              # какую команду запускать (run/build/теги)

# E-mail настройки (лучше держать пароль в переменной окружения DBT_SMTP_PASS)
$MailFrom = "you@example.com"
$MailTo   = "you@example.com"
$Smtp     = "smtp.example.com"
$Port     = 587
$User     = "you@example.com"
$Pass     = $env:DBT_SMTP_PASS  # ЗАДАЙ через переменную окружения, см. ниже

# отправлять письмо всегда (True) или только при ошибке (False)
$NotifyAlways = $true

# ================== Подготовка путей/логов ==================
$LogDir = Join-Path $ProjectDir "logs"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
$Log = Join-Path $LogDir ("dbt_build_" + (Get-Date -Format "yyyyMMdd_HHmmss") + ".log")

# ================== Запуск dbt ==================
try {
  & $Venv
  Set-Location $ProjectDir

  "=== $(Get-Date) :: dbt deps ==="  | Tee-Object -FilePath $Log -Append
  dbt deps                            | Tee-Object -FilePath $Log -Append

  "=== $(Get-Date) :: $DbtCmd ==="   | Tee-Object -FilePath $Log -Append
  Invoke-Expression $DbtCmd           | Tee-Object -FilePath $Log -Append

  $exit = $LASTEXITCODE
} catch {
  $exit = 1
  "ERROR: $_" | Tee-Object -FilePath $Log -Append
}

# ================== Уведомление по e-mail ==================
try {
  $subject = "dbt: " + ($(if ($exit -eq 0) {"SUCCESS"}) else {"FAIL"}) + " @ " + (Get-Date -Format "yyyy-MM-dd HH:mm")
  $body    = Get-Content $Log -Raw

  if ($NotifyAlways -or $exit -ne 0) {
    $secure = ConvertTo-SecureString ($Pass) -AsPlainText -Force
    $cred   = New-Object System.Management.Automation.PSCredential ($User, $secure)

    Send-MailMessage `
      -From $MailFrom -To $MailTo `
      -Subject $subject -Body $body `
      -SmtpServer $Smtp -Port $Port -UseSsl -Credential $cred `
      -Attachments $Log
  }
} catch {
  "MAIL ERROR: $_" | Tee-Object -FilePath $Log -Append
}

exit $exit

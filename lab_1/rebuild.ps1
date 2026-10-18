# Полная пересборка из Docker Hub и PyPI с повторной инициализацией и сохранением томов баз.
$ErrorActionPreference = 'Stop'
Push-Location $PSScriptRoot
# Команды выполняются из каталога скрипта независимо от текущего каталога пользователя.
try {
    docker compose build --pull --no-cache setup
    if ($LASTEXITCODE -ne 0) { throw 'Image build failed. Containers were not replaced. Check Docker Hub/PyPI access.' }
    docker compose up -d --force-recreate
    if ($LASTEXITCODE -ne 0) { throw 'Startup failed. Inspect: docker compose logs setup' }
    docker compose ps
    if ($LASTEXITCODE -ne 0) { throw 'Cannot inspect container status.' }
} finally {
    # Исходный рабочий каталог восстанавливается и при успешной сборке, и при ошибке.
    Pop-Location
}

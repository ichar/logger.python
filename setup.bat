ren config.py config_.py
del /F /Q build
del /F /Q dist
pyinstaller -F --hidden-import=pymssql --hidden-import=_mssql --hidden-import=win32timezone service.py -n loggerService
ren config_.py config.py
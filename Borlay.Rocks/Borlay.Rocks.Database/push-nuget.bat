echo off

set location=not found

for %%f in (Bin\Debug\*.nupkg) do set location=%%f

echo %location%

pause

nuget.exe push -Source "lovey" -ApiKey az %location%

pause